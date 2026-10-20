import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';

import '../codec/mqtt_decoder.dart';
import '../connection/connection_state.dart';
import '../connection/reconnect_policy.dart';
import '../errors.dart';
import '../subscription/topic_matcher.dart';
import 'mqtt_config.dart';

typedef MqttServerClientFactory = MqttServerClient Function(MqttConfig config);

typedef MqttMessageEvent = MqttReceivedMessage<MqttPublishMessage>;

extension MqttMessageEventPayload on MqttReceivedMessage<MqttPublishMessage> {
  Uint8List get bytes => Uint8List.fromList(payload.payload.message);

  String get text => utf8.decode(bytes, allowMalformed: true);
}

class ResilientMqttClient {
  ResilientMqttClient({
    required this.config,
    ReconnectPolicy? reconnectPolicy,
    MqttServerClientFactory? clientFactory,
  })  : reconnectPolicy = reconnectPolicy ?? ExponentialBackoff(),
        _clientFactory = clientFactory ?? defaultClientFactory;

  static MqttServerClient defaultClientFactory(MqttConfig config) =>
      MqttServerClient.withPort(
        config.host,
        config.clientId,
        config.port,
        maxConnectionAttempts: 1,
      );

  final MqttConfig config;

  final ReconnectPolicy reconnectPolicy;

  final MqttServerClientFactory _clientFactory;

  final StreamController<ConnectionState> _stateController =
      StreamController<ConnectionState>.broadcast();
  final StreamController<DecodeError> _decodeErrors =
      StreamController<DecodeError>.broadcast();
  final Map<String, _Subscription> _subscriptions = {};

  ConnectionState _state = const Disconnected();
  MqttServerClient? _client;

  StreamSubscription<List<MqttReceivedMessage<MqttMessage>>>? _updatesSub;
  Timer? _retryTimer;
  Future<bool>? _inFlight;
  int _retryAttempt = 0;
  Object? _lastError;
  bool _wantConnected = false;
  bool _disposed = false;

  ConnectionState get currentState => _state;

  bool get isConnected => _state is Connected;

  Stream<ConnectionState> get connectionState => Stream.multi(
        (controller) {
          controller.add(_state);
          if (_stateController.isClosed) {
            controller.close();
            return;
          }
          final sub = _stateController.stream.listen(
            controller.add,
            onDone: controller.close,
          );
          controller.onCancel = sub.cancel;
        },
        isBroadcast: true,
      );

  Stream<DecodeError> get decodeErrors => _decodeErrors.stream;

  Future<bool> connect() {
    _checkNotDisposed();
    if (_state is Connected) return Future.value(true);
    _wantConnected = true;
    final inFlight = _inFlight;
    if (inFlight != null) {
      if (_state is Disconnected) _setState(const Connecting());
      return inFlight;
    }
    if (_retryTimer != null) {
      _retryTimer!.cancel();
      _retryTimer = null;
      return _runAttempt();
    }
    _retryAttempt = 0;
    _setState(const Connecting());
    return _runAttempt();
  }

  Future<void> waitUntilConnected({Duration? timeout}) async {
    final future = connectionState.firstWhere(
      (s) =>
          s is Connected ||
          (s is Disconnected && s.reason == DisconnectReason.retriesExhausted),
    );
    final state = await (timeout == null ? future : future.timeout(timeout));
    if (state is Disconnected) {
      throw MqttNotConnectedException(
        'Reconnect policy gave up (last error: ${state.lastError})',
      );
    }
  }

  void notifyNetworkAvailable() {
    if (_disposed || !_wantConnected || _retryTimer == null) return;
    _retryTimer!.cancel();
    _retryTimer = null;
    unawaited(_runAttempt());
  }

  Future<void> disconnect() async {
    _wantConnected = false;
    _retryTimer?.cancel();
    _retryTimer = null;
    final client = _client;
    await _detach();
    if (client != null) _safeDisconnect(client);
    if (!_disposed) {
      _setState(const Disconnected(reason: DisconnectReason.userRequested));
    }
  }

  Future<void> dispose() async {
    if (_disposed) return;
    await disconnect();
    _disposed = true;
    final controllers = [
      for (final s in _subscriptions.values) s.controller,
    ];
    _subscriptions.clear();
    await Future.wait([
      for (final c in controllers) c.close(),
      _stateController.close(),
      _decodeErrors.close(),
    ]);
  }

  int publish(
    String topic,
    List<int> payload, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
  }) {
    _checkNotDisposed();

    final client = _client;
    if (client == null || _state is! Connected) {
      throw const MqttNotConnectedException('Cannot publish while offline');
    }
    final builder = MqttClientPayloadBuilder();
    builder.payload!.addAll(payload);
    try {
      return client.publishMessage(
        topic,
        qos,
        builder.payload!,
        retain: retain,
      );
    } on ConnectionException catch (e) {
      throw MqttNotConnectedException(e.toString());
    }
  }

  int publishString(
    String topic,
    String text, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
  }) =>
      publish(topic, utf8.encode(text), qos: qos, retain: retain);

  Stream<MqttMessageEvent> subscribe(
    String topicFilter, {
    MqttQos qos = MqttQos.atMostOnce,
  }) {
    _checkNotDisposed();
    if (topicFilter.isEmpty) {
      throw ArgumentError.value(
          topicFilter, 'topicFilter', 'must not be empty');
    }
    final existing = _subscriptions[topicFilter];
    if (existing != null) {
      if (qos.index > existing.qos.index) existing.qos = qos;
      return existing.controller.stream;
    }
    late final _Subscription sub;
    sub = _Subscription(
      topicFilter,
      qos,
      StreamController<MqttMessageEvent>.broadcast(
        onListen: () => _activate(sub),
        onCancel: () => _deactivate(sub),
      ),
    );
    _subscriptions[topicFilter] = sub;
    return sub.controller.stream;
  }

  Stream<T> subscribeDecoded<T>(
    String topicFilter,
    MqttDecoder<T> decoder, {
    MqttQos qos = MqttQos.atMostOnce,
  }) =>
      subscribe(topicFilter, qos: qos).transform(
        StreamTransformer<MqttMessageEvent, T>.fromHandlers(
          handleData: (message, sink) {
            final bytes = message.bytes;
            try {
              sink.add(decoder.decode(message.topic, bytes));
            } catch (error, stackTrace) {
              if (!_decodeErrors.isClosed) {
                _decodeErrors.add(
                  DecodeError(
                    topic: message.topic,
                    payload: bytes,
                    error: error,
                    stackTrace: stackTrace,
                  ),
                );
              }
            }
          },
        ),
      );

  Future<bool> _runAttempt() {
    final future = _attemptOnce().then((ok) {
      _inFlight = null;
      if (!ok && _wantConnected && !_disposed) _scheduleRetry();
      return ok;
    });
    _inFlight = future;
    return future;
  }

  Future<bool> _attemptOnce() async {
    final client = _createClient();
    Object? error;
    try {
      await client.connect(config.username, config.password);
    } catch (e) {
      error = e;
    }
    final status = client.connectionStatus;
    final ok = status?.state == MqttConnectionState.connected;
    if (!ok || !_wantConnected || _disposed) {
      if (!ok) {
        _lastError = error ??
            NoConnectionException(
              'Connection refused (return code: ${status?.returnCode})',
            );
      }
      _safeDisconnect(client);
      return false;
    }
    _onConnected(client);
    return true;
  }

  MqttServerClient _createClient() {
    final client = _clientFactory(config)
      ..autoReconnect = false
      ..keepAlivePeriod = config.keepAlive.inSeconds
      ..disconnectOnNoResponsePeriod = config.keepAlive.inSeconds
      ..connectTimeoutPeriod = config.connectTimeout.inMilliseconds
      ..connectionTimeout = config.connectTimeout.inMilliseconds
      ..secure = config.secure
      ..setProtocolV311()
      ..logging(on: config.logging);
    final context = config.securityContext;
    if (context != null) client.securityContext = context;

    var message = MqttConnectMessage().withClientIdentifier(config.clientId);
    if (config.cleanSession) message = message.startClean();
    final will = config.will;
    if (will != null) {
      message = message
          .withWillTopic(will.topic)
          .withWillMessage(will.message)
          .withWillQos(will.qos);
      if (will.retain) message = message.withWillRetain();
    }
    client.connectionMessage = message;
    client.onDisconnected = () => _onClientDisconnected(client);
    return client;
  }

  void _onConnected(MqttServerClient client) {
    _client = client;
    _retryAttempt = 0;
    _lastError = null;
    reconnectPolicy.reset();
    _updatesSub = client.updates?.listen(_dispatch);

    for (final sub in _subscriptions.values) {
      if (sub.active) _brokerSubscribe(client, sub);
    }

    _setState(const Connected());
  }

  void _onClientDisconnected(MqttServerClient client) {
    if (!identical(client, _client)) return;
    unawaited(_detach());
    if (_wantConnected && !_disposed) {
      _lastError = 'Connection lost';
      _retryAttempt = 0;
      _scheduleRetry();
    }
  }

  void _scheduleRetry() {
    _retryAttempt++;
    final delay = reconnectPolicy.nextDelay(_retryAttempt);
    if (delay == null) {
      _wantConnected = false;
      _setState(
        Disconnected(
          reason: DisconnectReason.retriesExhausted,
          lastError: _lastError,
        ),
      );
      return;
    }
    _setState(
      Reconnecting(
        attempt: _retryAttempt,
        nextDelay: delay,
        lastError: _lastError,
      ),
    );
    _retryTimer?.cancel();
    _retryTimer = Timer(delay, () {
      _retryTimer = null;
      if (_wantConnected && !_disposed) unawaited(_runAttempt());
    });
  }

  Future<void> _detach() async {
    _client = null;
    final updates = _updatesSub;
    _updatesSub = null;
    await updates?.cancel();
  }

  void _safeDisconnect(MqttServerClient client) {
    client.onDisconnected = null;
    try {
      client.disconnect();
    } catch (_) {}
  }

  void _activate(_Subscription sub) {
    sub.active = true;
    final client = _client;
    if (client != null) _brokerSubscribe(client, sub);
  }

  void _deactivate(_Subscription sub) {
    sub.active = false;
    final client = _client;
    if (client == null) return;
    try {
      client.unsubscribe(sub.filter);
    } catch (_) {}
  }

  void _brokerSubscribe(MqttServerClient client, _Subscription sub) {
    try {
      client.subscribe(sub.filter, sub.qos);
    } catch (error, stackTrace) {
      sub.controller.addError(error, stackTrace);
    }
  }

  void _dispatch(List<MqttReceivedMessage<MqttMessage>> batch) {
    for (final received in batch) {
      final message = received.payload;
      if (message is! MqttPublishMessage) continue;
      final event = MqttMessageEvent(received.topic, message);

      for (final sub in _subscriptions.values) {
        if (sub.active && topicMatches(sub.filter, received.topic)) {
          sub.controller.add(event);
        }
      }
    }
  }

  void _setState(ConnectionState state) {
    if (state == _state) return;
    _state = state;
    if (!_stateController.isClosed) _stateController.add(state);
  }

  void _checkNotDisposed() {
    if (_disposed) throw StateError('ResilientMqttClient was disposed');
  }
}

class _Subscription {
  _Subscription(this.filter, this.qos, this.controller);

  final String filter;
  MqttQos qos;
  final StreamController<MqttMessageEvent> controller;
  bool active = false;
}
