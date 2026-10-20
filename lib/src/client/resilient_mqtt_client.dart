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

/// Builds the underlying `mqtt_client` instance for one connection attempt.
///
/// A fresh client is created per attempt so that no state leaks between
/// sessions. Override it to customise the client (socket options, websockets,
/// `onBadCertificate`, ...) or to inject a fake in tests. The returned
/// client is further configured by [ResilientMqttClient] (auto-reconnect is
/// always disabled, keep-alive / timeouts / connect message come from
/// [MqttConfig]).
typedef MqttServerClientFactory = MqttServerClient Function(MqttConfig config);

/// A message received on a subscription.
typedef MqttMessageEvent = MqttReceivedMessage<MqttPublishMessage>;

/// Convenience accessors for received messages.
extension MqttMessageEventPayload on MqttReceivedMessage<MqttPublishMessage> {
  /// The raw payload bytes (copied).
  Uint8List get bytes => Uint8List.fromList(payload.payload.message);

  /// The payload decoded as UTF-8 (malformed sequences are replaced).
  String get text => utf8.decode(bytes, allowMalformed: true);
}

/// A resilience layer on top of `mqtt_client`'s [MqttServerClient].
///
/// * Reconnects with a pluggable [ReconnectPolicy] (exponential backoff with
///   full jitter by default) instead of `mqtt_client`'s immediate retries.
/// * Exposes the connection state as a [Stream] of [ConnectionState].
/// * Keeps a registry of subscriptions and re-subscribes after every
///   reconnection; subscriptions are plain Dart [Stream]s (optionally
///   decoded with an [MqttDecoder]).
///
/// ```dart
/// final client = ResilientMqttClient(
///   config: const MqttConfig(host: 'localhost', clientId: 'demo'),
/// );
/// client.connectionState.listen(print);
/// await client.connect();
/// client.subscribeDecoded('sensors/+/temp', const StringMqttDecoder())
///     .listen(print);
/// ```
class ResilientMqttClient {
  /// Creates a client. Nothing happens on the network until [connect].
  ResilientMqttClient({
    required this.config,
    ReconnectPolicy? reconnectPolicy,
    MqttServerClientFactory? clientFactory,
  })  : reconnectPolicy = reconnectPolicy ?? ExponentialBackoff(),
        _clientFactory = clientFactory ?? defaultClientFactory;

  /// The default [MqttServerClientFactory]: a TCP [MqttServerClient] that
  /// makes a single attempt per `connect()` (retries are driven by the
  /// [ReconnectPolicy]).
  static MqttServerClient defaultClientFactory(MqttConfig config) =>
      MqttServerClient.withPort(
        config.host,
        config.clientId,
        config.port,
        maxConnectionAttempts: 1,
      );

  /// Connection settings.
  final MqttConfig config;

  /// Policy that spaces reconnection attempts.
  final ReconnectPolicy reconnectPolicy;

  final MqttServerClientFactory _clientFactory;

  final StreamController<ConnectionState> _stateController =
      StreamController<ConnectionState>.broadcast();
  final StreamController<DecodeError> _decodeErrors =
      StreamController<DecodeError>.broadcast();
  final Map<String, _Subscription> _subscriptions = {};

  ConnectionState _state = const Disconnected();
  MqttServerClient? _client;
  // Cancelled in _detach().
  // ignore: cancel_subscriptions
  StreamSubscription<List<MqttReceivedMessage<MqttMessage>>>? _updatesSub;
  Timer? _retryTimer;
  Future<bool>? _inFlight;
  int _retryAttempt = 0;
  Object? _lastError;
  bool _wantConnected = false;
  bool _disposed = false;

  /// The current connection state.
  ConnectionState get currentState => _state;

  /// Whether the client is currently connected.
  bool get isConnected => _state is Connected;

  /// Connection state changes. Each listener first receives the current
  /// state, then every subsequent change.
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

  /// Payloads that a decoder passed to [subscribeDecoded] failed to decode.
  Stream<DecodeError> get decodeErrors => _decodeErrors.stream;

  /// Starts connecting and keeps the connection alive until [disconnect] or
  /// [dispose].
  ///
  /// Completes with `true` if the first attempt succeeded. On failure it
  /// completes with `false` (it does not throw on network errors) and keeps
  /// retrying in the background according to [reconnectPolicy]; observe
  /// [connectionState] or await [waitUntilConnected].
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
      // A retry is already scheduled: run it now.
      _retryTimer!.cancel();
      _retryTimer = null;
      return _runAttempt();
    }
    _retryAttempt = 0;
    _setState(const Connecting());
    return _runAttempt();
  }

  /// Completes when the client is [Connected].
  ///
  /// Completes with a [MqttNotConnectedException] if the policy gives up
  /// first, or a [TimeoutException] after [timeout].
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

  /// Hint that the network is (probably) back, e.g. from a connectivity
  /// listener in a Flutter app. Skips the pending backoff delay and retries
  /// immediately. No-op unless a retry is scheduled.
  void notifyNetworkAvailable() {
    if (_disposed || !_wantConnected || _retryTimer == null) return;
    _retryTimer!.cancel();
    _retryTimer = null;
    unawaited(_runAttempt());
  }

  /// Disconnects and stops reconnecting. Subscriptions stay registered and
  /// are restored by the next [connect].
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

  /// Disconnects and releases all resources. Every subscription stream and
  /// [connectionState] stream is closed. The client cannot be reused.
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

  /// Publishes [payload] to [topic] and returns the message identifier.
  ///
  /// Throws [MqttNotConnectedException] while offline.
  int publish(
    String topic,
    List<int> payload, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
  }) {
    _checkNotDisposed();
    // TODO(v0.2): enqueue into an OfflineQueue while offline (overflow
    // strategy + TTL) instead of throwing, flush it in order on reconnect,
    // and return a Future<PublishResult> completed on PUBACK/PUBCOMP.
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

  /// Publishes [text] encoded as UTF-8. See [publish].
  int publishString(
    String topic,
    String text, {
    MqttQos qos = MqttQos.atMostOnce,
    bool retain = false,
  }) =>
      publish(topic, utf8.encode(text), qos: qos, retain: retain);

  /// Subscribes to [topicFilter] (wildcards `+` and `#` allowed).
  ///
  /// The returned broadcast stream subscribes on the broker when it gets its
  /// first listener and unsubscribes when the last listener cancels. The
  /// subscription survives reconnections. Calling [subscribe] again with the
  /// same filter returns the same stream; a higher [qos] is applied from the
  /// next (re)connection.
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

  /// Like [subscribe], but decodes every payload with [decoder].
  ///
  /// Payloads the decoder throws on are skipped and reported on
  /// [decodeErrors]; the stream itself never errors because of them.
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

  // ---------------------------------------------------------------------
  // Connection handling
  // ---------------------------------------------------------------------

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
      // Without a ping-response timeout a half-open TCP connection is never
      // detected; one keep-alive period is a reasonable default.
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
    // Re-subscribe everything that has listeners. With cleanSession=false
    // the broker may already hold these; re-subscribing is idempotent.
    for (final sub in _subscriptions.values) {
      if (sub.active) _brokerSubscribe(client, sub);
    }
    // TODO(v0.2): flush the offline queue here, in order, rate limited.
    _setState(const Connected());
  }

  void _onClientDisconnected(MqttServerClient client) {
    // Ignore callbacks from stale clients and from our own disconnect().
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
    } catch (_) {
      // Already closed / never connected.
    }
  }

  // ---------------------------------------------------------------------
  // Subscriptions
  // ---------------------------------------------------------------------

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
    } catch (_) {
      // Connection dropped meanwhile; nothing to undo on the broker.
    }
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
      // TODO(v0.2): with overlapping filters some brokers deliver a message
      // once per matching subscription; dedupe by packet id here.
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
