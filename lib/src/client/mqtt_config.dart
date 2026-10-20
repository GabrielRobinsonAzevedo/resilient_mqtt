import 'dart:io' show SecurityContext;

import 'package:mqtt_client/mqtt_client.dart' show MqttQos;

/// Last Will and Testament published by the broker if the client vanishes.
final class MqttWill {
  /// Creates a Last Will message.
  const MqttWill({
    required this.topic,
    required this.message,
    this.qos = MqttQos.atLeastOnce,
    this.retain = false,
  });

  /// Topic the will is published to.
  final String topic;

  /// UTF-8 payload of the will.
  final String message;

  /// QoS of the will message.
  final MqttQos qos;

  /// Whether the broker retains the will message.
  final bool retain;
}

/// Immutable connection settings for a `ResilientMqttClient`.
final class MqttConfig {
  /// Creates a configuration.
  ///
  /// [connectTimeout] bounds a single connection attempt (TCP + CONNACK);
  /// the spacing between attempts is controlled by the `ReconnectPolicy`.
  const MqttConfig({
    required this.host,
    required this.clientId,
    this.port = 1883,
    this.keepAlive = const Duration(seconds: 30),
    this.connectTimeout = const Duration(seconds: 5),
    this.cleanSession = true,
    this.username,
    this.password,
    this.secure = false,
    this.securityContext,
    this.will,
    this.logging = false,
  });

  /// Broker host name or IP address.
  final String host;

  /// Broker TCP port.
  final int port;

  /// MQTT client identifier. Must be unique per broker.
  final String clientId;

  /// Keep-alive interval (PINGREQ period). Rounded down to whole seconds.
  final Duration keepAlive;

  /// Timeout for a single connection attempt.
  final Duration connectTimeout;

  /// Whether to start a clean session (MQTT 3.1.1 `cleanSession` flag).
  final bool cleanSession;

  /// Optional user name.
  final String? username;

  /// Optional password.
  final String? password;

  /// Whether to use TLS over TCP.
  final bool secure;

  /// TLS settings when [secure] is `true`; defaults to
  /// `SecurityContext.defaultContext`.
  final SecurityContext? securityContext;

  /// Optional Last Will and Testament.
  final MqttWill? will;

  /// Enables `mqtt_client` debug logging.
  final bool logging;

  /// Returns a copy with the given fields replaced.
  MqttConfig copyWith({
    String? host,
    int? port,
    String? clientId,
    Duration? keepAlive,
    Duration? connectTimeout,
    bool? cleanSession,
    String? username,
    String? password,
    bool? secure,
    SecurityContext? securityContext,
    MqttWill? will,
    bool? logging,
  }) =>
      MqttConfig(
        host: host ?? this.host,
        port: port ?? this.port,
        clientId: clientId ?? this.clientId,
        keepAlive: keepAlive ?? this.keepAlive,
        connectTimeout: connectTimeout ?? this.connectTimeout,
        cleanSession: cleanSession ?? this.cleanSession,
        username: username ?? this.username,
        password: password ?? this.password,
        secure: secure ?? this.secure,
        securityContext: securityContext ?? this.securityContext,
        will: will ?? this.will,
        logging: logging ?? this.logging,
      );

  @override
  String toString() => 'MqttConfig($host:$port, clientId: $clientId, '
      'secure: $secure, cleanSession: $cleanSession)';
}
