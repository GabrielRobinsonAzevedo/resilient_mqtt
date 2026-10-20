import 'dart:io' show SecurityContext;

import 'package:mqtt_client/mqtt_client.dart' show MqttQos;

final class MqttWill {
  const MqttWill({
    required this.topic,
    required this.message,
    this.qos = MqttQos.atLeastOnce,
    this.retain = false,
  });

  final String topic;

  final String message;

  final MqttQos qos;

  final bool retain;
}

final class MqttConfig {
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

  final String host;

  final int port;

  final String clientId;

  final Duration keepAlive;

  final Duration connectTimeout;

  final bool cleanSession;

  final String? username;

  final String? password;

  final bool secure;

  final SecurityContext? securityContext;

  final MqttWill? will;

  final bool logging;

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
