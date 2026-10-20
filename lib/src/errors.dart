final class MqttNotConnectedException implements Exception {
  const MqttNotConnectedException([this.message = 'Not connected']);

  final String message;

  @override
  String toString() => 'MqttNotConnectedException: $message';
}
