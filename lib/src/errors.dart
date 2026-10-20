/// Thrown when an operation needs a live connection but the client is
/// offline.
///
/// v0.2 will add an offline queue so `publish` no longer throws.
final class MqttNotConnectedException implements Exception {
  /// Creates the exception.
  const MqttNotConnectedException([this.message = 'Not connected']);

  /// Human readable description.
  final String message;

  @override
  String toString() => 'MqttNotConnectedException: $message';
}
