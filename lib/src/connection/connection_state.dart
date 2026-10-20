enum DisconnectReason {
  notStarted,

  userRequested,

  retriesExhausted,
}

sealed class ConnectionState {
  const ConnectionState();

  bool get isConnected => this is Connected;
}

final class Connecting extends ConnectionState {
  const Connecting();

  @override
  bool operator ==(Object other) => other is Connecting;

  @override
  int get hashCode => (Connecting).hashCode;

  @override
  String toString() => 'Connecting()';
}

final class Connected extends ConnectionState {
  const Connected();

  @override
  bool operator ==(Object other) => other is Connected;

  @override
  int get hashCode => (Connected).hashCode;

  @override
  String toString() => 'Connected()';
}

final class Reconnecting extends ConnectionState {
  const Reconnecting({
    required this.attempt,
    required this.nextDelay,
    this.lastError,
  }) : assert(attempt > 0, 'attempt is 1-based');

  final int attempt;

  final Duration nextDelay;

  final Object? lastError;

  @override
  bool operator ==(Object other) =>
      other is Reconnecting &&
      other.attempt == attempt &&
      other.nextDelay == nextDelay;

  @override
  int get hashCode => Object.hash(Reconnecting, attempt, nextDelay);

  @override
  String toString() => 'Reconnecting(attempt: $attempt, nextDelay: $nextDelay'
      '${lastError == null ? '' : ', lastError: $lastError'})';
}

final class Disconnected extends ConnectionState {
  const Disconnected({
    this.reason = DisconnectReason.notStarted,
    this.lastError,
  });

  final DisconnectReason reason;

  final Object? lastError;

  @override
  bool operator ==(Object other) =>
      other is Disconnected && other.reason == reason;

  @override
  int get hashCode => Object.hash(Disconnected, reason);

  @override
  String toString() => 'Disconnected(reason: ${reason.name}'
      '${lastError == null ? '' : ', lastError: $lastError'})';
}
