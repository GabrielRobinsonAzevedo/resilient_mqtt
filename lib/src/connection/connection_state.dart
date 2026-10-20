/// Reason attached to a [Disconnected] state.
enum DisconnectReason {
  /// The client was created but `connect()` has not been called yet.
  notStarted,

  /// The application called `disconnect()` (or `dispose()`).
  userRequested,

  /// The `ReconnectPolicy` gave up (e.g. `maxAttempts` reached).
  retriesExhausted,
}

/// The connection state of a `ResilientMqttClient`.
///
/// This is a sealed hierarchy, so `switch` expressions over it are
/// exhaustive:
///
/// ```dart
/// client.connectionState.listen((state) => switch (state) {
///   Connecting() => print('connecting...'),
///   Connected() => print('online'),
///   Reconnecting(:final attempt, :final nextDelay) =>
///       print('retry #$attempt in $nextDelay'),
///   Disconnected(:final reason) => print('offline: $reason'),
/// });
/// ```
sealed class ConnectionState {
  const ConnectionState();

  /// Whether messages can currently be published.
  bool get isConnected => this is Connected;
}

/// The initial connection attempt (triggered by `connect()`) is in progress.
final class Connecting extends ConnectionState {
  /// Creates a [Connecting] state.
  const Connecting();

  @override
  bool operator ==(Object other) => other is Connecting;

  @override
  int get hashCode => (Connecting).hashCode;

  @override
  String toString() => 'Connecting()';
}

/// The client is connected to the broker.
final class Connected extends ConnectionState {
  /// Creates a [Connected] state.
  const Connected();

  @override
  bool operator ==(Object other) => other is Connected;

  @override
  int get hashCode => (Connected).hashCode;

  @override
  String toString() => 'Connected()';
}

/// The connection was lost (or could not be established) and a new attempt
/// is scheduled after [nextDelay].
final class Reconnecting extends ConnectionState {
  /// Creates a [Reconnecting] state.
  ///
  /// [attempt] is 1-based: the first retry after a failure is attempt `1`.
  const Reconnecting({
    required this.attempt,
    required this.nextDelay,
    this.lastError,
  }) : assert(attempt > 0, 'attempt is 1-based');

  /// 1-based number of the reconnection attempt that will run next.
  final int attempt;

  /// How long the client waits before running [attempt].
  final Duration nextDelay;

  /// The error that caused the previous attempt to fail, if any.
  ///
  /// Not part of equality.
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

/// The client is offline and will not reconnect on its own.
final class Disconnected extends ConnectionState {
  /// Creates a [Disconnected] state.
  const Disconnected({
    this.reason = DisconnectReason.notStarted,
    this.lastError,
  });

  /// Why the client is disconnected.
  final DisconnectReason reason;

  /// The last connection error, when [reason] is
  /// [DisconnectReason.retriesExhausted]. Not part of equality.
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
