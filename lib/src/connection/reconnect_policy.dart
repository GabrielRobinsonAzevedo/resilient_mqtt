import 'dart:math' as math;

/// Decides how long to wait before each reconnection attempt.
///
/// Implementations must be cheap and side-effect free apart from internal
/// bookkeeping reset by [reset].
abstract interface class ReconnectPolicy {
  /// Returns the delay before reconnection [attempt] (1-based), or `null` to
  /// stop reconnecting.
  Duration? nextDelay(int attempt);

  /// Called after a successful connection so stateful policies can start
  /// over.
  void reset();
}

/// A policy that never reconnects.
final class NoReconnect implements ReconnectPolicy {
  /// Creates a [NoReconnect] policy.
  const NoReconnect();

  @override
  Duration? nextDelay(int attempt) => null;

  @override
  void reset() {}
}

/// Exponential backoff with "full jitter".
///
/// For attempt `n` (1-based) the ceiling is
/// `min(max, base * multiplier^(n - 1))` and the actual delay is drawn
/// uniformly from `[0, ceiling]`. Full jitter spreads reconnecting clients
/// over time and avoids a thundering herd when a broker comes back up (see
/// "Exponential Backoff And Jitter", AWS Architecture Blog).
///
/// Pass a seeded [math.Random] to make delays deterministic in tests.
final class ExponentialBackoff implements ReconnectPolicy {
  /// Creates an exponential backoff policy.
  ///
  /// * [base]: ceiling for the first attempt. Must be positive.
  /// * [max]: upper bound for any delay. Must be `>= base`.
  /// * [multiplier]: growth factor per attempt. Must be `>= 1`.
  /// * [maxAttempts]: `null` retries forever; otherwise [nextDelay] returns
  ///   `null` once `attempt > maxAttempts`. Must be `>= 0`.
  /// * [jitter]: `false` disables jitter (delay == ceiling). Useful for
  ///   debugging; keep it on in production.
  /// * [random]: source of randomness, injectable for tests.
  ExponentialBackoff({
    this.base = const Duration(seconds: 1),
    this.max = const Duration(minutes: 2),
    this.multiplier = 2.0,
    this.maxAttempts,
    this.jitter = true,
    math.Random? random,
  }) : _random = random ?? math.Random() {
    if (base <= Duration.zero) {
      throw ArgumentError.value(base, 'base', 'must be positive');
    }
    if (max < base) {
      throw ArgumentError.value(max, 'max', 'must be >= base ($base)');
    }
    if (multiplier.isNaN || multiplier < 1) {
      throw ArgumentError.value(multiplier, 'multiplier', 'must be >= 1');
    }
    if (maxAttempts != null && maxAttempts! < 0) {
      throw ArgumentError.value(maxAttempts, 'maxAttempts', 'must be >= 0');
    }
  }

  /// Ceiling of the first attempt.
  final Duration base;

  /// Upper bound for any delay.
  final Duration max;

  /// Growth factor applied per attempt.
  final double multiplier;

  /// Maximum number of attempts, or `null` for unlimited.
  final int? maxAttempts;

  /// Whether full jitter is applied.
  final bool jitter;

  final math.Random _random;

  /// The (un-jittered) upper bound of the delay for [attempt] (1-based).
  Duration ceilingFor(int attempt) {
    if (attempt < 1) {
      throw RangeError.range(attempt, 1, null, 'attempt');
    }
    final maxUs = max.inMicroseconds.toDouble();
    // base * multiplier^(attempt-1), computed in doubles; pow may overflow to
    // Infinity for large attempts, which min() clamps to max.
    final raw =
        base.inMicroseconds * math.pow(multiplier, attempt - 1).toDouble();
    final capped = raw.isFinite ? math.min(raw, maxUs) : maxUs;
    return Duration(microseconds: capped.round());
  }

  @override
  Duration? nextDelay(int attempt) {
    final limit = maxAttempts;
    if (limit != null && attempt > limit) return null;
    final ceiling = ceilingFor(attempt);
    if (!jitter) return ceiling;
    // nextDouble() is in [0, 1), so the delay is in [0, ceiling).
    return Duration(
      microseconds: (_random.nextDouble() * ceiling.inMicroseconds).floor(),
    );
  }

  /// Stateless: the attempt number fully determines the ceiling.
  @override
  void reset() {}

  @override
  String toString() => 'ExponentialBackoff(base: $base, max: $max, '
      'multiplier: $multiplier, maxAttempts: $maxAttempts, jitter: $jitter)';
}
