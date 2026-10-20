import 'dart:math' as math;

abstract interface class ReconnectPolicy {
  Duration? nextDelay(int attempt);

  void reset();
}

final class NoReconnect implements ReconnectPolicy {
  const NoReconnect();

  @override
  Duration? nextDelay(int attempt) => null;

  @override
  void reset() {}
}

final class ExponentialBackoff implements ReconnectPolicy {
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

  final Duration base;

  final Duration max;

  final double multiplier;

  final int? maxAttempts;

  final bool jitter;

  final math.Random _random;

  Duration ceilingFor(int attempt) {
    if (attempt < 1) {
      throw RangeError.range(attempt, 1, null, 'attempt');
    }
    final maxUs = max.inMicroseconds.toDouble();

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

    return Duration(
      microseconds: (_random.nextDouble() * ceiling.inMicroseconds).floor(),
    );
  }

  @override
  void reset() {}

  @override
  String toString() => 'ExponentialBackoff(base: $base, max: $max, '
      'multiplier: $multiplier, maxAttempts: $maxAttempts, jitter: $jitter)';
}
