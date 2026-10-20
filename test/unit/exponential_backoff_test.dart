import 'dart:math';

import 'package:resilient_mqtt/resilient_mqtt.dart';
import 'package:test/test.dart';

class _FixedRandom implements Random {
  _FixedRandom(this.value);

  final double value;

  @override
  double nextDouble() => value;

  @override
  bool nextBool() => throw UnimplementedError();

  @override
  int nextInt(int max) => throw UnimplementedError();
}

void main() {
  group('ExponentialBackoff.ceilingFor', () {
    final policy = ExponentialBackoff(
      base: const Duration(seconds: 1),
      max: const Duration(seconds: 30),
    );

    test('doubles every attempt starting at base', () {
      expect(policy.ceilingFor(1), const Duration(seconds: 1));
      expect(policy.ceilingFor(2), const Duration(seconds: 2));
      expect(policy.ceilingFor(3), const Duration(seconds: 4));
      expect(policy.ceilingFor(5), const Duration(seconds: 16));
    });

    test('is capped at max', () {
      expect(policy.ceilingFor(6), const Duration(seconds: 30));
      expect(policy.ceilingFor(100), const Duration(seconds: 30));
    });

    test('does not overflow for huge attempt numbers', () {
      expect(policy.ceilingFor(1 << 20), const Duration(seconds: 30));
    });

    test('honours a custom multiplier', () {
      final p = ExponentialBackoff(
        base: const Duration(milliseconds: 100),
        max: const Duration(seconds: 10),
        multiplier: 3,
      );
      expect(p.ceilingFor(1), const Duration(milliseconds: 100));
      expect(p.ceilingFor(2), const Duration(milliseconds: 300));
      expect(p.ceilingFor(3), const Duration(milliseconds: 900));
    });

    test('rejects attempt < 1', () {
      expect(() => policy.ceilingFor(0), throwsRangeError);
    });
  });

  group('ExponentialBackoff.nextDelay (full jitter)', () {
    test('scales the ceiling by Random.nextDouble()', () {
      final p = ExponentialBackoff(
        base: const Duration(seconds: 1),
        max: const Duration(minutes: 1),
        random: _FixedRandom(0.5),
      );
      expect(p.nextDelay(1), const Duration(milliseconds: 500));
      expect(p.nextDelay(3), const Duration(seconds: 2));
    });

    test('random 0 gives zero delay', () {
      final p = ExponentialBackoff(random: _FixedRandom(0));
      expect(p.nextDelay(4), Duration.zero);
    });

    test('stays within [0, ceiling] for many attempts', () {
      final p = ExponentialBackoff(
        base: const Duration(milliseconds: 200),
        max: const Duration(seconds: 20),
        random: Random(42),
      );
      for (var attempt = 1; attempt <= 50; attempt++) {
        for (var i = 0; i < 20; i++) {
          final d = p.nextDelay(attempt)!;
          expect(d, greaterThanOrEqualTo(Duration.zero));
          expect(d, lessThanOrEqualTo(p.ceilingFor(attempt)));
        }
      }
    });

    test('is deterministic with a seeded Random', () {
      List<Duration?> run() {
        final p = ExponentialBackoff(random: Random(7));
        return [for (var a = 1; a <= 10; a++) p.nextDelay(a)];
      }

      expect(run(), run());
    });

    test('actually spreads delays (jitter)', () {
      final p = ExponentialBackoff(
        base: const Duration(seconds: 10),
        max: const Duration(seconds: 10),
        random: Random(1),
      );
      final delays = {for (var i = 0; i < 100; i++) p.nextDelay(1)};
      expect(delays.length, greaterThan(90));
    });

    test('jitter: false returns the ceiling', () {
      final p = ExponentialBackoff(
        base: const Duration(seconds: 1),
        max: const Duration(seconds: 5),
        jitter: false,
      );
      expect(
        [for (var a = 1; a <= 5; a++) p.nextDelay(a)],
        const [
          Duration(seconds: 1),
          Duration(seconds: 2),
          Duration(seconds: 4),
          Duration(seconds: 5),
          Duration(seconds: 5),
        ],
      );
    });
  });

  group('ExponentialBackoff.maxAttempts', () {
    test('null retries forever', () {
      final p = ExponentialBackoff(random: Random(0));
      expect(p.nextDelay(1000000), isNotNull);
    });

    test('returns null after maxAttempts', () {
      final p = ExponentialBackoff(maxAttempts: 3, random: Random(0));
      expect(p.nextDelay(1), isNotNull);
      expect(p.nextDelay(3), isNotNull);
      expect(p.nextDelay(4), isNull);
    });

    test('maxAttempts: 0 never retries', () {
      expect(ExponentialBackoff(maxAttempts: 0).nextDelay(1), isNull);
    });

    test('reset() keeps the policy usable', () {
      final p = ExponentialBackoff(maxAttempts: 1, jitter: false)..reset();
      expect(p.nextDelay(1), const Duration(seconds: 1));
    });
  });

  group('ExponentialBackoff validation', () {
    test('rejects non-positive base', () {
      expect(
          () => ExponentialBackoff(base: Duration.zero), throwsArgumentError);
    });

    test('rejects max < base', () {
      expect(
        () => ExponentialBackoff(
          base: const Duration(seconds: 10),
          max: const Duration(seconds: 1),
        ),
        throwsArgumentError,
      );
    });

    test('rejects multiplier < 1', () {
      expect(() => ExponentialBackoff(multiplier: 0.5), throwsArgumentError);
    });

    test('rejects negative maxAttempts', () {
      expect(() => ExponentialBackoff(maxAttempts: -1), throwsArgumentError);
    });
  });

  test('NoReconnect never retries', () {
    expect(const NoReconnect().nextDelay(1), isNull);
  });
}
