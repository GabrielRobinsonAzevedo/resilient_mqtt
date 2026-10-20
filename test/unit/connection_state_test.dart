import 'package:resilient_mqtt/resilient_mqtt.dart';
import 'package:test/test.dart';

String describe(ConnectionState state) => switch (state) {
      Connecting() => 'connecting',
      Connected() => 'connected',
      Reconnecting(:final attempt, :final nextDelay) =>
        'reconnecting #$attempt in ${nextDelay.inMilliseconds}ms',
      Disconnected(:final reason) => 'disconnected (${reason.name})',
    };

void main() {
  group('ConnectionState', () {
    test('exhaustive switch covers every subtype', () {
      expect(describe(const Connecting()), 'connecting');
      expect(describe(const Connected()), 'connected');
      expect(
        describe(
          const Reconnecting(
            attempt: 2,
            nextDelay: Duration(milliseconds: 1500),
          ),
        ),
        'reconnecting #2 in 1500ms',
      );
      expect(describe(const Disconnected()), 'disconnected (notStarted)');
    });

    test('isConnected is only true for Connected', () {
      expect(const Connected().isConnected, isTrue);
      expect(const Connecting().isConnected, isFalse);
      expect(
        const Reconnecting(attempt: 1, nextDelay: Duration.zero).isConnected,
        isFalse,
      );
      expect(const Disconnected().isConnected, isFalse);
    });

    test('value equality', () {
      expect(const Connected(), const Connected());
      expect(const Connecting(), const Connecting());
      expect(const Connected(), isNot(const Connecting()));
      expect(
        const Reconnecting(attempt: 1, nextDelay: Duration(seconds: 1)),
        const Reconnecting(attempt: 1, nextDelay: Duration(seconds: 1)),
      );
      expect(
        const Reconnecting(attempt: 1, nextDelay: Duration(seconds: 1)),
        isNot(const Reconnecting(attempt: 2, nextDelay: Duration(seconds: 1))),
      );
      expect(
        const Disconnected(reason: DisconnectReason.userRequested),
        isNot(const Disconnected()),
      );
    });

    test('lastError is not part of equality', () {
      expect(
        const Reconnecting(
          attempt: 1,
          nextDelay: Duration.zero,
          lastError: 'boom',
        ),
        const Reconnecting(attempt: 1, nextDelay: Duration.zero),
      );
      expect(
        const Disconnected(
          reason: DisconnectReason.retriesExhausted,
          lastError: 'x',
        ).hashCode,
        const Disconnected(reason: DisconnectReason.retriesExhausted).hashCode,
      );
    });

    test('toString is informative', () {
      expect(
        const Reconnecting(attempt: 3, nextDelay: Duration(seconds: 2))
            .toString(),
        contains('attempt: 3'),
      );
      expect(
        const Disconnected(reason: DisconnectReason.userRequested).toString(),
        contains('userRequested'),
      );
    });
  });

  group('ResilientMqttClient initial state', () {
    test('starts Disconnected(notStarted) and replays it to listeners',
        () async {
      final client = ResilientMqttClient(
        config: const MqttConfig(host: 'localhost', clientId: 'test'),
      );
      expect(client.currentState, const Disconnected());
      expect(await client.connectionState.first, const Disconnected());
      await client.dispose();
    });

    test('publish while offline throws MqttNotConnectedException', () async {
      final client = ResilientMqttClient(
        config: const MqttConfig(host: 'localhost', clientId: 'test'),
      );
      expect(
        () => client.publishString('a/b', 'hi'),
        throwsA(isA<MqttNotConnectedException>()),
      );
      await client.dispose();
    });

    test('gives up with retriesExhausted when the broker is unreachable',
        () async {
      final client = ResilientMqttClient(
        // Port 1 on localhost: connection refused immediately.
        config: const MqttConfig(
          host: '127.0.0.1',
          port: 1,
          clientId: 'test',
          connectTimeout: Duration(seconds: 1),
        ),
        reconnectPolicy: ExponentialBackoff(
          base: const Duration(milliseconds: 10),
          max: const Duration(milliseconds: 20),
          maxAttempts: 2,
        ),
      );
      final states = <ConnectionState>[];
      final sub = client.connectionState.listen(states.add);

      expect(await client.connect(), isFalse);
      await expectLater(
        client.waitUntilConnected(timeout: const Duration(seconds: 20)),
        throwsA(isA<MqttNotConnectedException>()),
      );

      expect(states.first, const Disconnected());
      expect(states[1], const Connecting());
      expect(
        states.whereType<Reconnecting>().map((s) => s.attempt),
        [1, 2],
      );
      expect(
        states.last,
        const Disconnected(reason: DisconnectReason.retriesExhausted),
      );
      await sub.cancel();
      await client.dispose();
    });
  });
}
