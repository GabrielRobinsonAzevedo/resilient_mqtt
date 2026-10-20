@Tags(['integration'])
library;

import 'dart:async';
import 'dart:io';

import 'package:resilient_mqtt/resilient_mqtt.dart';
import 'package:test/test.dart';

final _env = Platform.environment;
final _host = _env['MQTT_HOST'] ?? 'localhost';
final _port = int.parse(_env['MQTT_PORT'] ?? '1883');
final _stopCmd = _env['MQTT_BROKER_STOP_CMD'] ?? 'docker stop mosquitto';
final _startCmd = _env['MQTT_BROKER_START_CMD'] ?? 'docker start mosquitto';

String _uniqueId(String prefix) =>
    '$prefix-$pid-${DateTime.now().microsecondsSinceEpoch}';

ResilientMqttClient _newClient(String prefix) => ResilientMqttClient(
      config: MqttConfig(
        host: _host,
        port: _port,
        clientId: _uniqueId(prefix),
        keepAlive: const Duration(seconds: 5),
        connectTimeout: const Duration(seconds: 2),
      ),
      reconnectPolicy: ExponentialBackoff(
        base: const Duration(milliseconds: 200),
        max: const Duration(seconds: 2),
      ),
    );

Future<void> _sh(String command) async {
  final result = await Process.run('sh', ['-c', command]);
  if (result.exitCode != 0) {
    fail('`$command` failed (${result.exitCode}): ${result.stderr}');
  }
}

void main() {
  test('connects, subscribes with a decoder and receives its own publish',
      () async {
    final client = _newClient('rt');
    addTearDown(client.dispose);
    final topic = 'resilient_mqtt/test/${_uniqueId('t')}';

    expect(await client.connect(), isTrue);
    expect(client.currentState, const Connected());

    final received =
        client.subscribeDecoded('$topic/#', const StringMqttDecoder()).first;

    await Future<void>.delayed(const Duration(milliseconds: 300));
    client.publishString('$topic/a', 'hello', qos: MqttQos.atLeastOnce);

    expect(await received.timeout(const Duration(seconds: 5)), 'hello');
  });

  test('reports decode errors without breaking the stream', () async {
    final client = _newClient('dec');
    addTearDown(client.dispose);
    final topic = 'resilient_mqtt/test/${_uniqueId('d')}';
    await client.connect();

    final errors = client.decodeErrors.first;
    final values = client
        .subscribeDecoded(topic, JsonMqttDecoder((json) => json! as int))
        .first;
    await Future<void>.delayed(const Duration(milliseconds: 300));
    client
      ..publishString(topic, 'not json')
      ..publishString(topic, '7');

    expect((await errors.timeout(const Duration(seconds: 5))).topic, topic);
    expect(await values.timeout(const Duration(seconds: 5)), 7);
  });

  test(
    'reconnects with backoff and re-subscribes after a broker restart',
    () async {
      final client = _newClient('restart');
      addTearDown(client.dispose);
      final topic = 'resilient_mqtt/test/${_uniqueId('r')}';
      final states = <ConnectionState>[];
      client.connectionState.listen(states.add);

      await client.connect();
      final messages = StreamIterator(
        client.subscribeDecoded(topic, const StringMqttDecoder()),
      );
      addTearDown(messages.cancel);
      final firstMessage = messages.moveNext();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      client.publishString(topic, 'before');
      expect(await firstMessage.timeout(const Duration(seconds: 5)), isTrue);
      expect(messages.current, 'before');

      await _sh(_stopCmd);
      await client.connectionState
          .firstWhere((s) => s is Reconnecting)
          .timeout(const Duration(seconds: 20));
      expect(
        () => client.publishString(topic, 'offline'),
        throwsA(isA<MqttNotConnectedException>()),
      );

      await _sh(_startCmd);
      await client.waitUntilConnected(timeout: const Duration(seconds: 30));

      final secondMessage = messages.moveNext();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      client.publishString(topic, 'after');
      expect(await secondMessage.timeout(const Duration(seconds: 5)), isTrue);
      expect(messages.current, 'after');
      expect(states.whereType<Reconnecting>(), isNotEmpty);
    },
    skip: _env['MQTT_SKIP_RESTART'] == '1' ? 'MQTT_SKIP_RESTART=1' : false,
    timeout: const Timeout(Duration(minutes: 2)),
  );
}
