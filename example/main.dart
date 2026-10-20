import 'dart:async';
import 'dart:io';

import 'package:resilient_mqtt/resilient_mqtt.dart';

Future<void> main(List<String> args) async {
  final host = args.isNotEmpty ? args[0] : 'localhost';
  final port = args.length > 1 ? int.parse(args[1]) : 1883;

  final client = ResilientMqttClient(
    config: MqttConfig(
      host: host,
      port: port,
      clientId: 'resilient-mqtt-example-$pid',
      keepAlive: const Duration(seconds: 10),
    ),
    reconnectPolicy: ExponentialBackoff(
      base: const Duration(seconds: 1),
      max: const Duration(seconds: 30),
    ),
  );

  client.connectionState.listen(
    (state) => stdout.writeln(switch (state) {
      Connecting() => '… connecting to $host:$port',
      Connected() => '✓ connected',
      Reconnecting(:final attempt, :final nextDelay) =>
        '↻ retry #$attempt in ${nextDelay.inMilliseconds} ms',
      Disconnected(:final reason) => '✗ disconnected (${reason.name})',
    }),
  );

  client
      .subscribeDecoded('resilient_mqtt/demo/#', const StringMqttDecoder())
      .listen((text) => stdout.writeln('← $text'));
  client.decodeErrors.listen((e) => stderr.writeln('decode error: $e'));

  await client.connect();

  var counter = 0;
  final timer = Timer.periodic(const Duration(seconds: 2), (_) {
    if (!client.isConnected) return;
    client.publishString('resilient_mqtt/demo/tick', 'tick ${++counter}');
  });

  await ProcessSignal.sigint.watch().first;
  timer.cancel();
  await client.dispose();
  exit(0);
}
