import 'dart:convert';
import 'dart:typed_data';

import 'package:resilient_mqtt/resilient_mqtt.dart';
import 'package:test/test.dart';

Uint8List bytesOf(String s) => Uint8List.fromList(utf8.encode(s));

void main() {
  test('StringMqttDecoder decodes UTF-8', () {
    expect(const StringMqttDecoder().decode('t', bytesOf('olá')), 'olá');
  });

  test('StringMqttDecoder throws on malformed input unless allowed', () {
    final bad = Uint8List.fromList([0xff, 0xfe]);
    expect(
      () => const StringMqttDecoder().decode('t', bad),
      throwsFormatException,
    );
    expect(
      const StringMqttDecoder(allowMalformed: true).decode('t', bad),
      isNotEmpty,
    );
  });

  test('JsonMqttDecoder maps JSON', () {
    final decoder = JsonMqttDecoder<int>(
      (json) => (json! as Map<String, Object?>)['v']! as int,
    );
    expect(decoder.decode('t', bytesOf('{"v": 42}')), 42);
    expect(() => decoder.decode('t', bytesOf('nope')), throwsFormatException);
  });

  test('BytesMqttDecoder and FunctionMqttDecoder', () {
    final payload = Uint8List.fromList([1, 2, 3]);
    expect(const BytesMqttDecoder().decode('t', payload), payload);
    final decoder = FunctionMqttDecoder((topic, p) => '$topic:${p.length}');
    expect(decoder.decode('x/y', payload), 'x/y:3');
  });
}
