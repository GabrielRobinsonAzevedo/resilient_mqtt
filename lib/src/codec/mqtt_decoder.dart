import 'dart:convert';
import 'dart:typed_data';

abstract interface class MqttDecoder<T> {
  T decode(String topic, Uint8List payload);
}

final class BytesMqttDecoder implements MqttDecoder<Uint8List> {
  const BytesMqttDecoder();

  @override
  Uint8List decode(String topic, Uint8List payload) => payload;
}

final class StringMqttDecoder implements MqttDecoder<String> {
  const StringMqttDecoder({this.allowMalformed = false});

  final bool allowMalformed;

  @override
  String decode(String topic, Uint8List payload) =>
      utf8.decode(payload, allowMalformed: allowMalformed);
}

final class JsonMqttDecoder<T> implements MqttDecoder<T> {
  const JsonMqttDecoder(this.fromJson);

  final T Function(Object? json) fromJson;

  @override
  T decode(String topic, Uint8List payload) =>
      fromJson(json.decode(utf8.decode(payload)));
}

final class FunctionMqttDecoder<T> implements MqttDecoder<T> {
  const FunctionMqttDecoder(this._decode);

  final T Function(String topic, Uint8List payload) _decode;

  @override
  T decode(String topic, Uint8List payload) => _decode(topic, payload);
}

final class DecodeError {
  const DecodeError({
    required this.topic,
    required this.payload,
    required this.error,
    required this.stackTrace,
  });

  final String topic;

  final Uint8List payload;

  final Object error;

  final StackTrace stackTrace;

  @override
  String toString() =>
      'DecodeError(topic: $topic, ${payload.length} bytes, error: $error)';
}
