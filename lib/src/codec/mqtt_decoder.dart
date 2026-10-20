import 'dart:convert';
import 'dart:typed_data';

/// Converts a raw MQTT payload into a value of type [T].
///
/// Implementations may throw; errors are caught by `ResilientMqttClient` and
/// reported on its `decodeErrors` stream instead of breaking the
/// subscription.
abstract interface class MqttDecoder<T> {
  /// Decodes the [payload] received on [topic].
  T decode(String topic, Uint8List payload);
}

/// Returns the payload bytes unchanged.
final class BytesMqttDecoder implements MqttDecoder<Uint8List> {
  /// Creates a [BytesMqttDecoder].
  const BytesMqttDecoder();

  @override
  Uint8List decode(String topic, Uint8List payload) => payload;
}

/// Decodes the payload as UTF-8 text.
final class StringMqttDecoder implements MqttDecoder<String> {
  /// Creates a [StringMqttDecoder].
  ///
  /// When [allowMalformed] is `false` (default) invalid UTF-8 throws a
  /// [FormatException].
  const StringMqttDecoder({this.allowMalformed = false});

  /// Whether invalid UTF-8 sequences are replaced instead of throwing.
  final bool allowMalformed;

  @override
  String decode(String topic, Uint8List payload) =>
      utf8.decode(payload, allowMalformed: allowMalformed);
}

/// Decodes a UTF-8 JSON payload and maps it with [fromJson].
///
/// ```dart
/// final decoder = JsonMqttDecoder(Telemetry.fromJson);
/// ```
final class JsonMqttDecoder<T> implements MqttDecoder<T> {
  /// Creates a [JsonMqttDecoder] that maps decoded JSON with [fromJson].
  const JsonMqttDecoder(this.fromJson);

  /// Maps the decoded JSON value (a `Map`, `List`, `String`, `num`, `bool`
  /// or `null`) to [T].
  final T Function(Object? json) fromJson;

  @override
  T decode(String topic, Uint8List payload) =>
      fromJson(json.decode(utf8.decode(payload)));
}

/// Adapts a plain function into an [MqttDecoder].
final class FunctionMqttDecoder<T> implements MqttDecoder<T> {
  /// Creates a decoder that delegates to [_decode].
  const FunctionMqttDecoder(this._decode);

  final T Function(String topic, Uint8List payload) _decode;

  @override
  T decode(String topic, Uint8List payload) => _decode(topic, payload);
}

/// A payload that could not be decoded.
final class DecodeError {
  /// Creates a [DecodeError].
  const DecodeError({
    required this.topic,
    required this.payload,
    required this.error,
    required this.stackTrace,
  });

  /// The topic the message arrived on.
  final String topic;

  /// The raw payload.
  final Uint8List payload;

  /// The error thrown by the decoder.
  final Object error;

  /// Where the decoder threw.
  final StackTrace stackTrace;

  @override
  String toString() =>
      'DecodeError(topic: $topic, ${payload.length} bytes, error: $error)';
}
