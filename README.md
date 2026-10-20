# resilient_mqtt

[![ci](https://github.com/GabrielRobinsonAzevedo/resilient_mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/GabrielRobinsonAzevedo/resilient_mqtt/actions/workflows/ci.yml)

**The resilience layer `mqtt_client` was missing: pure Dart, idiomatic
(Streams/Futures), testable, and you keep the client you already use.**

> **Status: early development (0.0.x).** The API is unstable and will change
> before 0.1.0. Not ready for production yet.

[`mqtt_client`](https://pub.dev/packages/mqtt_client) is the de-facto MQTT
client for Dart, but its built-in auto-reconnect retries immediately (no
backoff, no jitter), connection state is only exposed through callbacks,
publishing while offline throws, and payloads arrive as raw byte buffers.
`resilient_mqtt` wraps it and adds:

- **Exponential backoff with full jitter** and a pluggable `ReconnectPolicy`,
  so a fleet of devices does not hammer a broker that just came back.
- **Connection state as a `Stream`** of a sealed `ConnectionState`
  (`Connecting`, `Connected`, `Reconnecting(attempt, nextDelay)`,
  `Disconnected(reason)`), perfect for exhaustive `switch`.
- **Subscriptions as `Stream`s** that survive reconnections (automatic
  re-subscribe) and unsubscribe when the last listener cancels.
- **Typed payloads** via `MqttDecoder<T>` (string, JSON, bytes, custom), with
  decode failures reported on a separate `decodeErrors` stream.
- **Pure Dart**: no Flutter, no `connectivity_plus`. Works in CLI tools,
  servers and Flutter apps (call `notifyNetworkAvailable()` from your own
  connectivity listener to skip the backoff delay).

## Quick start

```yaml
dependencies:
  resilient_mqtt: ^0.0.1
```

```dart
import 'package:resilient_mqtt/resilient_mqtt.dart';

Future<void> main() async {
  final client = ResilientMqttClient(
    config: const MqttConfig(host: 'broker.local', clientId: 'device-42'),
    reconnectPolicy: ExponentialBackoff(
      base: const Duration(seconds: 1),
      max: const Duration(minutes: 2),
    ),
  );

  client.connectionState.listen((state) => print(switch (state) {
        Connecting() => 'connecting…',
        Connected() => 'online',
        Reconnecting(:final attempt, :final nextDelay) =>
          'retry #$attempt in $nextDelay',
        Disconnected(:final reason) => 'offline ($reason)',
      }));

  // Typed, re-subscribed automatically after every reconnection.
  client
      .subscribeDecoded(
        'devices/42/cmd/#',
        JsonMqttDecoder((json) => json! as Map<String, Object?>),
        qos: MqttQos.atLeastOnce,
      )
      .listen(print);
  client.decodeErrors.listen((e) => print('bad payload: $e'));

  // Never throws on network errors: retries in the background.
  await client.connect();
  await client.waitUntilConnected(timeout: const Duration(seconds: 30));

  client.publishString('devices/42/telemetry', '{"temp": 21.5}',
      qos: MqttQos.atLeastOnce);

  await client.dispose();
}
```

A runnable CLI demo lives in [`example/main.dart`](example/main.dart):

```sh
docker run --rm -p 1883:1883 \
  -v "$PWD/tool/mosquitto:/mosquitto/config" eclipse-mosquitto:2
dart run example/main.dart localhost 1883
```

Stop and restart the container to watch the client back off and recover.

## Roadmap

| Version | Theme | Highlights |
|---|---|---|
| **0.1.0** | Connects and survives | `ConnectionState` stream ✅, `ExponentialBackoff` ✅, reconnection supervisor on top of `mqtt_client` ✅ (initial), re-subscribe ✅, username/password auth ✅, injectable clock, Mosquitto integration tests in CI (incl. broker restart) |
| **0.2.0** | Offline-first and typed | `OfflineQueue` interface + in-memory queue (overflow strategy, TTL), `publish()` returning `Future<PublishResult>` completed on PUBACK, ordered flush on reconnect, decoders ✅, subscription ref-counting/dedupe, TLS/mTLS |
| **1.0.0** | Production | Persistent queue satellite package (`resilient_mqtt_hive`), web support (`MqttBrowserClient`), dynamic credentials/token refresh, observability hooks, full dartdoc, migration guide |

## Comparison

- **`mqtt_client`** – the foundation; `resilient_mqtt` builds on it and turns
  off its native `autoReconnect` to take over reconnection.
- **`pulse_mqtt`** – similar goals but depends on Flutter and
  `connectivity_plus`; `resilient_mqtt` is pure Dart and Stream-based.
- **`kinnow_mqtt`** – its own MQTT 5 implementation; not a drop-in for
  existing `mqtt_client` users.

## Development

```sh
dart pub get
dart format --output=none --set-exit-if-changed .
dart analyze --fatal-infos
dart test -x integration                 # unit tests

# Integration tests need a broker on localhost:1883:
docker run -d --name mosquitto -p 1883:1883 \
  -v "$PWD/tool/mosquitto:/mosquitto/config:ro" eclipse-mosquitto:2
dart test -t integration --concurrency=1
```

The broker-restart test runs `docker stop|start mosquitto` by default;
override with `MQTT_BROKER_STOP_CMD` / `MQTT_BROKER_START_CMD`, or skip it
with `MQTT_SKIP_RESTART=1`.

## License

MIT © 2026 Gabriel Robinson. See [LICENSE](LICENSE).
