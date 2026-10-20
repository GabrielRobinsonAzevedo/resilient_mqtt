## 0.0.1

- Initial scaffold (early development, API unstable).
- `ConnectionState` sealed hierarchy exposed as a `Stream`.
- `ReconnectPolicy` interface and `ExponentialBackoff` with full jitter.
- `ResilientMqttClient` wrapping `MqttServerClient` with backoff-driven
  reconnection and automatic re-subscribe.
- `MqttDecoder<T>` with string, JSON and bytes decoders; typed subscriptions.
