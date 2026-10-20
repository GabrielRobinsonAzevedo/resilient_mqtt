import 'package:resilient_mqtt/resilient_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('topicMatches', () {
    final cases = <(String, String, bool)>[
      ('a/b/c', 'a/b/c', true),
      ('a/b/c', 'a/b/d', false),
      ('a/+/c', 'a/b/c', true),
      ('a/+/c', 'a/b/x/c', false),
      ('a/+', 'a/', true),
      ('+/+', '/finance', true),
      ('a/#', 'a', true),
      ('a/#', 'a/b/c', true),
      ('#', 'anything/at/all', true),
      ('a/b', 'a/b/c', false),
      ('a/b/c', 'a/b', false),
      ('#', r'$SYS/broker/uptime', false),
      ('+/broker/uptime', r'$SYS/broker/uptime', false),
      (r'$SYS/#', r'$SYS/broker/uptime', true),
    ];
    for (final (filter, topic, expected) in cases) {
      test('"$filter" ${expected ? 'matches' : 'does not match'} "$topic"', () {
        expect(topicMatches(filter, topic), expected);
      });
    }
  });
}
