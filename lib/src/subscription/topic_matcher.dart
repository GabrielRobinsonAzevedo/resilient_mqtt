/// MQTT topic filter matching (MQTT 3.1.1, section 4.7).
///
/// Supports the single-level (`+`) and multi-level (`#`) wildcards. Topics
/// starting with `$` (e.g. `$SYS/...`) are not matched by filters starting
/// with a wildcard.
bool topicMatches(String filter, String topic) {
  if (filter == topic) return true;
  if (topic.startsWith(r'$') &&
      (filter.startsWith('+') || filter.startsWith('#'))) {
    return false;
  }
  final f = filter.split('/');
  final t = topic.split('/');
  for (var i = 0; i < f.length; i++) {
    final level = f[i];
    if (level == '#') {
      // '#' must be last; it also matches the parent level ("a/#" ~ "a").
      return i == f.length - 1;
    }
    if (i >= t.length) return false;
    if (level != '+' && level != t[i]) return false;
  }
  return f.length == t.length;
}
