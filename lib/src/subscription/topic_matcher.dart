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
      return i == f.length - 1;
    }
    if (i >= t.length) return false;
    if (level != '+' && level != t[i]) return false;
  }
  return f.length == t.length;
}
