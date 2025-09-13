// fire_json_full.dart
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';

/// ----------------------- Utilities -----------------------
String _randId([int len = 20]) {
  const alphabet =
      '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  final rnd = Random.secure();
  return List<int>.generate(
    len,
    (_) => alphabet.codeUnitAt(rnd.nextInt(alphabet.length)),
  ).map((c) => String.fromCharCode(c)).join();
}

void _unawaited(Future<void> f) {}

const String _COMP_SEP = '\u241E'; // composite separator unlikely to appear

/// ----------------------- Primary NDJSON Store -----------------------
/// Manages data.ndjson and primary.idx.json (with prev-values tracking)
class JsonCollectionStore {
  final Directory dir;
  final File dataFile;
  final File primaryIndexFile;
  final Map<String, Map<String, dynamic>> primaryIndex =
      {}; // id -> {offset,length,version,tombstone, prev:{field:value}}
  int _ver = 1;

  JsonCollectionStore._(this.dir)
    : dataFile = File('${dir.path}/data.ndjson'),
      primaryIndexFile = File('${dir.path}/primary.idx.json');

  static Future<JsonCollectionStore> open({
    required Directory directory,
  }) async {
    final s = JsonCollectionStore._(directory);
    if (!await directory.exists()) await directory.create(recursive: true);
    if (!await s.dataFile.exists()) await s.dataFile.create();
    await s._loadPrimaryIndex();
    return s;
  }

  Future<void> _loadPrimaryIndex() async {
    if (!await primaryIndexFile.exists()) return;
    try {
      final txt = await primaryIndexFile.readAsString();
      final parsed = jsonDecode(txt) as Map<String, dynamic>;
      primaryIndex.clear();
      parsed.forEach((k, v) {
        primaryIndex[k] = Map<String, dynamic>.from(v as Map);
      });
      var maxv = 0;
      for (final v in primaryIndex.values) {
        if ((v['version'] ?? 0) is int && v['version'] > maxv)
          maxv = v['version'];
      }
      _ver = maxv + 1;
    } catch (e) {
      print('Primary index load failed: $e');
    }
  }

  Future<void> _flushPrimaryIndexAtomic() async {
    final tmp = File('${primaryIndexFile.path}.tmp');
    await tmp.writeAsString(jsonEncode(primaryIndex));
    await tmp.rename(primaryIndexFile.path);
  }

  Future<Map<String, dynamic>?> getById(String id) async {
    final meta = primaryIndex[id];
    if (meta == null || (meta['tombstone'] ?? 0) == 1) return null;
    final offset = meta['offset'] as int;
    final len = meta['length'] as int;
    final raf = await dataFile.open();
    await raf.setPosition(offset);
    final bytes = await raf.read(len);
    await raf.close();
    final line = utf8.decode(bytes).trim();
    try {
      return jsonDecode(line) as Map<String, dynamic>;
    } catch (e) {
      return null;
    }
  }

  bool existsLive(String id) {
    return primaryIndex.containsKey(id) &&
        (primaryIndex[id]!['tombstone'] ?? 0) == 0;
  }

  Future<void> appendRecord(
    Map<String, dynamic> obj, {
    Map<String, dynamic>? prevIndexedValues,
  }) async {
    final id = obj['id'].toString();
    final line = jsonEncode(obj);
    final bytes = utf8.encode(line);
    final raf = await dataFile.open(mode: FileMode.append);
    final offset = await raf.length();
    await raf.writeFrom(bytes);
    await raf.writeByte(10);
    await raf.close();

    primaryIndex[id] = {
      'offset': offset,
      'length': bytes.length + 1,
      'version': _ver++,
      'tombstone': obj.containsKey('_deleted') && obj['_deleted'] == true
          ? 1
          : 0,
      if (prevIndexedValues != null) 'prev': prevIndexedValues,
    };
    await _flushPrimaryIndexAtomic();
  }

  /// Rebuild primary index from scratch (useful for recovery)
  Future<void> rebuildPrimaryIndex() async {
    primaryIndex.clear();
    final stream = dataFile
        .openRead()
        .transform(utf8.decoder)
        .transform(LineSplitter());
    int offset = 0;
    int ver = 1;
    await for (final line in stream) {
      final len = utf8.encode(line).length + 1;
      try {
        final obj = jsonDecode(line) as Map<String, dynamic>;
        final id = obj['id'].toString();
        final tomb = obj['_deleted'] == true;
        primaryIndex[id] = {
          'offset': offset,
          'length': len,
          'version': ver++,
          'tombstone': tomb ? 1 : 0,
        };
      } catch (e) {}
      offset += len;
    }
    _ver = ver;
    await _flushPrimaryIndexAtomic();
  }

  /// Read all live docs (costly for many docs)
  Future<List<Map<String, dynamic>>> readAllLive() async {
    final out = <Map<String, dynamic>>[];
    for (final id in primaryIndex.keys) {
      final m = primaryIndex[id]!;
      if ((m['tombstone'] ?? 0) == 1) continue;
      final rec = await getById(id);
      if (rec != null) out.add(rec);
    }
    return out;
  }
}

/// ----------------------- Secondary / Composite Index Structure -----------------------
class SecondaryIndex {
  final List<String> keys = [];
  final Map<String, List<String>> postings = {}; // key -> list of ids

  SecondaryIndex();

  Map<String, dynamic> toJson() => {'keys': keys, 'postings': postings};

  static SecondaryIndex fromJson(Map<String, dynamic> j) {
    final s = SecondaryIndex();
    (j['keys'] as List<dynamic>? ?? []).forEach(
      (e) => s.keys.add(e.toString()),
    );
    final p = j['postings'] as Map<String, dynamic>? ?? {};
    p.forEach((k, v) {
      s.postings[k] = (v as List<dynamic>).map((e) => e.toString()).toList();
    });
    return s;
  }

  void add(String key, String id) {
    final list = postings.putIfAbsent(key, () {
      final pos = _findInsertPos(keys, key);
      keys.insert(pos, key);
      return <String>[];
    });
    if (list.isEmpty || list.last != id) {
      if (!list.contains(id)) list.add(id);
    }
  }

  void remove(String key, String id) {
    final p = postings[key];
    if (p == null) return;
    p.remove(id);
    if (p.isEmpty) {
      postings.remove(key);
      keys.remove(key);
    }
  }

  List<String> getExact(String key) => postings[key] ?? [];

  List<String> getRange(
    String? startKey,
    String? endKey, {
    bool startInclusive = true,
    bool endInclusive = true,
    int limit = 1000,
  }) {
    if (keys.isEmpty) return [];
    final startIdx = (startKey == null)
        ? 0
        : _lowerBound(keys, startKey, inclusive: startInclusive);
    final endIdx = (endKey == null)
        ? keys.length - 1
        : _upperBound(keys, endKey, inclusive: endInclusive);
    if (startIdx >= keys.length || endIdx < 0 || startIdx > endIdx) return [];
    final out = <String>[];
    for (int i = startIdx; i <= endIdx && i < keys.length; i++) {
      final k = keys[i];
      final p = postings[k];
      if (p != null) {
        out.addAll(p);
        if (out.length >= limit) {
          out.length = limit;
          break;
        }
      }
    }
    return out;
  }

  static int _findInsertPos(List<String> arr, String key) {
    int lo = 0, hi = arr.length;
    while (lo < hi) {
      final mid = (lo + hi) >> 1;
      if (arr[mid].compareTo(key) < 0)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  static int _lowerBound(
    List<String> arr,
    String key, {
    bool inclusive = true,
  }) {
    int lo = 0, hi = arr.length - 1, ans = arr.length;
    while (lo <= hi) {
      final mid = (lo + hi) >> 1;
      final cmp = arr[mid].compareTo(key);
      if (cmp > 0 || (!inclusive && cmp == 0)) {
        ans = mid;
        hi = mid - 1;
      } else {
        lo = mid + 1;
      }
    }
    return ans == arr.length ? arr.length : ans;
  }

  static int _upperBound(
    List<String> arr,
    String key, {
    bool inclusive = true,
  }) {
    int lo = 0, hi = arr.length - 1, ans = -1;
    while (lo <= hi) {
      final mid = (lo + hi) >> 1;
      final cmp = arr[mid].compareTo(key);
      if (cmp < 0 || (inclusive && cmp == 0)) {
        ans = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return ans;
  }
}

/// Metadata for each index
class IndexMeta {
  final List<String> fields;
  final Map<String, String> keyTypes;
  final bool ordered;
  IndexMeta({
    required this.fields,
    Map<String, String>? keyTypes,
    this.ordered = true,
  }) : keyTypes = keyTypes ?? {for (var f in fields) f: 'auto'};
  String name() => fields.join('__');
  String filename(String dirPath) {
    if (fields.length == 1)
      return '$dirPath/secondary_${fields.first}.idx.json';
    return '$dirPath/composite_${fields.join('__')}.idx.json';
  }
}

/// ----------------------- Index Manager -----------------------
class IndexManager {
  final JsonCollectionStore store;
  final Map<String, SecondaryIndex> loaded = {}; // indexName -> index
  final Map<String, IndexMeta> meta = {};

  IndexManager(this.store);

  String _canonicalKey(dynamic v, {String keyType = 'auto'}) {
    if (v == null) return '';
    if (keyType == 'auto') {
      if (v is num)
        keyType = 'num';
      else if (v is DateTime)
        keyType = 'date';
      else
        keyType = 'str';
    }
    if (keyType == 'num') {
      final n = (v as num).toInt();
      final offset = 1000000000000;
      final mapped = (n + offset).toString().padLeft(20, '0');
      return mapped;
    } else if (keyType == 'date') {
      final dt = v is DateTime ? v : DateTime.parse(v.toString());
      return dt.toIso8601String();
    } else {
      return v.toString();
    }
  }

  Future<void> ensureIndex(IndexMeta m, {bool useIsolate = true}) async {
    final name = m.name();
    meta[name] = m;
    if (loaded.containsKey(name)) return;
    final path = m.filename(store.dir.path);
    final f = File(path);
    if (await f.exists()) {
      try {
        final txt = await f.readAsString();
        loaded[name] = SecondaryIndex.fromJson(
          jsonDecode(txt) as Map<String, dynamic>,
        );
        return;
      } catch (e) {
        // fallthrough to rebuild
      }
    }
    // build index by scanning primary index; for heavy builds use isolate
    if (useIsolate) {
      final p = _IsolateIndexBuildRequest(
        store.dataFile.path,
        store.primaryIndex,
        m,
        path,
      );
      final result = await _runIsolateBuildIndex(p);
      loaded[name] = SecondaryIndex.fromJson(
        jsonDecode(result) as Map<String, dynamic>,
      );
      return;
    } else {
      // inline build
      final si = SecondaryIndex();
      for (final entry in store.primaryIndex.entries) {
        final id = entry.key;
        final pm = entry.value;
        if ((pm['tombstone'] ?? 0) == 1) continue;
        final rec = await store.getById(id);
        if (rec == null) continue;
        if (m.fields.length == 1) {
          final fkey = m.fields.first;
          if (rec.containsKey(fkey)) {
            final ck = _canonicalKey(
              rec[fkey],
              keyType: m.keyTypes[fkey] ?? 'auto',
            );
            si.add(ck, id);
          }
        } else {
          final parts = <String>[];
          for (final fkey in m.fields)
            parts.add(
              _canonicalKey(rec[fkey], keyType: m.keyTypes[fkey] ?? 'auto'),
            );
          final ck = parts.join(_COMP_SEP);
          si.add(ck, id);
        }
      }
      loaded[name] = si;
      await _flushIndex(m);
    }
  }

  Future<void> _flushIndex(IndexMeta m) async {
    final si = loaded[m.name()];
    if (si == null) return;
    final tmp = File(m.filename(store.dir.path) + '.tmp');
    await tmp.writeAsString(jsonEncode(si.toJson()));
    await tmp.rename(m.filename(store.dir.path));
  }

  /// Efficiently update indexes when a record is inserted/updated/deleted using prev-values stored in primary index
  Future<void> applyIndexChangesOnUpdate(
    String id,
    Map<String, dynamic>? prevIndexedValues,
    Map<String, dynamic>? newValues,
  ) async {
    // For each loaded index meta, remove from previous keys (if prev given) and add to new keys
    for (final kv in meta.entries) {
      final m = kv.value;
      final name = kv.key;
      final si = loaded[name];
      if (si == null) continue; // not loaded
      if (m.fields.length == 1) {
        final fkey = m.fields.first;
        final prevVal = prevIndexedValues == null
            ? null
            : prevIndexedValues[fkey];
        final newVal = newValues == null ? null : newValues[fkey];
        if (prevVal != null) {
          final pkey = _canonicalKey(
            prevVal,
            keyType: m.keyTypes[fkey] ?? 'auto',
          );
          si.remove(pkey, id);
        }
        if (newVal != null) {
          final nkey = _canonicalKey(
            newVal,
            keyType: m.keyTypes[fkey] ?? 'auto',
          );
          si.add(nkey, id);
        }
      } else {
        // composite
        final prevParts = <String>[];
        final newParts = <String>[];
        var hadPrev = true;
        for (final fkey in m.fields) {
          if (prevIndexedValues == null || !prevIndexedValues.containsKey(fkey))
            hadPrev = false;
          prevParts.add(
            hadPrev
                ? _canonicalKey(
                    prevIndexedValues?[fkey],
                    keyType: m.keyTypes[fkey] ?? 'auto',
                  )
                : '',
          );
          newParts.add(
            _canonicalKey(
              newValues == null ? null : newValues[fkey],
              keyType: m.keyTypes[fkey] ?? 'auto',
            ),
          );
        }
        if (hadPrev) {
          si.remove(prevParts.join(_COMP_SEP), id);
        }
        si.add(newParts.join(_COMP_SEP), id);
      }
      await _flushIndex(m);
    }
  }

  /// Save all loaded indexes (useful after bulk updates)
  Future<void> flushAll() async {
    for (final kv in meta.entries) {
      await _flushIndex(kv.value);
    }
  }

  /// Range/exact query helpers use loaded indexes
  SecondaryIndex? getLoaded(String name) => loaded[name];
}

/// ----------------------- Isolate helpers -----------------------
class _IsolateIndexBuildRequest {
  final String dataFilePath;
  final Map<String, Map<String, dynamic>> primaryIndexSnapshot;
  final IndexMeta meta;
  final String outPath;
  _IsolateIndexBuildRequest(
    this.dataFilePath,
    this.primaryIndexSnapshot,
    this.meta,
    this.outPath,
  );
}

Future<String> _runIsolateBuildIndex(_IsolateIndexBuildRequest req) async {
  final p = ReceivePort();
  await Isolate.spawn(_isolateBuildIndex, [p.sendPort, req]);
  final completer = Completer<String>();
  p.listen((msg) {
    if (msg is String) {
      completer.complete(msg);
      p.close();
    } else if (msg is List && msg.isNotEmpty && msg.first is String) {
      completer.complete(msg.first as String);
      p.close();
    } else if (msg is SendPort) {
      // ignore
    } else {
      // ignore
    }
  });
  return completer.future;
}

void _isolateBuildIndex(List<dynamic> args) async {
  final SendPort send = args[0] as SendPort;
  final _IsolateIndexBuildRequest req = args[1] as _IsolateIndexBuildRequest;
  final si = SecondaryIndex();
  // Open data file for reads
  final data = File(req.dataFilePath);
  final prim = req.primaryIndexSnapshot;
  // For each primary index entry, read record and add to index
  for (final entry in prim.entries) {
    final id = entry.key;
    final meta = entry.value;
    if ((meta['tombstone'] ?? 0) == 1) continue;
    try {
      final raf = await data.open();
      await raf.setPosition(meta['offset'] as int);
      final bytes = await raf.read(meta['length'] as int);
      await raf.close();
      final line = utf8.decode(bytes).trim();
      final obj = jsonDecode(line) as Map<String, dynamic>;
      if (req.meta.fields.length == 1) {
        final f = req.meta.fields.first;
        if (obj.containsKey(f)) {
          final k = _isoCanonicalKey(
            obj[f],
            keyType: req.meta.keyTypes[f] ?? 'auto',
          );
          si.add(k, id);
        }
      } else {
        final parts = <String>[];
        for (final f in req.meta.fields) {
          parts.add(
            _isoCanonicalKey(obj[f], keyType: req.meta.keyTypes[f] ?? 'auto'),
          );
        }
        si.add(parts.join(_COMP_SEP), id);
      }
    } catch (e) {
      // ignore bad lines
    }
  }
  final jsonOut = jsonEncode(si.toJson());
  // write output file for speed (optional)
  try {
    final out = File(req.outPath);
    await out.writeAsString(jsonOut);
  } catch (e) {}
  send.send(jsonOut);
}

String _isoCanonicalKey(dynamic v, {String keyType = 'auto'}) {
  if (v == null) return '';
  if (keyType == 'auto') {
    if (v is num)
      keyType = 'num';
    else
      keyType = 'str';
  }
  if (keyType == 'num') {
    final n = (v as num).toInt();
    final offset = 1000000000000;
    return (n + offset).toString().padLeft(20, '0');
  } else {
    return v.toString();
  }
}

/// ----------------------- Fire-like API (Collections, Docs, Snapshots) -----------------------
class FireJson {
  final Directory root;
  final Map<String, JsonCollectionStore> _stores = {};
  final Map<String, IndexManager> _indexManagers = {};
  final Map<String, StreamController<void>> _collectionControllers = {};
  final Map<String, Map<String, StreamController<DocumentSnapshot>>>
  _docControllers = {};
  final Map<String, StreamSubscription<FileSystemEvent>> _fileWatchSubs = {};

  FireJson._(this.root);

  static Future<FireJson> open(String rootPath) async {
    final dir = Directory(rootPath);
    if (!await dir.exists()) await dir.create(recursive: true);

    final f = FireJson._(dir);
    return f;
  }

  CollectionRef collection(String name) => CollectionRef(this, name);

  Future<JsonCollectionStore> _openCollectionStore(String collection) async {
    if (_stores.containsKey(collection)) return _stores[collection]!;
    final sub = Directory('${root.path}/$collection');
    final store = await JsonCollectionStore.open(directory: sub);
    _stores[collection] = store;
    _indexManagers[collection] = IndexManager(store);
    _collectionControllers.putIfAbsent(
      collection,
      () => StreamController<void>.broadcast(),
    );
    _docControllers.putIfAbsent(collection, () => {});
    _ensureFileWatcher(collection);
    return store;
  }

  IndexManager indexManager(String collection) {
    return _indexManagers[collection]!;
  }

  void emitDocChange(String collection, String id) {
    final collCtrl = _collectionControllers.putIfAbsent(
      collection,
      () => StreamController<void>.broadcast(),
    );
    try {
      collCtrl.add(null);
    } catch (e) {}
    final docMap = _docControllers.putIfAbsent(collection, () => {});
    final docCtrl = docMap[id];
    if (docCtrl != null && !docCtrl.isClosed) {
      _unawaited(_pushDocSnapshot(collection, id, docCtrl));
    }
  }

  Future<void> _pushDocSnapshot(
    String collection,
    String id,
    StreamController<DocumentSnapshot> ctrl,
  ) async {
    final store = await _openCollectionStore(collection);
    final rec = await store.getById(id);
    final snap = DocumentSnapshot._(id, rec);
    try {
      ctrl.add(snap);
    } catch (e) {}
  }

  // Stream<DocumentSnapshot> _docStream(String collection, String id) {
  //   final docMap = _docControllers.putIfAbsent(collection, () => {});
  //   final ctrl = docMap.putIfAbsent(
  //     id,
  //     () => StreamController<DocumentSnapshot>.broadcast(
  //       onListen: () async {
  //         _unawaited(_pushDocSnapshot(collection, id, ctrl));
  //       },
  //     ),
  //   );
  //   return ctrl.stream;
  // }

  Stream<DocumentSnapshot> _docStream(String collection, String id) {
    final docMap = _docControllers.putIfAbsent(
      collection,
      () => <String, StreamController<DocumentSnapshot>>{},
    );

    // If controller already exists, reuse it
    if (docMap.containsKey(id)) {
      return docMap[id]!.stream;
    }

    late StreamController<DocumentSnapshot> ctrl;

    ctrl = StreamController<DocumentSnapshot>.broadcast(
      onListen: () {
        _unawaited(_pushDocSnapshot(collection, id, ctrl));
      },
      onCancel: () {
        // Optional: clean up when no more listeners
        // docMap.remove(id);
        // ctrl.close();
      },
    );

    docMap[id] = ctrl;
    return ctrl.stream;
  }

  Stream<void> collectionStream(String collection) {
    final ctrl = _collectionControllers.putIfAbsent(
      collection,
      () => StreamController<void>.broadcast(),
    );
    return ctrl.stream;
  }

  void _ensureFileWatcher(String collection) {
    if (_fileWatchSubs.containsKey(collection)) return;
    final subDir = Directory('${root.path}/$collection');
    try {
      final sub = subDir.watch(
        events:
            FileSystemEvent.modify |
            FileSystemEvent.create |
            FileSystemEvent.delete,
      );
      final subsc = sub.listen((ev) {
        // on any file change we emit collection-level event to pick up cross-process changes
        final collCtrl = _collectionControllers.putIfAbsent(
          collection,
          () => StreamController<void>.broadcast(),
        );
        try {
          collCtrl.add(null);
        } catch (e) {}
      });
      _fileWatchSubs[collection] = subsc;
    } catch (e) {
      // if file watch not available, continue without cross-process notifications
    }
  }
}

/// Document snapshot
class DocumentSnapshot {
  final String id;
  final Map<String, dynamic>? data;
  DocumentSnapshot._(this.id, this.data);
  bool exists() => data != null;
}

/// Query snapshot
class QuerySnapshot {
  final List<DocumentSnapshot> docs;
  QuerySnapshot(this.docs);
}

/// DocumentRef
class DocumentRef {
  final FireJson _fire;
  final String collectionName;
  final String id;
  DocumentRef(this._fire, this.collectionName, this.id);

  Future<DocumentSnapshot> get() async {
    final store = await _fire._openCollectionStore(collectionName);
    final rec = await store.getById(id);
    return DocumentSnapshot._(id, rec);
  }

  Future<void> set(Map<String, dynamic> data, {bool merge = false}) async {
    final store = await _fire._openCollectionStore(collectionName);
    final exists = store.existsLive(id);
    if (!exists) {
      final obj = Map<String, dynamic>.from(data)..['id'] = id;
      // prev values null
      await store.appendRecord(obj, prevIndexedValues: null);
      // update indexes if loaded
      await _fire
          .indexManager(collectionName)
          .applyIndexChangesOnUpdate(id, null, obj);
    } else {
      if (!merge) {
        final obj = Map<String, dynamic>.from(data)..['id'] = id;
        final prevIndexed =
            store.primaryIndex[id]?['prev'] as Map<String, dynamic>?;
        await store.appendRecord(obj, prevIndexedValues: prevIndexed);
        await _fire
            .indexManager(collectionName)
            .applyIndexChangesOnUpdate(id, prevIndexed, obj);
      } else {
        final cur = await store.getById(id) ?? <String, dynamic>{};
        final merged = Map<String, dynamic>.from(cur)
          ..addAll(data)
          ..['id'] = id;
        final prevIndexed =
            store.primaryIndex[id]?['prev'] as Map<String, dynamic>?;
        await store.appendRecord(merged, prevIndexedValues: prevIndexed);
        await _fire
            .indexManager(collectionName)
            .applyIndexChangesOnUpdate(id, prevIndexed, merged);
      }
    }
    _fire.emitDocChange(collectionName, id);
  }

  Future<void> update(Map<String, dynamic> data) async {
    final store = await _fire._openCollectionStore(collectionName);
    if (!store.existsLive(id)) throw StateError('Document $id does not exist');
    final cur = await store.getById(id) ?? <String, dynamic>{};
    final merged = Map<String, dynamic>.from(cur)
      ..addAll(data)
      ..['id'] = id;
    final prevIndexed =
        store.primaryIndex[id]?['prev'] as Map<String, dynamic>?;
    await store.appendRecord(merged, prevIndexedValues: prevIndexed);
    await _fire
        .indexManager(collectionName)
        .applyIndexChangesOnUpdate(id, prevIndexed, merged);
    _fire.emitDocChange(collectionName, id);
  }

  Future<void> delete() async {
    final store = await _fire._openCollectionStore(collectionName);
    if (!store.existsLive(id)) return;
    final prevIndexed =
        store.primaryIndex[id]?['prev'] as Map<String, dynamic>?;
    final tomb = {'id': id, '_deleted': true};
    await store.appendRecord(tomb, prevIndexedValues: prevIndexed);
    // remove from indexes using prevIndexed
    await _fire
        .indexManager(collectionName)
        .applyIndexChangesOnUpdate(id, prevIndexed, null);
    _fire.emitDocChange(collectionName, id);
  }

  Stream<DocumentSnapshot> snapshots() {
    return _fire._docStream(collectionName, id);
  }
}

/// Query descriptor (where clauses, orderBy, limit)
class QueryDescriptor {
  final List<Map<String, dynamic>> whereClauses;
  final List? orderBy;
  final int? limit;
  QueryDescriptor({this.whereClauses = const [], this.orderBy, this.limit});
}

/// CollectionRef
class CollectionRef {
  final FireJson _fire;
  final String name;
  CollectionRef(this._fire, this.name);

  DocumentRef doc([String? id]) {
    return DocumentRef(_fire, name, id ?? _randId());
  }

  Future<String> add(Map<String, dynamic> data) async {
    final id = _randId();
    final store = await _fire._openCollectionStore(name);
    final obj = Map<String, dynamic>.from(data)..['id'] = id;
    await store.appendRecord(obj, prevIndexedValues: null);
    await _fire.indexManager(name).applyIndexChangesOnUpdate(id, null, obj);
    _fire.emitDocChange(name, id);
    return id;
  }

  /// Get supports naive mode (reads all) OR indexed mode if indexes are present and match.
  Future<QuerySnapshot> get([QueryDescriptor? qd]) async {
    final store = await _fire._openCollectionStore(name);
    final idx = _fire.indexManager(name);

    qd ??= QueryDescriptor();

    // Planner: try composite full-match or prefix; else single-field ordered; else equality intersection; fallback full scan.
    // Find best composite
    IndexMeta? bestComposite;
    for (final m in idx.meta.values) {
      if (m.fields.length <= 1) continue;
      var ok = true;
      for (final f in m.fields) {
        if (!qd.whereClauses.any((c) => c['field'] == f)) {
          ok = false;
          break;
        }
      }
      if (ok) {
        bestComposite = m;
        break;
      }
    }

    if (bestComposite != null) {
      await idx.ensureIndex(bestComposite);
      final si = idx.getLoaded(bestComposite.name())!;
      // build start & end composite keys
      final startParts = <String>[];
      final endParts = <String>[];
      for (final f in bestComposite.fields) {
        final clause = qd.whereClauses.firstWhere((c) => c['field'] == f);
        final op = clause['op'];
        final val = clause['value'];
        final endVal = clause['endValue'];
        startParts.add(
          op == '==' || op == '>='
              ? idx._canonicalKey(
                  val,
                  keyType: bestComposite.keyTypes[f] ?? 'auto',
                )
              : '',
        );
        endParts.add(
          op == '==' || op == '<='
              ? idx._canonicalKey(
                  val,
                  keyType: bestComposite.keyTypes[f] ?? 'auto',
                )
              : (endVal != null
                    ? idx._canonicalKey(
                        endVal,
                        keyType: bestComposite.keyTypes[f] ?? 'auto',
                      )
                    : '\uFFFF'),
        );
      }
      final ids = si.getRange(
        startParts.join(_COMP_SEP),
        endParts.join(_COMP_SEP),
        limit: qd.limit ?? 1000,
      );
      final docs = <DocumentSnapshot>[];
      for (final id in ids) {
        final rec = await store.getById(id);
        if (rec != null) docs.add(DocumentSnapshot._(id, rec));
      }
      return QuerySnapshot(docs);
    }

    // Try orderBy single-field index
    if (qd.orderBy != null && qd.orderBy!.isNotEmpty) {
      final orderField = qd.orderBy![0].toString();
      if (idx.meta.containsKey(orderField)) {
        final m = idx.meta[orderField]!;
        await idx.ensureIndex(m);
        final si = idx.getLoaded(m.name())!;
        // if where clause on orderField exists, use range
        final where = qd.whereClauses.firstWhere(
          (c) => c['field'] == orderField,
          orElse: () => {},
        );
        if (where.isNotEmpty) {
          final op = where['op'];
          final v = where['value'];
          final ev = where['endValue'];
          String? startK, endK;
          if (op == '==')
            startK = endK = idx._canonicalKey(
              v,
              keyType: m.keyTypes[orderField] ?? 'auto',
            );
          else if (op == '>=' || op == '>')
            startK = idx._canonicalKey(
              v,
              keyType: m.keyTypes[orderField] ?? 'auto',
            );
          else if (op == '<=' || op == '<')
            endK = idx._canonicalKey(
              v,
              keyType: m.keyTypes[orderField] ?? 'auto',
            );
          else if (op == 'range') {
            startK = idx._canonicalKey(
              v,
              keyType: m.keyTypes[orderField] ?? 'auto',
            );
            endK = idx._canonicalKey(
              ev,
              keyType: m.keyTypes[orderField] ?? 'auto',
            );
          }
          final ids = si.getRange(startK, endK, limit: qd.limit ?? 1000);
          final docs = <DocumentSnapshot>[];
          for (final id in ids) {
            final rec = await store.getById(id);
            if (rec != null) docs.add(DocumentSnapshot._(id, rec));
          }
          return QuerySnapshot(docs);
        } else {
          final ids = si.getRange(null, null, limit: qd.limit ?? 1000);
          final docs = <DocumentSnapshot>[];
          for (final id in ids) {
            final rec = await store.getById(id);
            if (rec != null) docs.add(DocumentSnapshot._(id, rec));
          }
          return QuerySnapshot(docs);
        }
      }
    }

    // Fallback: equality intersection using available single-field indexes (or full scan)
    final equalityClauses = qd.whereClauses
        .where((c) => c['op'] == '==')
        .toList();
    if (equalityClauses.isNotEmpty) {
      final candidateLists = <List<String>>[];
      for (final c in equalityClauses) {
        final f = c['field'].toString();
        if (!idx.meta.containsKey(f)) {
          await idx.ensureIndex(
            IndexMeta(fields: [f], keyTypes: {f: 'auto'}, ordered: false),
          );
        }
        final m = idx.meta[f]!;
        final si = idx.getLoaded(m.name())!;
        final key = idx._canonicalKey(
          c['value'],
          keyType: m.keyTypes[f] ?? 'auto',
        );
        candidateLists.add(si.getExact(key));
      }
      if (candidateLists.isEmpty) return QuerySnapshot([]);
      candidateLists.sort((a, b) => a.length.compareTo(b.length));
      var res = candidateLists.first.toSet();
      for (int i = 1; i < candidateLists.length; i++)
        res = res.intersection(candidateLists[i].toSet());
      final ids = res.take(qd.limit ?? res.length).toList();
      final docs = <DocumentSnapshot>[];
      for (final id in ids) {
        final rec = await store.getById(id);
        if (rec != null) docs.add(DocumentSnapshot._(id, rec));
      }
      return QuerySnapshot(docs);
    }

    // Last fallback: full scan
    final all = await store.readAllLive();
    final filtered = <Map<String, dynamic>>[];
    for (final m in all) {
      var ok = true;
      for (final c in qd.whereClauses) {
        final lhs = m[c['field']];
        if (!_evalOp(lhs, c['op'], c['value'], c['endValue'])) {
          ok = false;
          break;
        }
      }
      if (ok) filtered.add(m);
    }
    // ordering
    if (qd.orderBy != null) {
      final field = qd.orderBy![0].toString();
      final desc = (qd.orderBy!.length > 1) ? (qd.orderBy![1] == true) : false;
      filtered.sort((a, b) {
        final av = a[field];
        final bv = b[field];
        if (av == null && bv == null) return 0;
        if (av == null) return -1;
        if (bv == null) return 1;
        final cmp = (av is Comparable && bv is Comparable)
            ? av.compareTo(bv)
            : av.toString().compareTo(bv.toString());
        return desc ? -cmp : cmp;
      });
    }
    final limit = qd.limit ?? filtered.length;
    final docs = filtered
        .take(limit)
        .map((m) => DocumentSnapshot._(m['id'].toString(), m))
        .toList();
    return QuerySnapshot(docs);
  }

  Stream<QuerySnapshot> snapshots([QueryDescriptor? qd]) {
    final name = nameOfCollectionStreamKey(this);
    // Here we map collection-level events to snapshots
    // But simpler: the stream on FireJson._collectionStream(name) is returned as asyncMap that runs get()
    final stream = StreamController<QuerySnapshot>.broadcast();
    _unawaited(_openAndForwardCollectionStream(name, qd, stream));
    return stream.stream;
  }

  Future<void> _openAndForwardCollectionStream(
    String collectionName,
    QueryDescriptor? qd,
    StreamController<QuerySnapshot> target,
  ) async {
    final store = await _fire._openCollectionStore(
      collectionName,
    ); // ensure store opened and watcher attached
    final sub = _fire.collectionStream(collectionName).listen((_) async {
      try {
        final snap = await get(qd);
        target.add(snap);
      } catch (e) {}
    });
    // push initial snapshot
    try {
      final snap = await get(qd);
      target.add(snap);
    } catch (e) {}
    // keep stream alive until canceled
    target.onCancel = () {
      sub.cancel();
    };
  }

  // helper to build consistent key (for internal stream map)
  String nameOfCollectionStreamKey(CollectionRef cr) => cr.name;
}

/// Helper: eval op
bool _evalOp(dynamic lhs, String op, dynamic v, [dynamic ev]) {
  if (lhs == null) return false;
  try {
    if (op == '==') return lhs == v;
    if (op == '>=') return (lhs as Comparable).compareTo(v) >= 0;
    if (op == '>') return (lhs as Comparable).compareTo(v) > 0;
    if (op == '<=') return (lhs as Comparable).compareTo(v) <= 0;
    if (op == '<') return (lhs as Comparable).compareTo(v) < 0;
    if (op == 'range' && ev != null) {
      return (lhs as Comparable).compareTo(v) >= 0 &&
          (lhs as Comparable).compareTo(ev) <= 0;
    }
  } catch (e) {
    return false;
  }
  return false;
}
