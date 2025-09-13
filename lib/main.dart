// lib/main.dart
import 'dart:io';
import 'dart:async';
import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import 'fire_json_full.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(MyApp());
}

class MyApp extends StatefulWidget {
  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  late Future<FireJson> _fireFuture;

  @override
  void initState() {
    super.initState();
    _fireFuture = _initStore();
  }

  Future<FireJson> _initStore() async {
    final docDir = await getApplicationDocumentsDirectory();
    final rootPath = Directory('${docDir.path}/firejson_example');
    print('Using FireJson root path: ${rootPath.path}');
    if (!await rootPath.exists()) await rootPath.create(recursive: true);

    // Open FireJson pointing at the folder
    final fire = await FireJson.open(rootPath.path);

    // Try to create indexes but don't let failures break startup.
    // Index builds can be heavy and might throw on first run if store hasn't created files yet.
    try {
      final idxMgr = fire.indexManager('users');

      // Wrap each ensureIndex in try/catch so one bad index won't stop everything.
      try {
        await idxMgr.ensureIndex(
          IndexMeta(
            fields: ['name'],
            keyTypes: {'name': 'str'},
            ordered: false,
          ),
        );
      } catch (e, st) {
        print('ensureIndex(name) failed: $e\n$st');
      }

      try {
        await idxMgr.ensureIndex(
          IndexMeta(
            fields: ['email'],
            keyTypes: {'email': 'str'},
            ordered: false,
          ),
        );
      } catch (e, st) {
        print('ensureIndex(email) failed: $e\n$st');
      }

      try {
        await idxMgr.ensureIndex(
          IndexMeta(fields: ['age'], keyTypes: {'age': 'num'}, ordered: true),
        );
      } catch (e, st) {
        print('ensureIndex(age) failed: $e\n$st');
      }

      try {
        await idxMgr.ensureIndex(
          IndexMeta(
            fields: ['createdAt'],
            keyTypes: {'createdAt': 'date'},
            ordered: true,
          ),
        );
      } catch (e, st) {
        print('ensureIndex(createdAt) failed: $e\n$st');
      }

      try {
        await idxMgr.ensureIndex(
          IndexMeta(
            fields: ['age', 'createdAt'],
            keyTypes: {'age': 'num', 'createdAt': 'date'},
            ordered: true,
          ),
        );
      } catch (e, st) {
        print('ensureIndex(age_createdAt) failed: $e\n$st');
      }
    } catch (outerEx, outerSt) {
      // Something unexpected when accessing index manager; we still want to continue with the app.
      print(
        'Index manager / index creation failed (continuing with empty store): $outerEx\n$outerSt',
      );
    }

    return fire;
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'FireJson Example',
      theme: ThemeData(primarySwatch: Colors.indigo),
      home: FutureBuilder<FireJson>(
        future: _fireFuture,
        builder: (context, snap) {
          if (snap.connectionState != ConnectionState.done) {
            return Scaffold(body: Center(child: CircularProgressIndicator()));
          }

          if (snap.hasError) {
            // print full stack for debugging
            print('FireJson.open failed: ${snap.error}');
            if (snap.stackTrace != null) print(snap.stackTrace);

            return Scaffold(
              body: Center(
                child: Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Text(
                    'Error initializing store:\n${snap.error}\n\nCheck console for full stack trace. The app will still run but indexing may be disabled.',
                    textAlign: TextAlign.center,
                  ),
                ),
              ),
            );
          }

          if (!snap.hasData || snap.data == null) {
            // This should be rare because FireJson.open returns an instance,
            // but handle defensively.
            return Scaffold(
              body: Center(
                child: Text(
                  'Initializing store... (no store instance returned)',
                ),
              ),
            );
          }

          final fire = snap.data!; // now safe
          return HomePage(fire: fire);
        },
      ),
    );
  }
}

class HomePage extends StatefulWidget {
  final FireJson fire;
  HomePage({required this.fire});
  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  int _selectedIndex = 0;

  // simple navigation between two pages
  static const List<Widget> _pagesPlaceholder = [
    SizedBox.shrink(), // will be replaced
    SizedBox.shrink(),
  ];

  @override
  Widget build(BuildContext context) {
    final pages = [
      DataTablePage(fire: widget.fire),
      FormPage(fire: widget.fire),
    ];
    return Scaffold(
      appBar: AppBar(title: Text('FireJson Example — Users')),
      body: pages[_selectedIndex],
      bottomNavigationBar: BottomNavigationBar(
        currentIndex: _selectedIndex,
        items: [
          BottomNavigationBarItem(
            icon: Icon(Icons.table_chart),
            label: 'Table',
          ),
          BottomNavigationBarItem(
            icon: Icon(Icons.add_box),
            label: 'Add / Form',
          ),
        ],
        onTap: (i) => setState(() => _selectedIndex = i),
      ),
    );
  }
}

/// Data table page: shows users collection records in a DataTable
/// Data table page: shows users collection records in a DataTable
/// Data table page: shows users collection records in a DataTable
class DataTablePage extends StatefulWidget {
  final FireJson fire;
  DataTablePage({required this.fire});

  @override
  State<DataTablePage> createState() => _DataTablePageState();
}

class _DataTablePageState extends State<DataTablePage> {
  List<Map<String, dynamic>> _rows = [];
  bool _loading = false;

  StreamSubscription<void>? _collSub;
  bool _refreshInProgress = false;

  // Filters
  final _nameFilterCtrl = TextEditingController();
  final _emailFilterCtrl = TextEditingController();
  final _minAgeCtrl = TextEditingController();

  @override
  void initState() {
    super.initState();
    _refresh();
    _collSub = widget.fire.collectionStream('users').listen((_) {
      if (!_refreshInProgress) _refresh();
    });
  }

  @override
  void dispose() {
    _collSub?.cancel();
    _nameFilterCtrl.dispose();
    _emailFilterCtrl.dispose();
    _minAgeCtrl.dispose();
    super.dispose();
  }

  Future<void> _refresh() async {
    if (!mounted) return;
    _refreshInProgress = true;
    if (mounted) setState(() => _loading = true);

    try {
      final users = widget.fire.collection('users');
      final snap = await users.get();
      var rows = snap.docs.map((d) => d.data ?? <String, dynamic>{}).toList();

      // Filters
      final nameFilter = _nameFilterCtrl.text.trim().toLowerCase();
      final emailFilter = _emailFilterCtrl.text.trim().toLowerCase();
      final minAge = int.tryParse(_minAgeCtrl.text.trim());

      if (nameFilter.isNotEmpty) {
        rows = rows.where((r) {
          final v = (r['name'] ?? '').toString().toLowerCase();
          return v.contains(nameFilter);
        }).toList();
      }
      if (emailFilter.isNotEmpty) {
        rows = rows.where((r) {
          final v = (r['email'] ?? '').toString().toLowerCase();
          return v.contains(emailFilter);
        }).toList();
      }
      if (minAge != null) {
        rows = rows.where((r) {
          final a = r['age'];
          if (a == null) return false;
          final ai = (a is num) ? a.toInt() : int.tryParse(a.toString());
          if (ai == null) return false;
          return ai >= minAge;
        }).toList();
      }

      // Sort
      rows.sort((a, b) {
        final av = a['createdAt'];
        final bv = b['createdAt'];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return b['createdAt'].toString().compareTo(a['createdAt'].toString());
      });

      if (!mounted) return;
      setState(() {
        _rows = rows;
      });
    } catch (e, st) {
      print('Refresh error: $e\n$st');
    } finally {
      _refreshInProgress = false;
      if (mounted) setState(() => _loading = false);
    }
  }

  void _applyFilters() => _refresh();
  void _clearFilters() {
    _nameFilterCtrl.clear();
    _emailFilterCtrl.clear();
    _minAgeCtrl.clear();
    _refresh();
  }

  Future<void> _editRow(Map<String, dynamic> row) async {
    final id = row['id'];
    if (id == null) return;

    final nameCtrl = TextEditingController(text: row['name']?.toString() ?? '');
    final emailCtrl = TextEditingController(
      text: row['email']?.toString() ?? '',
    );
    final ageCtrl = TextEditingController(text: row['age']?.toString() ?? '');

    await showDialog(
      context: context,
      builder: (ctx) {
        return AlertDialog(
          title: Text('Edit User'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                TextField(
                  controller: nameCtrl,
                  decoration: InputDecoration(labelText: 'Name'),
                ),
                TextField(
                  controller: emailCtrl,
                  decoration: InputDecoration(labelText: 'Email'),
                ),
                TextField(
                  controller: ageCtrl,
                  decoration: InputDecoration(labelText: 'Age'),
                  keyboardType: TextInputType.number,
                ),
              ],
            ),
          ),
          actions: [
            TextButton(
              child: Text('Cancel'),
              onPressed: () => Navigator.pop(ctx),
            ),
            ElevatedButton(
              child: Text('Save'),
              onPressed: () async {
                final name = nameCtrl.text.trim();
                final email = emailCtrl.text.trim();
                final age = int.tryParse(ageCtrl.text.trim()) ?? 0;

                try {
                  final users = widget.fire.collection('users');
                  await users.doc(id).set({
                    'name': name,
                    'email': email,
                    'age': age,
                  }, merge: true);

                  widget.fire.emitDocChange('users', id);
                  Navigator.pop(ctx);
                } catch (e) {
                  print('Edit failed: $e');
                }
              },
            ),
          ],
        );
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        // Filters
        Padding(
          padding: const EdgeInsets.all(8.0),
          child: Column(
            children: [
              Row(
                children: [
                  Expanded(
                    child: TextField(
                      controller: _nameFilterCtrl,
                      decoration: InputDecoration(
                        labelText: 'Name contains',
                        prefixIcon: Icon(Icons.person_search),
                      ),
                      onSubmitted: (_) => _applyFilters(),
                    ),
                  ),
                  SizedBox(width: 8),
                  Expanded(
                    child: TextField(
                      controller: _emailFilterCtrl,
                      decoration: InputDecoration(
                        labelText: 'Email contains',
                        prefixIcon: Icon(Icons.email),
                      ),
                      onSubmitted: (_) => _applyFilters(),
                    ),
                  ),
                  SizedBox(width: 8),
                  SizedBox(
                    width: 120,
                    child: TextField(
                      controller: _minAgeCtrl,
                      decoration: InputDecoration(
                        labelText: 'Min age',
                        prefixIcon: Icon(Icons.timer),
                      ),
                      keyboardType: TextInputType.number,
                      onSubmitted: (_) => _applyFilters(),
                    ),
                  ),
                ],
              ),
              SizedBox(height: 8),
              Row(
                children: [
                  ElevatedButton.icon(
                    onPressed: _applyFilters,
                    icon: Icon(Icons.search),
                    label: Text('Apply'),
                  ),
                  SizedBox(width: 8),
                  OutlinedButton.icon(
                    onPressed: _clearFilters,
                    icon: Icon(Icons.clear),
                    label: Text('Clear'),
                  ),
                  SizedBox(width: 16),
                  ElevatedButton.icon(
                    onPressed: _refresh,
                    icon: Icon(Icons.refresh),
                    label: Text('Refresh'),
                  ),
                  SizedBox(width: 12),
                  Text('Total: ${_rows.length}'),
                ],
              ),
            ],
          ),
        ),

        // Table
        Expanded(
          child: _loading
              ? Center(child: CircularProgressIndicator())
              : SingleChildScrollView(
                  scrollDirection: Axis.horizontal,
                  child: DataTable(
                    columns: const [
                      DataColumn(label: Text('ID')),
                      DataColumn(label: Text('Name')),
                      DataColumn(label: Text('Email')),
                      DataColumn(label: Text('Age')),
                      DataColumn(label: Text('CreatedAt')),
                      DataColumn(label: Text('Actions')),
                    ],
                    rows: _rows.map((r) {
                      return DataRow(
                        cells: [
                          DataCell(Text(r['id']?.toString() ?? '-')),
                          DataCell(Text(r['name']?.toString() ?? '-')),
                          DataCell(Text(r['email']?.toString() ?? '-')),
                          DataCell(Text(r['age']?.toString() ?? '-')),
                          DataCell(Text(r['createdAt']?.toString() ?? '-')),
                          DataCell(
                            IconButton(
                              icon: Icon(Icons.edit),
                              tooltip: 'Edit',
                              onPressed: () => _editRow(r),
                            ),
                          ),
                        ],
                      );
                    }).toList(),
                  ),
                ),
        ),
      ],
    );
  }
}

/// Form page: add a new user
class FormPage extends StatefulWidget {
  final FireJson fire;
  FormPage({required this.fire});

  @override
  State<FormPage> createState() => _FormPageState();
}

class _FormPageState extends State<FormPage> {
  final _formKey = GlobalKey<FormState>();
  final _nameCtrl = TextEditingController();
  final _emailCtrl = TextEditingController();
  final _ageCtrl = TextEditingController();

  bool _submitting = false;
  String _status = '';

  Future<void> _submit() async {
    if (!_formKey.currentState!.validate()) return;
    setState(() {
      _submitting = true;
      _status = '';
    });
    try {
      final name = _nameCtrl.text.trim();
      final email = _emailCtrl.text.trim();
      final age = int.tryParse(_ageCtrl.text.trim()) ?? 0;
      final createdAt = DateTime.now().toUtc().toIso8601String();
      final users = widget.fire.collection('users');

      final id = await users.add({
        'name': name,
        'email': email,
        'age': age,
        'createdAt': createdAt,
      });

      setState(() {
        _status = 'Saved (id=$id)';
        _nameCtrl.clear();
        _emailCtrl.clear();
        _ageCtrl.clear();
      });
      // notify collection listeners (store.add already emits, but safe to ensure)
      widget.fire.emitDocChange('users', id);
    } catch (e) {
      setState(() => _status = 'Save failed: $e');
    } finally {
      setState(() => _submitting = false);
    }
  }

  @override
  void dispose() {
    _nameCtrl.dispose();
    _emailCtrl.dispose();
    _ageCtrl.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      padding: const EdgeInsets.all(20),
      child: Column(
        children: [
          Form(
            key: _formKey,
            child: Column(
              children: [
                TextFormField(
                  controller: _nameCtrl,
                  decoration: InputDecoration(labelText: 'Name'),
                  validator: (v) =>
                      (v == null || v.trim().isEmpty) ? 'Required' : null,
                ),
                TextFormField(
                  controller: _emailCtrl,
                  decoration: InputDecoration(labelText: 'Email'),
                  validator: (v) =>
                      (v == null || v.trim().isEmpty) ? 'Required' : null,
                ),
                TextFormField(
                  controller: _ageCtrl,
                  decoration: InputDecoration(labelText: 'Age'),
                  keyboardType: TextInputType.number,
                  validator: (v) {
                    if (v == null || v.trim().isEmpty) return 'Required';
                    final n = int.tryParse(v.trim());
                    if (n == null) return 'Invalid number';
                    return null;
                  },
                ),
                SizedBox(height: 16),
                Row(
                  children: [
                    ElevatedButton.icon(
                      onPressed: _submitting ? null : _submit,
                      icon: Icon(Icons.save),
                      label: Text(_submitting ? 'Saving...' : 'Save'),
                    ),
                    SizedBox(width: 12),
                    Text(_status),
                  ],
                ),
              ],
            ),
          ),
          SizedBox(height: 24),
          // quick sample: prefill with sample data button
          ElevatedButton(
            onPressed: () {
              _nameCtrl.text = 'Sample User';
              _emailCtrl.text = 'sample@example.com';
              _ageCtrl.text = '27';
            },
            child: Text('Fill sample'),
          ),
        ],
      ),
    );
  }
}
