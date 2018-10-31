var tape = require('tape')
var Readable = require('stream').Readable

var create = require('./helpers/create')
var run = require('./helpers/run')

var EthCrypto = require('eth-crypto');
var identity = EthCrypto.createIdentity();

tape('basic put/get', function (t) {
	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'world';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.same(node.key, key1)
		t.same(node.value, value1)
		t.error(err, 'no error')
		db.get(key1, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key1, 'same key')
			t.same(node.value, value1, 'same value')
			t.end()
		})
	})
})

tape('get on empty db', function (t) {
	var db = create.one()

	db.get('hello', function (err, node) {
		t.error(err, 'no error')
		t.same(node, null, 'node is not found')
		t.end()
	})
})

tape('not found', function (t) {
	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'world';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.get('hej', function (err, node) {
			t.error(err, 'no error')
			t.same(node, null, 'node is not found')
			t.end()
		})
	})
})

tape('leading / is ignored', function (t) {
	t.plan(7)
	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'world';

	db.put('/' + key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash('/' + key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.get('/' + key1, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key1, 'same key')
			t.same(node.value, value1, 'same value')
		})
		db.get(key1, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key1, 'same key')
			t.same(node.value, value1, 'same value')
		})
	})
})

tape('multiple put/get', function (t) {
	t.plan(8)

	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'world';

	var key2 = identity.publicKey + '/world';
	var value2 = 'hello';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
			t.error(err, 'no error')
			db.get(key1, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key1, 'same key')
				t.same(node.value, value1, 'same value')
			})
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key2, 'same key')
				t.same(node.value, value2, 'same value')
			})
		})
	})
})

tape('overwrites', function (t) {
	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'world';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.get(key1, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key1, 'same key')
			t.same(node.value, value1, 'same value')
			value1 = 'verden'
			db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {

				t.error(err, 'no error')
				db.get(key1, function (err, node) {
					t.error(err, 'no error')
					t.same(node.key, key1, 'same key')
					t.same(node.value, value1, 'same value')
					t.end()
				})
			})
		})
	})
})

tape('put/gets namespaces', function (t) {
	t.plan(8)

	var db = create.one()
	var key1 = identity.publicKey + '/hello/world';
	var value1 = 'world';

	var key2 = identity.publicKey + '/world';
	var value2 = 'hello';

	db.put(key1 + '/world', value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1 + '/world', value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
			t.error(err, 'no error')
			db.get(key1 + '/world', function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key1 + '/world', 'same key')
				t.same(node.value, value1, 'same value')
			})
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key2, 'same key')
				t.same(node.value, value2, 'same value')
			})
		})
	})
})

tape('put in tree', function (t) {
	t.plan(8)

	var db = create.one()
	var key1 = identity.publicKey + '/hello';
	var value1 = 'a';
	var key2 = identity.publicKey + '/hello/world';
	var value2 = 'b';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
			t.error(err, 'no error')
			db.get(key1, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key1, 'same key')
				t.same(node.value, value1, 'same value')
			})
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key2, 'same key')
				t.same(node.value, value2, 'same value')
			})
		})
	})
})

tape('put in tree reverse order', function (t) {
	t.plan(8)

	var db = create.one()
	var key1 = identity.publicKey + '/hello/world';
	var value1 = 'b';
	var key2 = identity.publicKey + '/hello';
	var value2 = 'a';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
			t.error(err, 'no error')
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key2, 'same key')
				t.same(node.value, value2, 'same value')
			})
			db.get(key1, function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, key1, 'same key')
				t.same(node.value, value1, 'same value')
			})
		})
	})
})

tape('multiple put in tree', function (t) {
	t.plan(13)

	var db = create.one()
	var key1 = identity.publicKey + '/hello/world';
	var value1 = 'b';
	var key2 = identity.publicKey + '/hello';
	var value2 = 'a';
	var key3 = identity.publicKey + '/hello/verden';
	var value3 = 'c';

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function (err, node) {
		t.error(err, 'no error')
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
			t.error(err, 'no error')
			db.put(key3, value3, EthCrypto.sign(identity.privateKey, db.createSignHash(key3, value3)), identity.publicKey, function (err, node) {
				t.error(err, 'no error')
				value2 = 'd';
				db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function (err, node) {
					t.error(err, 'no error')
					db.get(key2, function (err, node) {
						t.error(err, 'no error')
						t.same(node.key, key2, 'same key')
						t.same(node.value, value2, 'same value')
					})
					db.get(key1, function (err, node) {
						t.error(err, 'no error')
						t.same(node.key, key1, 'same key')
						t.same(node.value, value1, 'same value')
					})
					db.get(key3, function (err, node) {
						t.error(err, 'no error')
						t.same(node.key, key3, 'same key')
						t.same(node.value, value3, 'same value')
					})
				})
			})
		})
	})
})

tape('insert 100 values and get them all', function (t) {
	var db = create.one()
	var max = 100
	var i = 0

	t.plan(3 * max)

	loop()

	function loop () {
		if (i === max) return validate()
		var key = identity.publicKey + '/#' + i;
		var value = '#' + (i++);
		db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, loop)
	}

	function validate () {
		for (var i = 0; i < max; i++) {
			db.get(identity.publicKey + '/#' + i, same(identity.publicKey + '/#' + i, '#' + i))
		}
	}

	function same (key, value) {
		return function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key, 'same key')
			t.same(node.value, value, 'same value')
		}
	}
})

tape('race works', function (t) {
	t.plan(40)

	var missing = 10
	var db = create.one()

	for (var i = 0; i < 10; i++) {
		var key = identity.publicKey + '/#' + i;
		var value = '#' + i;
		db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, done)
	}

	function done (err) {
		t.error(err, 'no error')
		if (--missing) return
		for (var i = 0; i < 10; i++) same(identity.publicKey + '/#' + i, '#' + i)
	}

	function same (key, val) {
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key, 'same key')
			t.same(node.value, val, 'same value')
		})
	}
})

tape('version', function (t) {
	var db = create.one()

	db.version(function (err, version) {
		t.error(err, 'no error')
		t.same(version, Buffer.alloc(0))

		var key = identity.publicKey + '/hello';
		var value = 'world';
		db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, function () {
			db.version(function (err, version) {
				t.error(err, 'no error')
				var newValue = 'verden';
				db.put(key, newValue, EthCrypto.sign(identity.privateKey, db.createSignHash(key, newValue)), identity.publicKey, function () {
					db.checkout(version).get(key, function (err, node) {
						t.error(err, 'no error')
						t.same(node.value, value)
						t.end()
					})
				})
			})
		})
	})
})

tape('basic batch', function (t) {
	t.plan(1 + 3 + 3)

	var db = create.one()

	db.batch([
		{
			key: identity.publicKey + '/hello',
			value: 'world',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hello', 'world')),
			writerAddress: identity.publicKey
		},
		{
			key: identity.publicKey + '/hej',
			value: 'verden',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hej', 'verden')),
			writerAddress: identity.publicKey
		},
		{
			key: identity.publicKey + '/hello',
			value: 'welt',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hello', 'welt')),
			writerAddress: identity.publicKey
		}
	], function (err) {
		t.error(err, 'no error')
		db.get(identity.publicKey + '/hello', function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, identity.publicKey + '/hello')
			t.same(node.value, 'welt')
		})
		db.get(identity.publicKey + '/hej', function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, identity.publicKey + '/hej')
			t.same(node.value, 'verden')
		})
	})
})

tape('batch with del', function (t) {
	t.plan(1 + 1 + 3 + 2)

	var db = create.one()

	db.batch([
		{
			key: identity.publicKey + '/hello',
			value: 'world',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hello', 'world')),
			writerAddress: identity.publicKey
		},
		{
			key: identity.publicKey + '/hej',
			value: 'verden',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hej', 'verden')),
			writerAddress: identity.publicKey
		},
		{
			key: identity.publicKey + '/hello',
			value: 'welt',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hello', 'welt')),
			writerAddress: identity.publicKey
		}
	], function (err) {
		t.error(err, 'no error')
		db.batch([
			{
				key: identity.publicKey + '/hello',
				value: 'verden',
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hello', 'verden')),
				writerAddress: identity.publicKey
			},
			{
				type: 'del',
				key: identity.publicKey + '/hej',
				value: '',
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/hej', '')),
				writerAddress: identity.publicKey
			}
		], function (err) {
			t.error(err, 'no error')
			db.get(identity.publicKey + '/hello', function (err, node) {
				t.error(err, 'no error')
				t.same(node.key, identity.publicKey + '/hello')
				t.same(node.value, 'verden')
			})
			db.get(identity.publicKey + '/hej', function (err, node) {
				t.error(err, 'no error')
				t.same(node, null)
			})
		})
	})
})

tape('multiple batches', function (t) {
	t.plan(19)

	var db = create.one()

	db.batch([
		{
			type: 'put',
			key: identity.publicKey + '/foo',
			value: 'foo',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/foo', 'foo')),
			writerAddress: identity.publicKey
		},
		{
			type: 'put',
			key: identity.publicKey + '/bar',
			value: 'bar',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/bar', 'bar')),
			writerAddress: identity.publicKey
		}
	], function (err, nodes) {
		t.error(err)
		t.same(2, nodes.length)
		same(identity.publicKey + '/foo', 'foo')
		same(identity.publicKey + '/bar', 'bar')
		db.batch([
			{
				type: 'put',
				key: identity.publicKey + '/foo',
				value: 'foo2',
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/foo', 'foo2')),
				writerAddress: identity.publicKey
			},
			{
				type: 'put',
				key: identity.publicKey + '/bar',
				value: 'bar2',
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/bar', 'bar2')),
				writerAddress: identity.publicKey
			},
			{
				type: 'put',
				key: identity.publicKey + '/baz',
				value: 'baz',
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/baz', 'baz')),
				writerAddress: identity.publicKey
			}
		], function (err, nodes) {
			t.error(err)
			t.same(3, nodes.length)
			same(identity.publicKey + '/foo', 'foo2')
			same(identity.publicKey + '/bar', 'bar2')
			same(identity.publicKey + '/baz', 'baz')
		})
	})

	function same (key, val) {
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key)
			t.same(node.value, val)
		})
	}
})

tape('createWriteStream', function (t) {
	t.plan(10)
	var db = create.one()
	var writer = db.createWriteStream()

	writer.write([
		{
			type: 'put',
			key: identity.publicKey + '/foo',
			value: 'foo',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/foo', 'foo')),
			writerAddress: identity.publicKey
		},
		{
			type: 'put',
			key: identity.publicKey + '/bar',
			value: 'bar',
			signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/bar', 'bar')),
			writerAddress: identity.publicKey
		}
	])

	writer.write({
		type: 'put',
		key: identity.publicKey + '/baz',
		value: 'baz',
		signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/baz', 'baz')),
		writerAddress: identity.publicKey
	})

	writer.end(function (err) {
		t.error(err, 'no error')
		same(identity.publicKey + '/foo', 'foo')
		same(identity.publicKey + '/bar', 'bar')
		same(identity.publicKey + '/baz', 'baz')
	})

	function same (key, val) {
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key)
			t.same(node.value, val)
		})
	}
})

tape('createWriteStream pipe', function (t) {
	t.plan(10)
	var db = create.one()
	var writer = db.createWriteStream()
	var index = 0
	var reader = new Readable({
		objectMode: true,
		read: function (size) {
			var value = (index < 1000) ? {
				type: 'put',
				key: identity.publicKey + '/foo' + index,
				value: index,
				signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/foo' + index, index)),
				writerAddress: identity.publicKey
			} : null
			index++;
			this.push(value)
		}
	})
	reader.pipe(writer)
	writer.on('finish', function (err) {
		t.error(err, 'no error')
		same(identity.publicKey + '/foo1', '1')
		same(identity.publicKey + '/foo50', '50')
		same(identity.publicKey + '/foo999', '999')
	})

	function same (key, val) {
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key)
			t.same(node.value, val)
		})
	}
})

tape('create with precreated keypair', function (t) {
	var crypto = require('hypercore/lib/crypto')
	var keyPair = crypto.keyPair()

	var db = create.one(keyPair.publicKey, {secretKey: keyPair.secretKey})
	var key = identity.publicKey + '/hello';
	var value = 'world';

	db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, function (err, node) {
		t.same(node.value, value)
		t.error(err, 'no error')
		t.same(db.key, keyPair.publicKey, 'pubkey matches')
		db.source._storage.secretKey.read(0, keyPair.secretKey.length, function (err, secretKey) {
			t.error(err, 'no error')
			t.same(secretKey, keyPair.secretKey, 'secret key is stored')
		})
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.value, value, 'same value')
			t.end()
		})
	})
})

tape('can insert falsy values', function (t) {
	t.plan(2 * 2 + 3 + 1)

	var db = create.one(null, {valueEncoding: 'json'})

	var key1 = identity.publicKey + '/hello';
	var value1 = 0;
	var key2 = identity.publicKey + '/world';
	var value2 = false;

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function () {
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function () {
			db.get(key1, function (err, node) {
				t.error(err, 'no error')
				t.same(node && node.value, value1)
			})
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node && node.value, value2)
			})

			var ite = db.iterator()
			var result = {}

			ite.next(function loop (err, node) {
				t.error(err, 'no error')

				if (!node) {
					t.same(result, {[identity.publicKey + '/hello']: 0, [identity.publicKey + '/world']: false})
					return
				}

				result[node.key] = node.value
				ite.next(loop)
			})
		})
	})
})

tape('can put/get a null value', function (t) {
	t.plan(3)

	var db = create.one(null, {valueEncoding: 'json'})
	var key = identity.publicKey + '/some key';
	var value = null;
	db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, function (err) {
		t.error(err, 'no error')
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.value, null)
		})
	})
})

tape('does not reinsert if isNotExists is true in put', function (t) {
	t.plan(4)

	var db = create.one(null, {valueEncoding: 'utf8'})
	var key = identity.publicKey + '/some key';
	var value = 'hello';
	db.put(key, value, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value)), identity.publicKey, function (err) {
		t.error(err, 'no error')
		var value2 = 'goodbye'
		db.put(key, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key, value2)), identity.publicKey, { ifNotExists: true }, function (err) {
			t.error(err, 'no error')
			db.get(key, function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value)
			})
		})
	})
})

tape('normal insertions work with ifNotExists', function (t) {
	t.plan(5)

	var db = create.one()
	run(
		cb => db.put(identity.publicKey + '/some key', 'hello', EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/some key', 'hello')), identity.publicKey, { ifNotExists: true }, cb),
		cb => db.put(identity.publicKey + '/some key', 'goodbye', EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/some key', 'goodbye')), identity.publicKey, { ifNotExists: true }, cb),
		cb => db.put(identity.publicKey + '/another key', 'something else', EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/another key', 'something else')), identity.publicKey, { ifNotExists: true }, cb),
		done
	)

	function done (err) {
		t.error(err, 'no error')
		db.get(identity.publicKey + '/some key', function (err, node) {
			t.error(err, 'no error')
			t.same(node.value, 'hello')
			db.get(identity.publicKey + '/another key', function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, 'something else')
			})
		})
	}
})

tape('put with ifNotExists does not reinsert with conflict', function (t) {
	t.plan(5)

	create.two(function (db1, db2, replicate) {
		run(
			cb => db1.put(identity.publicKey + '/0', '0', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/0', '0')), identity.publicKey, cb),
			replicate,
			cb => db1.put(identity.publicKey + '/1', '1a', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/1', '1a')), identity.publicKey, cb),
			cb => db2.put(identity.publicKey + '/1', '1b', EthCrypto.sign(identity.privateKey, db2.createSignHash(identity.publicKey + '/1', '1b')), identity.publicKey, cb),
			cb => db1.put(identity.publicKey + '/10', '10', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/10', '10')), identity.publicKey, cb),
			replicate,
			cb => db1.put(identity.publicKey + '/2', '2', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/2', '2')), identity.publicKey, cb),
			cb => db1.put(identity.publicKey + '/1/0', '1/0', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/1/0', '1/0')), identity.publicKey, cb),
			done
		)

		function done (err) {
			t.error(err, 'no error')
			db1.put(identity.publicKey + '/1', '1c', EthCrypto.sign(identity.privateKey, db1.createSignHash(identity.publicKey + '/1', '1c')), identity.publicKey, { ifNotExists: true }, function (err) {
				t.error(err, 'no error')
				db1.get(identity.publicKey + '/1', function (err, nodes) {
					t.error(err, 'no error')
					t.same(nodes.length, 2)
					var vals = nodes.map(function (n) {
						return n.value
					})
					t.same(vals, ['1b', '1a'])
				})
			})
		}
	})
})

tape('opts is not mutated', function (t) {
	var opts = {firstNode: true}
	create.one(opts)
	t.deepEqual(opts, {firstNode: true})
	t.end()
})
