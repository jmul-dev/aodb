var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var run = require('./helpers/run')
var EthCrypto = require('eth-crypto');
var identityA = EthCrypto.createIdentity();
var identityB = EthCrypto.createIdentity();

tape('authorized writer passes "authorized" api', function (t) {
	create.two(function (a, b) {
		var key = identityA.publicKey + '/foo';
		var value = 'bar';
		var signature = EthCrypto.sign(identityA.privateKey, a.createSignHash(key, value));
		a.put(key, value, signature, identityA.publicKey, function (err) {
			t.error(err)
			a.authorized(a.local.key, function (err, auth) {
				t.error(err)
				t.equals(auth, true)
				b.authorized(b.local.key, function (err, auth) {
					t.error(err)
					t.equals(auth, true)
					t.end()
				})
			})
		})
	})
})

tape('authorized writer passes "authorized" api', function (t) {
	create.two(function (a, b) {
		var key = identityB.publicKey + '/foo';
		var value = 'bar';
		var signature = EthCrypto.sign(identityB.privateKey, b.createSignHash(key, value));
		b.put(key, value, signature, identityB.publicKey, function (err) {
			t.error(err)
			a.authorized(a.local.key, function (err, auth) {
				t.error(err)
				t.equals(auth, true)
				b.authorized(b.local.key, function (err, auth) {
					t.error(err)
					t.equals(auth, true)
					t.end()
				})
			})
		})
	})
})

tape('unauthorized writer fails "authorized" api', function (t) {
	var a = create.one()
	a.ready(function () {
		var b = create.one(a.key)
		b.ready(function () {
			b.authorized(b.local.key, function (err, auth) {
				t.error(err)
				t.equals(auth, false)
				t.end()
			})
		})
	})
})

tape('local unauthorized writes =/> authorized', function (t) {
	var a = create.one()
	a.ready(function () {
		var b = create.one(a.key)
		b.ready(function () {
			var key = identityB.publicKey + '/foo';
			var value = 'bar';
			var signature = EthCrypto.sign(identityB.privateKey, b.createSignHash(key, value));
			b.put(key, value, signature, identityB.publicKey, function (err) {
				t.error(err)
				b.authorized(b.local.key, function (err, auth) {
					t.error(err)
					t.equals(auth, false)
					b.authorized(a.local.key, function (err, auth) {
						t.error(err)
						t.equals(auth, true)
						t.end()
					})
				})
			})
		})
	})
})

tape('unauthorized writer doing a put after replication', function (t) {
	t.plan(1)
	var a = create.one()
	a.ready(function () {
		var b = create.one(a.key)
		b.ready(function () {
			replicate(a, b, function () {
				var key = identityB.publicKey + '/foo';
				var value = 'bar';
				var signature = EthCrypto.sign(identityB.privateKey, b.createSignHash(key, value));
				b.put(key, value, signature, identityB.publicKey, function (err) {
					t.error(err)
				})
			})
		})
	})
})

tape('unauthorized writer fails "authorized" after some writes', function (t) {
	var a = create.one()
	a.ready(function () {
		run(
			cb => a.put(identityA.publicKey + '/foo', 'bar', EthCrypto.sign(identityA.privateKey, a.createSignHash(identityA.publicKey + '/foo', 'bar')), identityA.publicKey, cb),
			cb => a.put(identityA.publicKey + '/foo', 'bar2', EthCrypto.sign(identityA.privateKey, a.createSignHash(identityA.publicKey + '/foo', 'bar2')), identityA.publicKey, cb),
			cb => a.put(identityA.publicKey + '/foo', 'bar3', EthCrypto.sign(identityA.privateKey, a.createSignHash(identityA.publicKey + '/foo', 'bar3')), identityA.publicKey, cb),
			cb => a.put(identityA.publicKey + '/foo', 'bar4', EthCrypto.sign(identityA.privateKey, a.createSignHash(identityA.publicKey + '/foo', 'bar4')), identityA.publicKey, cb),
			done
		)

		function done (err) {
			t.error(err)
			var b = create.one(a.key)
			b.ready(function () {
				replicate(a, b, function () {
					b.authorized(b.local.key, function (err, auth) {
						t.error(err)
						t.equals(auth, false)
						t.end()
					})
				})
			})
		}
	})
})

tape('authorized is consistent', function (t) {
	t.plan(5)

	var a = create.one(null, {contentFeed: true})
	a.ready(function () {
		var b = create.one(a.key, {contentFeed: true, latency: 10})

		run(
			cb => b.put(identityB.publicKey + '/bar', 'foo', EthCrypto.sign(identityB.privateKey, b.createSignHash(identityB.publicKey + '/bar', 'foo')), identityB.publicKey, cb),
			cb => a.put(identityA.publicKey + '/foo', 'bar', EthCrypto.sign(identityA.privateKey, a.createSignHash(identityA.publicKey + '/foo', 'bar')), identityA.publicKey, cb),
			auth,
			replicate.bind(null, a, b),
			done
		)

		function done (err) {
			t.error(err, 'no error')
			a.authorized(b.local.key, function (err, auth) {
				t.error(err, 'no error')
				t.ok(auth)
			})
			b.authorized(b.local.key, function (err, auth) {
				t.error(err, 'no error')
				t.ok(auth)
			})
		}

		function auth (cb) {
			a.authorize(b.local.key, cb)
		}
	})
})
