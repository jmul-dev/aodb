var tape = require('tape')
var create = require('./helpers/create')
var EthCrypto = require('eth-crypto');
var identity = EthCrypto.createIdentity();

var key1 = '/' + identity.publicKey + '/idgcmnmna';
var value1 = 'a';
var key2 = '/' + identity.publicKey + '/mpomeiehc';
var value2 = 'b';

tape('two keys with same siphash', function (t) {
	t.plan(2 + 2)

	var db = create.one()

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function () {
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function () {
			db.get(key1, function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value1)
			})
			db.get(key2, function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value2)
			})
		})
	})
})

tape('two keys with same siphash (iterator)', function (t) {
	var db = create.one()

	db.put(key1, value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1, value1)), identity.publicKey, function () {
		db.put(key2, value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2, value2)), identity.publicKey, function () {
			var ite = db.iterator()

			ite.next(function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value1)
			})
			ite.next(function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value2)
			})
			ite.next(function (err, node) {
				t.error(err, 'no error')
				t.same(node, null)
				t.end()
			})
		})
	})
})

tape('two prefixes with same siphash (iterator)', function (t) {
	var db = create.one()

	db.put(key1 + '/a', value1, EthCrypto.sign(identity.privateKey, db.createSignHash(key1 + '/a', value1)), identity.publicKey, function () {
		db.put(key2 + '/b', value2, EthCrypto.sign(identity.privateKey, db.createSignHash(key2 + '/b', value2)), identity.publicKey, function () {
			var ite = db.iterator(key1)

			ite.next(function (err, node) {
				t.error(err, 'no error')
				t.same(node.value, value1)
			})
			ite.next(function (err, node) {
				t.error(err, 'no error')
				t.same(node, null)
				t.end()
			})
		})
	})
})
