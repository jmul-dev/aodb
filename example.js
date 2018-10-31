var aodb = require('./')
var EthCrypto = require('eth-crypto');

var identity = EthCrypto.createIdentity();

var db = aodb('./my.db', {
	valueEncoding: 'utf-8',
	reduce: (a, b) => a
})

/***** Put *****/
var key = identity.publicKey + '/hello';
var value = 'world';
var signature = EthCrypto.sign(identity.privateKey, db.createSignHash(key, value));

db.put(key, value, signature, identity.publicKey, function (err) {
	if (err) throw err
	db.get(key, function (err, node) {
		if (err) throw err
		console.log('inserted: ' + key + ' --> ' + node.value)
	})
})

/***** Delete *****/
// Create signature of empty value
var signature = EthCrypto.sign(identity.privateKey, db.createSignHash(key, ''));
db.del(key, signature, identity.publicKey, function (err) {
	if (err) throw err
	db.get(key, function (err, node) {
		if (err) throw err
		console.log('deleted: ' + key + ' --> ' + node)
	})
});

// Batch insert
var batch = [
	{
		type: 'put',
		key: identity.publicKey + '/key1',
		value: 'value1',
		signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/key1', 'value1')),
		writerAddress: identity.publicKey
	},
	{
		type: 'put',
		key: identity.publicKey + '/key2',
		value: 'value2',
		signature: EthCrypto.sign(identity.privateKey, db.createSignHash(identity.publicKey + '/key2', 'value2')),
		writerAddress: identity.publicKey
	}
];

db.batch(batch, function (err) {
	if (err) throw err
	db.get(batch[0].key, function (err, node) {
		if (err) throw err
		console.log('batch insert: ' + batch[0].key + ' --> ' + node.value)
	});
	db.get(batch[1].key, function (err, node) {
		if (err) throw err
		console.log('batch insert: ' + batch[1].key + ' --> ' + node.value)
	});
});
