var aodb = require('./')
var EthCrypto = require('eth-crypto');

var identity = EthCrypto.createIdentity();

var db = aodb('./my.db', {
	valueEncoding: 'utf-8',
	reduce: (a, b) => a
})

var key = '/' + identity.publicKey + '/hello';
var value = 'world';
var signature = EthCrypto.sign(identity.privateKey, EthCrypto.hash.keccak256(value));

db.put(key, value, signature, identity.publicKey, function (err) {
	if (err) throw err
	db.get(key, function (err, node) {
		if (err) throw err
		console.log(key + ' --> ' + node.value)
	})
})

// Batch insert
var batch = [
	{
		type: 'put',
		key: '/' + identity.publicKey + '/key1',
		value: 'value1',
		signature: EthCrypto.sign(identity.privateKey, EthCrypto.hash.keccak256('value1')),
		writerAddress: identity.publicKey
	},
	{
		type: 'put',
		key: '/' + identity.publicKey + '/key2',
		value: 'value2',
		signature: EthCrypto.sign(identity.privateKey, EthCrypto.hash.keccak256('value2')),
		writerAddress: identity.publicKey
	}
];

db.batch(batch, function (err) {
	if (err) throw err
	db.get(batch[0].key, function (err, node) {
		if (err) throw err
		console.log(batch[0].key + ' --> ' + node.value)
	});
});
