const aodb = require('./')
const EthCrypto = require('eth-crypto');

const { privateKey, publicKey : writerAddress }  = EthCrypto.createIdentity();

const db = aodb('./my.db', {
	valueEncoding: 'utf-8',
	reduce: (a, b) => a
})

/***** Put *****/
let key = writerAddress + '/hello';
let value = 'world';
let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

db.put(key, value, writerSignature, writerAddress, (err) => {
	if (err) throw err
	db.get(key, (err, node) => {
		if (err) throw err
		console.log('inserted: ' + key + ' --> ' + node.value)
	})
})

/***** Delete *****/
// Create writerSignature of empty value
writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, ''));
db.del(key, writerSignature, writerAddress, (err) => {
	if (err) throw err
	db.get(key, (err, node) => {
		if (err) throw err
		console.log('deleted: ' + key + ' --> ' + node)
	})
});

// Batch insert
const batch = [
	{
		type: 'put',
		key: writerAddress + '/key1',
		value: 'value1',
		writerSignature: EthCrypto.sign(privateKey, db.createSignHash(writerAddress + '/key1', 'value1')),
		writerAddress
	},
	{
		type: 'put',
		key: writerAddress + '/key2',
		value: 'value2',
		writerSignature: EthCrypto.sign(privateKey, db.createSignHash(writerAddress + '/key2', 'value2')),
		writerAddress: writerAddress
	}
];

db.batch(batch, (err) => {
	if (err) throw err
	db.get(batch[0].key, (err, node) => {
		if (err) throw err
		console.log('batch insert: ' + batch[0].key + ' --> ' + node.value)
	});
	db.get(batch[1].key, (err, node) => {
		if (err) throw err
		console.log('batch insert: ' + batch[1].key + ' --> ' + node.value)
	});
});
