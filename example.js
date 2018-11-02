const aodb = require('./')
const EthCrypto = require('eth-crypto');

const { privateKey, publicKey : writerAddress }  = EthCrypto.createIdentity();

const db = aodb('./my.db', {
	valueEncoding: 'json',
	reduce: (a, b) => a
})

/***** Add a Schema *****/
let key = 'schema/content/*/review/%writerAddress%';
let value = {
	keySchema: 'content/*/review/%writerAddress%',
	valueValidationKey: '',
	keyValidation: ''
};
let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

db.addSchema(key, value, writerSignature, writerAddress, (err) => {
	if (err) throw err
	db.get(key, (err, node) => {
		if (err) throw err
		console.log('inserted: ' + key + ' --> ' + JSON.stringify(node.value))

		/***** Put *****/
		key = 'content/0x123456789/review/' + writerAddress;
		value = 'Love the content';
		writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

		db.put(key, value, writerSignature, writerAddress, { schemaKey: 'schema/content/*/review/%writerAddress%' }, (err) => {
			if (err) throw err
			db.get(key, (err, node) => {
				if (err) throw err
				console.log('inserted: ' + key + ' --> ' + node.value)
			});
		})
	})
})

return;
/***** Put *****/
key = writerAddress + '/hello';
value = {foo: 'bar'};
writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

db.put(key, value, writerSignature, writerAddress, (err) => {
	if (err) throw err
	db.get(key, (err, node) => {
		if (err) throw err
		console.log('inserted: ' + key + ' --> ' + JSON.stringify(node.value))
		console.log('key', node.key);
		console.log('node', node);
	})
})
return;

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
