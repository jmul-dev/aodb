var EthCrypto = require("eth-crypto");

module.exports = function(db, privateKey, writerAddress, list, opts, cb) {
	var i = 0;
	loop(null);

	function loop(err) {
		if (err) return cb(err);
		if (i === list.length) return cb(null);

		var next = list[i++];
		if (typeof next === "string") next = { key: next, value: next };
		if (typeof next === "object" && next.schemaKey) {
			opts.schemaKey = next.schemaKey;
			if (!next.value) {
				next = { key: next.key, value: next.key };
			}
		}

		var signature = EthCrypto.sign(privateKey, db.createSignHash(next.key, next.value));
		db.put(next.key, next.value, signature, writerAddress, opts, loop);
	}
};
