const tape = require('tape')
const create = require('./helpers/create')
const replicate = require('./helpers/replicate')
const run = require('./helpers/run')
const EthCrypto = require('eth-crypto');
const { privateKey: privateKeyA, publicKey: writerAddressA } = EthCrypto.createIdentity();
const { privateKey: privateKeyB, publicKey: writerAddressB } = EthCrypto.createIdentity();

tape('authorized writer passes "authorized" api', function (t) {
	create.two(function (a, b) {
		const schemaKey = "schema/%writerAddress%/foo";
		const schemaValue = {
			keySchema: "%writerAddress%/foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKey, schemaValue));
		a.addSchema(schemaKey, schemaValue, writerSignatureA, writerAddressA, (err) => {
			t.error(err)

			const key = writerAddressA + '/foo';
			const value = 'bar';
			writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(key, value));
			a.put(key, value, writerSignatureA, writerAddressA, { schemaKey }, (err) => {
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
			});
		});
	})
})

tape('authorized writer passes "authorized" api', function (t) {
	create.two(function (a, b) {
		const schemaKey = "schema/%writerAddress%/foo";
		const schemaValue = {
			keySchema: "%writerAddress%/foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
		b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
			t.error(err)

			const key = writerAddressB + '/foo';
			const value = 'bar';
			writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
			b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
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
			});
		})
	})
})

tape('unauthorized writer fails "authorized" api', function (t) {
	const a = create.one()
	a.ready(function () {
		const b = create.one(a.key)
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
	const a = create.one()
	a.ready(function () {
		const b = create.one(a.key)
		b.ready(function () {
			const schemaKey = "schema/%writerAddress%/foo";
			const schemaValue = {
				keySchema: "%writerAddress%/foo",
				valueValidationKey: "",
				keyValidation: ""
			};
			let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
			b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
				t.error(err)

				const key = writerAddressB + '/foo';
				const value = 'bar';
				writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
				b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
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
})

tape('unauthorized writer doing a put after replication', function (t) {
	t.plan(2)
	const a = create.one()
	a.ready(function () {
		const b = create.one(a.key)
		b.ready(function () {
			replicate(a, b, function () {
				const schemaKey = "schema/%writerAddress%/foo";
				const schemaValue = {
					keySchema: "%writerAddress%/foo",
					valueValidationKey: "",
					keyValidation: ""
				};
				let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
				b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
					t.error(err)

					const key = writerAddressB + '/foo';
					const value = 'bar';
					writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
					b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
						console.log('err', err);
						t.error(err)
					})
				})
			})
		})
	})
})

tape('unauthorized writer fails "authorized" after some writes', function (t) {
	const a = create.one()
	a.ready(function () {
		const schemaKey = "schema/%writerAddress%/foo";
		const schemaValue = {
			keySchema: "%writerAddress%/foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKey, schemaValue));
		a.addSchema(schemaKey, schemaValue, writerSignatureA, writerAddressA, (err) => {
			t.error(err)

			run(
				cb => a.put(writerAddressA + '/foo', 'bar', EthCrypto.sign(privateKeyA, a.createSignHash(writerAddressA + '/foo', 'bar')), writerAddressA, { schemaKey }, cb),
				cb => a.put(writerAddressA + '/foo', 'bar2', EthCrypto.sign(privateKeyA, a.createSignHash(writerAddressA + '/foo', 'bar2')), writerAddressA, { schemaKey}, cb),
				cb => a.put(writerAddressA + '/foo', 'bar3', EthCrypto.sign(privateKeyA, a.createSignHash(writerAddressA + '/foo', 'bar3')), writerAddressA, { schemaKey }, cb),
				cb => a.put(writerAddressA + '/foo', 'bar4', EthCrypto.sign(privateKeyA, a.createSignHash(writerAddressA + '/foo', 'bar4')), writerAddressA, { schemaKey }, cb),
				done
			)

			function done (err) {
				t.error(err)
				const b = create.one(a.key)
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
})

tape('authorized is consistent', function (t) {
	t.plan(7)

	const a = create.one(null, {contentFeed: true})
	a.ready(function () {
		const schemaKeyA = "schema/%writerAddress%/foo";
		const schemaValueA = {
			keySchema: "%writerAddress%/foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKeyA, schemaValueA));
		a.addSchema(schemaKeyA, schemaValueA, writerSignatureA, writerAddressA, (err) => {
			t.error(err)

			const b = create.one(a.key, {contentFeed: true, latency: 10})

			const schemaKeyB = "schema/%writerAddress%/bar";
			const schemaValueB = {
				keySchema: "%writerAddress%/bar",
				valueValidationKey: "",
				keyValidation: ""
			};
			let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKeyB, schemaValueB));
			b.addSchema(schemaKeyB, schemaValueB, writerSignatureB, writerAddressB, (err) => {
				t.error(err)

				run(
					cb => b.put(writerAddressB + '/bar', 'foo', EthCrypto.sign(privateKeyB, b.createSignHash(writerAddressB + '/bar', 'foo')), writerAddressB, { schemaKey: schemaKeyB },  cb),
					cb => a.put(writerAddressA + '/foo', 'bar', EthCrypto.sign(privateKeyA, a.createSignHash(writerAddressA + '/foo', 'bar')), writerAddressA, { schemaKey: schemaKeyA }, cb),
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
	})
})
