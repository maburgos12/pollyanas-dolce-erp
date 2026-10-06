// Run: node scripts/test_media_archive_cache.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const handlers = {};
const stored = new Map();
let online = true;
let result;
const cache = {
  delete: async request => stored.delete(request.url),
  put: async (request, response) => stored.set(request.url, response),
};
vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../logistica/static/logistica/pwa/sw.js'), 'utf8'), {
  URL, Response,
  self: {addEventListener: (name, handler) => { handlers[name] = handler; }},
  caches: {open: async () => cache, match: async request => stored.get(request.url)},
  fetch: async () => { if (!online) throw Error('offline'); return result; },
});
const request = {url: 'https://erp.example/media/bitacora/test.jpg', method: 'GET'};
const read = () => {
  let response;
  handlers.fetch({request, respondWith: promise => { response = promise; }});
  return response;
};
(async () => {
  result = new Response('hot');
  assert.equal(await (await read()).text(), 'hot');
  assert.equal(stored.size, 1);
  online = false;
  assert.equal(await (await read()).text(), 'hot');
  online = true;
  result = new Response('archive', {headers: {'Cache-Control': 'private, no-store'}});
  assert.equal(await (await read()).text(), 'archive');
  assert.equal(stored.size, 0);
  online = false;
  assert.equal((await read()).type, 'error');
  online = true;
  stored.set(request.url, new Response('obsolete'));
  result = new Response('denied', {status: 404});
  assert.equal((await read()).status, 404);
  assert.equal(stored.size, 0);
  stored.set(request.url, new Response('obsolete'));
  cache.delete = async () => { throw Error('cache failure'); };
  assert.equal((await read()).status, 404);
  result = new Response('fresh');
  cache.put = async () => { throw Error('quota exceeded'); };
  assert.equal(await (await read()).text(), 'fresh');
  request.url = 'https://erp.example/api/logistica/test';
  online = false;
  await assert.rejects(read(), /offline/);
  console.log('PASS: public offline media preserved; archived and denied media never cached.');
})().catch(error => { console.error(error); process.exitCode = 1; });
