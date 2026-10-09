from pathlib import Path
import shutil
import subprocess
import unittest

from django.test import SimpleTestCase


class MermaPhotoTransportTests(SimpleTestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js no está disponible")
    def test_fotos_se_materializan_antes_de_serializar_multipart(self):
        result = subprocess.run(["node", "--input-type=module", "-e", r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const html = fs.readFileSync('mermas/templates/mermas/form.html', 'utf8');
const start = html.indexOf('photoNames.forEach((name) => {', html.indexOf('const formData = new FormData(form)'));
const newStart = html.indexOf('for (const name of photoNames)', html.indexOf('const formData = new FormData(form)'));
const offset = newStart === -1 ? start : newStart;
const end = html.indexOf('const response = await fetch', offset);
let block = html.slice(offset, end);
if (newStart === -1) block = block.slice(0, block.indexOf('if (token)'));
const run = new (Object.getPrototypeOf(async function(){}).constructor)('photoNames','photos','formData',block);
let reads = 0;
const bytes = new Uint8Array([255,216,42,255,217]);
const original = {name:'image.jpg', type:'image/jpeg', size:bytes.length,
    async arrayBuffer(){ reads++; return bytes.buffer; }};
const photos = {ticket_fotos:[original], producto_fotos:[original]};
const sent = [];
const body = {delete(){}, append(name, blob, filename){ sent.push({name,blob,filename}); }};
await run(Object.keys(photos), photos, body);
assert.equal(reads, 2, 'El archivo original de Safari no debe enviarse directamente');
assert.equal(sent.length, 2);
for (const part of sent) {
    assert.notEqual(part.blob, original);
    assert.equal(part.blob.type, 'image/jpeg');
    assert.equal(part.filename, 'image.jpg');
    assert.deepEqual(new Uint8Array(await part.blob.arrayBuffer()), bytes);
}
let appended = false;
await assert.rejects(run(['ticket_fotos'], {ticket_fotos:[{...original, async arrayBuffer(){ throw Error('unreadable'); }}]},
    {delete(){}, append(){ appended = true; }}));
assert.equal(appended, false, 'Una foto ilegible debe detener el envío');
console.log('photo transport: ok');
"""], cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
