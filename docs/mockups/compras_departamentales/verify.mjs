import assert from "node:assert/strict";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const nodeModules = process.env.CODEX_NODE_MODULES;
if (!nodeModules) throw new Error("Define CODEX_NODE_MODULES.");
const { chromium } = require(path.join(nodeModules, "playwright"));

const here = path.dirname(fileURLToPath(import.meta.url));
const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.BROWSER_EXECUTABLE || undefined,
});
const page = await browser.newPage({ viewport: { width: 1536, height: 1050 } });
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));

await page.goto(pathToFileURL(path.join(here, "index.html")).href, { waitUntil: "networkidle" });
await page.locator("#requester-title").waitFor();
assert.equal(await page.locator(".item-row").count(), 3);

await page.locator("#add-item").click();
assert.equal(await page.locator(".item-row").count(), 4);

const firstRow = page.locator(".item-row").first();
await firstRow.locator(".quantity").fill("2");
await firstRow.locator(".unit-cost").fill("9000");
assert.equal((await page.locator("#grand-total").textContent()).trim(), "$20,160");

await page.locator('[data-switch="buyer"]').click();
assert.equal(await page.locator("#view-buyer").getAttribute("class"), "view active");
await page.locator('[data-switch="director"]').click();
await page.getByRole("button", { name: "Posponer" }).click();
assert.equal((await toastTitle()).trim(), "Falta explicar la decisión");
await page.locator("#decision-comment").fill("Revisar flujo y alternativas de pago.");
await page.getByRole("button", { name: "Evaluar financiamiento" }).click();
assert.equal((await toastTitle()).trim(), "En evaluación de financiamiento");

await page.setViewportSize({ width: 430, height: 900 });
await page.locator('[data-switch="requester"]').click();
const dimensions = await page.evaluate(() => ({
  clientWidth: document.documentElement.clientWidth,
  scrollWidth: document.documentElement.scrollWidth,
}));
assert.equal(dimensions.scrollWidth, dimensions.clientWidth, "El mockup móvil no debe desbordarse horizontalmente.");
assert.deepEqual(errors, []);

await browser.close();
console.log("OK: navegación, cálculos, decisiones y responsive verificados.");

async function toastTitle() {
  await page.locator("#toast.show strong").waitFor();
  return page.locator("#toast strong").textContent();
}
