import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const nodeModules = process.env.CODEX_NODE_MODULES;
if (!nodeModules) {
  throw new Error("Define CODEX_NODE_MODULES con la ruta al runtime que contiene Playwright.");
}
const { chromium } = require(path.join(nodeModules, "playwright"));

const here = path.dirname(fileURLToPath(import.meta.url));
const outputDir = process.env.MOCKUP_OUTPUT_DIR;

if (!outputDir) {
  throw new Error("Define MOCKUP_OUTPUT_DIR para guardar las capturas fuera del repositorio.");
}

await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.BROWSER_EXECUTABLE || undefined,
});
const page = await browser.newPage({ viewport: { width: 1536, height: 1050 }, deviceScaleFactor: 1 });
await page.goto(pathToFileURL(path.join(here, "index.html")).href, { waitUntil: "networkidle" });

for (const view of ["requester", "buyer", "director", "tracking"]) {
  await page.locator(`[data-switch="${view}"]`).click();
  await page.screenshot({ path: path.join(outputDir, `${view}.png`), fullPage: true });
}

await page.setViewportSize({ width: 430, height: 900 });
await page.locator('[data-switch="requester"]').click();
await page.screenshot({ path: path.join(outputDir, "requester-mobile.png"), fullPage: true });

await browser.close();
