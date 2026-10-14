#!/usr/bin/env node
// Writes legion requirements' verification.scenarios to Gherkin feature files.
//
// usage: node scripts/features.mjs <requirement-id>...
//
// Each scenario goes to tests/<module>/<requirement-id>.feature, where <module> is the
// scenario's own @module-<module> tag, so the tests tree mirrors src. The text is the
// rafters toolchain's feature format: the same requirement row always renders the same bytes.
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

function render(row, title, scenarios) {
  const header = [
    `# Generated from legion requirement ${row.id}, revision ${row.updated_at}.`,
    "# Do not edit: regenerate with scripts/features.mjs.",
  ];
  const blocks = scenarios.map((s) => s.gherkin.trim().replace(/^/gm, "  "));
  return `${header.join("\n")}\n\nFeature: ${title}\n\n${blocks.join("\n\n")}\n`;
}

function generate(id) {
  const row = JSON.parse(
    execFileSync("legion", ["document", "view", id, "--json"], { encoding: "utf8" }),
  );
  const payload = typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload;
  const scenarios = payload?.verification?.scenarios;
  if (!Array.isArray(scenarios) || scenarios.length === 0) {
    throw new Error(`${id} has no verification.scenarios`);
  }
  const byModule = new Map();
  for (const scenario of scenarios) {
    const module = /@module-([a-z0-9_]+)/.exec(scenario.gherkin)?.[1];
    if (!module) throw new Error(`${id}: a scenario has no @module-* tag`);
    byModule.set(module, [...(byModule.get(module) ?? []), scenario]);
  }
  for (const [module, group] of byModule) {
    const dir = join("tests", module);
    mkdirSync(dir, { recursive: true });
    const path = join(dir, `${row.id}.feature`);
    writeFileSync(path, render(row, payload.title, group));
    console.log(path);
  }
}

const ids = process.argv.slice(2);
if (ids.length === 0) {
  console.error("usage: node scripts/features.mjs <requirement-id>...");
  process.exitCode = 2;
} else {
  for (const id of ids) generate(id);
}
