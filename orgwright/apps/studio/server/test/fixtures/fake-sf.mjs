import fs from 'node:fs';
import path from 'node:path';
import { setTimeout } from 'node:timers/promises';

const args = process.argv.slice(2);
const alias = args[args.indexOf('-o') + 1];
const directory = process.env.ORGWRIGHT_FIXTURE_DIR;
if (!directory || !/^fixture-[a-z-]+$/.test(alias)) {
  throw new Error('This CLI fixture only accepts its offline test aliases');
}
fs.appendFileSync(path.join(directory, 'calls.jsonl'), JSON.stringify(args) + '\n');
const release = path.join(directory, `${alias}.json`);
// The parent releases this subprocess only after proving the server can respond.
const deadline = Date.now() + 15000;
while (!fs.existsSync(release)) {
  if (Date.now() > deadline) throw new Error('Fixture was not released by its test');
  await setTimeout(10);
}
const response = JSON.parse(fs.readFileSync(release, 'utf8'));
if (response.error) {
  process.stderr.write(response.error);
  process.exitCode = 1;
} else {
  process.stdout.write(response.raw ?? JSON.stringify({ status: 0, result: response.echoArgs ? args : response.result }));
}
