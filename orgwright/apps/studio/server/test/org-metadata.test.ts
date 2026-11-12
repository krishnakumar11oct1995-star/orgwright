import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { fork, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import { setTimeout } from 'node:timers/promises';
import { WebSocket } from 'ws';
import { executeSfCommand } from '@orgwright/introspector';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'orgwright-metadata-'));
const bin = path.join(root, 'bin');
let server: ChildProcess;
let base: string;
let logs = '';

async function within<T>(promise: Promise<T>, ms = 3000): Promise<T> {
  let timer: ReturnType<typeof globalThis.setTimeout>;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = globalThis.setTimeout(() => reject(new Error('Studio did not respond while the CLI was pending')), ms);
      })
    ]);
  } finally {
    globalThis.clearTimeout(timer!);
  }
}

function calls(): string[][] {
  const file = path.join(root, 'calls.jsonl');
  return fs.existsSync(file) ? fs.readFileSync(file, 'utf8').trim().split('\n').map(line => JSON.parse(line)) : [];
}

function release(alias: string, response: object) {
  fs.writeFileSync(path.join(root, `${alias}.json`), JSON.stringify(response));
}

async function waitForCall(alias: string) {
  const deadline = Date.now() + 10000;
  while (!calls().some(args => args.includes(alias))) {
    assert.ok(Date.now() < deadline, `CLI did not start for ${alias}\n${logs}`);
    await setTimeout(10);
  }
}

before(async () => {
  fs.mkdirSync(bin);
  const fixture = path.join(here, 'fixtures', 'fake-sf.mjs');
  if (process.platform === 'win32') {
    fs.writeFileSync(path.join(bin, 'sf.cmd'), `@echo off\r\n"${process.execPath}" "${fixture}" %*\r\n`);
  } else {
    fs.writeFileSync(path.join(bin, 'sf'), '#!/usr/bin/env node\n' + fs.readFileSync(fixture, 'utf8'), { mode: 0o755 });
  }
  const require = createRequire(import.meta.url);
  server = fork(path.join(here, 'fixtures', 'server-bootstrap.mjs'), [], {
    execArgv: ['--import', pathToFileURL(require.resolve('tsx/esm')).href],
    env: {
      ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH || ''}`,
      ORGWRIGHT_ROOT: root,
      ORGWRIGHT_HOST: '127.0.0.1',
      ORGWRIGHT_FIXTURE_DIR: root,
      SF_USERNAME: 'fixture-admin@example.com'
    },
    silent: true
  });
  server.stdout!.on('data', chunk => logs += chunk);
  server.stderr!.on('data', chunk => logs += chunk);
  const ready = new Promise<number>((resolve, reject) => {
    server.once('message', (message: any) => resolve(message.port));
    server.once('exit', code => reject(new Error(`Studio exited ${code}\n${logs}`)));
  });
  base = `http://127.0.0.1:${await within(ready, 15000)}`;
});

after(async () => {
  if (server && server.exitCode === null && server.signalCode === null) {
    const exited = once(server, 'exit');
    server.kill();
    await exited;
  }
  // Only remove the directory this test created under the configured temp root.
  assert.equal(path.dirname(root), path.resolve(os.tmpdir()));
  assert.ok(path.basename(root).startsWith('orgwright-metadata-'));
  fs.rmSync(root, { recursive: true, force: true });
});

for (const [label, value] of [
  ['percent', '%ORGWRIGHT_FIXTURE_PERCENT%'],
  ['exclamation', '!ORGWRIGHT_FIXTURE_PERCENT!'],
  ['caret', 'before^after'],
  ['opening quote', '"begin'],
  ['closing quote', 'end"'],
  ['both quotes', '"quoted"'],
  ['trailing backslash', 'path with space\\'],
  ['backslash before quote', 'path\\"quoted'],
  ['empty value', ''],
  ['combined', 'space & pipe | (parentheses) %ORGWRIGHT_FIXTURE_PERCENT% ^ < > "quote" trailing\\'],
]) {
  test(`CLI arguments retain ${label} literally`, async () => {
    const previousPath = process.env.PATH;
    const previousDirectory = process.env.ORGWRIGHT_FIXTURE_DIR;
    const previousPercent = process.env.ORGWRIGHT_FIXTURE_PERCENT;
    const args = ['data', 'query', '-o', 'fixture-quoting', '-q', 'SELECT "Name", Id FROM User', value];
    release('fixture-quoting', { echoArgs: true });
    try {
      process.env.PATH = `${bin}${path.delimiter}${previousPath || ''}`;
      process.env.ORGWRIGHT_FIXTURE_DIR = root;
      process.env.ORGWRIGHT_FIXTURE_PERCENT = 'must-not-expand';
      assert.deepEqual(await executeSfCommand(args), args);
    } finally {
      for (const [key, value] of Object.entries({ PATH: previousPath, ORGWRIGHT_FIXTURE_DIR: previousDirectory, ORGWRIGHT_FIXTURE_PERCENT: previousPercent })) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
}

for (const endpoint of ['objects', 'users']) {
  test(`${endpoint}: invalid aliases are rejected before running the CLI`, async () => {
    const count = calls().length;
    for (const alias of ['bad alias', '../outside', 'bad&command', 'bad"quote']) {
      const response = await fetch(`${base}/api/org/${endpoint}?alias=${encodeURIComponent(alias)}`);
      assert.equal(response.status, 400);
      assert.deepEqual(await response.json(), { success: false, error: 'Invalid alias' });
    }
    assert.equal(calls().length, count);
  });

  test(`${endpoint}: HTTP and WebSocket stay responsive until CLI output arrives`, async () => {
    const alias = `fixture-${endpoint}-pending`;
    const result = endpoint === 'objects'
      ? ['Custom__c', 'AccountHistory', 'Account', 'Settings__mdt', 'Contact']
      : { records: [
          { Id: '005-fixture', Name: 'Fixture User', Username: 'fixture-user@example.com', Email: 'fixture-user@example.com', Profile: { Name: 'Standard User' }, UserRole: null },
          { Id: '005-auto', Name: 'Automation', Username: 'autoproc@example.com' }
        ] };
    const pending = fetch(`${base}/api/org/${endpoint}?alias=${alias}`).then(response => response.json());
    // Attach a handler immediately so a failed assertion cannot leave an unhandled fetch.
    pending.catch(() => {});
    let socket: WebSocket | undefined;
    try {
      await waitForCall(alias);
      const health: any = await within(fetch(`${base}/api/health`).then(response => response.json()));
      assert.equal(health.status, 'ok');
      socket = new WebSocket(base.replace('http:', 'ws:') + '/ws');
      const [message] = await within(once(socket, 'message'));
      assert.equal(JSON.parse(String(message)).type, 'connected');
    } finally {
      socket?.terminate();
      release(alias, { result });
      await pending;
    }
    const data: any = await pending;
    assert.equal(data.fromCache, false);
    assert.equal(data.alias, alias);
    if (endpoint === 'objects') {
      assert.deepEqual(data.objects, ['Account', 'Contact', 'Custom__c']);
      assert.deepEqual(data.custom, ['Custom__c']);
      assert.equal(data.totalCount, 3);
    } else {
      assert.deepEqual(data.users.map((user: any) => user.id), ['admin', '005-fixture']);
      assert.equal(data.users[1].profileName, 'Standard User');
      assert.equal(data.users[1].roleName, '');
    }
    const count = calls().length;
    const cached: any = await fetch(`${base}/api/org/${endpoint}?alias=${alias}`).then(response => response.json());
    assert.equal(cached.fromCache, true);
    assert.equal(calls().length, count);
    const refreshed: any = await fetch(`${base}/api/org/${endpoint}?alias=${alias}&refresh=true`).then(response => response.json());
    assert.equal(refreshed.fromCache, false);
    assert.equal(calls().length, count + 1);
    const invocation = calls().find(args => args.includes(alias))!;
    assert.deepEqual(invocation.slice(0, 2), endpoint === 'objects' ? ['sobject', 'list'] : ['data', 'query']);
    assert.equal(invocation[invocation.indexOf('-o') + 1], alias);
    if (endpoint === 'users') assert.match(invocation[invocation.indexOf('-q') + 1], /^SELECT Id, Name, Username, Email, Profile.Name, UserRole.Name, IsActive FROM User /);
  });

  for (const mode of ['error', 'invalid-json']) {
    test(`${endpoint}: ${mode} retains its fallback response`, async () => {
      const alias = `fixture-${endpoint}-${mode}`;
      release(alias, mode === 'error' ? { error: 'Fixture CLI failure' } : { raw: 'not JSON' });
      const data: any = await fetch(`${base}/api/org/${endpoint}?alias=${alias}`).then(response => response.json());
      assert.equal(data.success, true);
      assert.equal(data.fallback, true);
      assert.equal(data.alias, alias);
      if (endpoint === 'objects') assert.ok(data.objects.includes('Account'));
      else assert.equal(data.users[0].id, 'admin');
    });
  }
}
