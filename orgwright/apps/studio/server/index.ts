import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import * as path from 'path';
import * as fs from 'fs';
import { execFileSync } from 'child_process';
import { isValidAlias, safeJoin, runSfSync } from './lib/validators';

// Orgwright Monorepo package imports
import { resolveTestSteps } from '@orgwright/resolver';
import { generatePlaywrightSpec } from '@orgwright/codegen';
import { executePlaywrightTest, stopTestExecution, isTestRunning } from '@orgwright/runner';
import { executeSfCommand, getCachedMetadata } from '@orgwright/introspector';
import { ROOT_DIR, ORGWRIGHT_DIR, isValidTestId } from './lib/paths';
import { resolveWithCatalog } from './lib/catalog';
import { expandBlocks } from './lib/blocks';
import { buildSpec, clearTelemetry, readRunOutcome, toLines } from './lib/spec-builder';
import { appendHistory, summarizeHistory } from './lib/history';
import { runSuite, readDefinitions, hasTag } from './lib/suite-runner';
import { registerStepRoutes } from './routes/steps';
import { registerSuiteRoutes } from './routes/suite';
import {
  startRecording,
  stopRecording,
  getRecorderStatus,
  dispatchRecorderClick,
  dispatchRecorderScroll,
  dispatchRecorderType,
  dispatchRecorderKeyPress
} from './recorder';
import { analyzeTestFailure, FailureAnalysis } from './failure-analyzer';
import {
  fetchRecentSalesforceLogs,
  buildExecutionLogReport,
  FullExecutionLogReport,
  friendlyStepError
} from './salesforce-log-analyzer';
import { auditPlaywrightSpec, getQASkillsCatalog } from './qa-skills-engine';
import {
  listConnectedOrgs,
  getOrgStatus,
  testOrgConnection,
  openOrg,
  logoutOrg,
  startWebAuth,
  getAuthJobStatus,
  cancelAuthJob,
  loginWithAccessToken,
  loginWithSfdxUrl
} from './org-manager';
import { SALESFORCE_STANDARD_TEMPLATES } from '../src/data/salesforce-templates';

// Project root is derived from this file's location (override with ORGWRIGHT_ROOT)
const rootPlaywrightDir = ROOT_DIR;
const targetDir = path.resolve(rootPlaywrightDir, 'tests', 'generated');
const definitionsDir = path.resolve(rootPlaywrightDir, 'tests', 'definitions');

let lastFailureAnalysis: FailureAnalysis | null = null;
let lastExecutionLogReport: FullExecutionLogReport | null = null;
let isExecutionPaused = false;

// Auto-seed last failure analysis and execution log report from most recent run
try {
  const initResultsDir = path.resolve(rootPlaywrightDir, 'test-results');
  if (fs.existsSync(initResultsDir)) {
    const rFiles = fs.readdirSync(initResultsDir, { recursive: true }) as string[];
    const errCtx = rFiles.find((f: string) => f.endsWith('error-context.md'));
    if (errCtx) {
      const errContent = fs.readFileSync(path.join(initResultsDir, errCtx), 'utf-8');
      lastFailureAnalysis = analyzeTestFailure(errContent, rootPlaywrightDir, 'TC_002');
    }
    const logJson = rFiles.find((f: string) => f.endsWith('-execution-logs.json'));
    if (logJson) {
      lastExecutionLogReport = JSON.parse(fs.readFileSync(path.join(initResultsDir, logJson), 'utf-8').replace(/^\uFEFF/, ''));
    }
  }
} catch {}

const app = express();
const port = 3001;
// The Studio server runs tests and writes files, so it only listens on this machine by default.
// Set ORGWRIGHT_HOST=0.0.0.0 to expose it on your network deliberately.
const host = process.env.ORGWRIGHT_HOST || '127.0.0.1';

// Only the Studio's own page may call the API from a browser. Requests without an Origin header
// (the test runner, curl, scripts on this machine) are allowed. This stops any website you happen
// to have open from driving the server.
const allowedOrigins = new Set(
  ['http://localhost:5173', 'http://127.0.0.1:5173', ...(process.env.ORGWRIGHT_ALLOWED_ORIGINS || '').split(',')]
    .map((o) => o.trim())
    .filter(Boolean)
);
const isAllowedOrigin = (origin?: string) =>
  !origin ||
  allowedOrigins.has(origin);

// Browsers omit the Origin header on "simple" cross-site GETs (<img>, no-cors fetch), so a missing
// Origin alone does not prove the caller is a local script. Modern browsers always send
// Sec-Fetch-Site; when there is no Origin but Sec-Fetch-Site says the request came from another
// site, reject it. Non-browser callers (runner, curl, scripts) send neither header and still work,
// and the Studio UI (same-origin through the Vite proxy) sends an allowed Origin or
// Sec-Fetch-Site: same-origin.
const isAllowedRequest = (origin: string | undefined, fetchSite: string | undefined) => {
  if (origin) return isAllowedOrigin(origin);
  return !fetchSite || fetchSite === 'same-origin' || fetchSite === 'none';
};

app.use(cors({ origin: (origin, cb) => cb(null, isAllowedOrigin(origin)) }));
app.use((req, res, next) => {
  const fetchSite = req.headers['sec-fetch-site'] as string | undefined;
  if (!isAllowedRequest(req.headers.origin, fetchSite)) {
    return res.status(403).json({
      error: `Request from ${req.headers.origin || fetchSite} is not allowed to use the Orgwright Studio API`
    });
  }
  next();
});
app.use(express.json());

const server = createServer(app);
const wss = new WebSocketServer({
  server,
  path: '/ws',
  verifyClient: ({ origin, req }: { origin?: string; req: import('http').IncomingMessage }) =>
    isAllowedRequest(origin, req.headers['sec-fetch-site'] as string | undefined)
});

const clients = new Set<WebSocket>();

wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'connected', message: 'Orgwright Studio WebSocket connected' }));

  ws.on('message', (raw) => {
    try {
      const parsed = JSON.parse(raw.toString());
      // A step failed and the test is paused for repair: send testers a plain-language reason
      if (parsed?.type === 'step_failed_paused' && parsed.error) {
        parsed.rawError = parsed.error;
        parsed.error = friendlyStepError(parsed.error);
      }
      // Broadcast live frames, steps, and logs to all frontend studio viewers
      broadcast(parsed);
    } catch {}
  });

  ws.on('close', () => clients.delete(ws));
});

function broadcast(data: any) {
  const msg = JSON.stringify(data);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

// Stream frame HTTP endpoint (fallback for HTTP posts)
app.post('/api/stream/frame', (req, res) => {
  const { frame, url, step, testId } = req.body;
  if (frame) {
    broadcast({ type: 'stream_frame', testId, frame, url, step });
  }
  res.json({ ok: true });
});

// 1. Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// Cache directory for org objects and users
const cacheDir = path.resolve(rootPlaywrightDir, '.orgwright', 'cache');
if (!fs.existsSync(cacheDir)) {
  fs.mkdirSync(cacheDir, { recursive: true });
}

// 2. Org List (All connected orgs from CLI)
app.get('/api/org/list', (req, res) => {
  try {
    // ORGWRIGHT_ORGS="alias1,alias2" limits the Studio to those CLI orgs (e.g. keep client orgs out of a demo)
    const allowed = (process.env.ORGWRIGHT_ORGS || '').split(',').map((s) => s.trim()).filter(Boolean);
    const orgs = listConnectedOrgs().filter((o) => !allowed.length || allowed.includes(o.alias) || allowed.includes(o.username));
    const defaultAlias = process.env.ORGWRIGHT_DEFAULT_ORG || process.env.SF_ORG_ALIAS || 'sandboxOrg';
    res.json({ success: true, orgs, defaultAlias });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3. Org Status for a specific alias
app.get('/api/org/status', (req, res) => {
  const targetOrg = (req.query.alias as string) || 'sandboxOrg';
  const status = getOrgStatus(targetOrg);
  res.json(status);
});

// 3.1 Test Org Connection & Latency
app.post('/api/org/test-connection', (req, res) => {
  const { alias } = req.body;
  if (!alias) return res.status(400).json({ error: 'alias is required' });
  const result = testOrgConnection(alias);
  res.json(result);
});

// 3.2 Open Org in Browser / Retrieve Frontdoor URL
app.post('/api/org/open', (req, res) => {
  const { alias, path: sPath, urlOnly } = req.body;
  if (!alias) return res.status(400).json({ error: 'alias is required' });
  const result = openOrg(alias, { path: sPath, urlOnly: urlOnly !== false });
  res.json(result);
});

// 3.3 Log out / Revoke an Org
app.post('/api/org/logout', (req, res) => {
  const { alias } = req.body;
  if (!alias) return res.status(400).json({ error: 'alias is required' });
  if (!isValidAlias(alias)) return res.status(400).json({ error: 'Invalid alias' });
  const result = logoutOrg(alias);
  // Clear any cached objects/users for this org
  try {
    const objCache = safeJoin(cacheDir, `objects-${alias}.json`);
    const usrCache = safeJoin(cacheDir, `users-${alias}.json`);
    if (fs.existsSync(objCache)) fs.unlinkSync(objCache);
    if (fs.existsSync(usrCache)) fs.unlinkSync(usrCache);
  } catch {}
  res.json(result);
});

// 3.4 Launch Interactive Web Login Flow
app.post('/api/org/login/web', (req, res) => {
  const { alias, instanceUrl, browser, setDefault } = req.body;
  if (!alias) return res.status(400).json({ error: 'alias is required' });
  const targetUrl = instanceUrl || 'https://test.salesforce.com';

  let job;
  try {
  job = startWebAuth(
    alias,
    targetUrl,
    { browser, setDefault },
    (completedJob) => {
      // Broadcast WebSocket notification to all studio UI clients
      broadcast({
        type: 'ORG_AUTH_COMPLETED',
        jobId: completedJob.id,
        alias: completedJob.alias,
        status: completedJob.status,
        resultOrg: completedJob.resultOrg,
        error: completedJob.error
      });
    }
  );
  } catch (err: any) {
    return res.status(400).json({ success: false, error: err.message || 'Invalid login parameters' });
  }

  res.json({
    success: true,
    jobId: job.id,
    alias: job.alias,
    status: job.status,
    message: 'Salesforce Web Login flow launched. Follow browser prompt to authorize.'
  });
});

// 3.5 Check Status of Web Login Job
app.get('/api/org/login/status/:jobId', (req, res) => {
  const job = getAuthJobStatus(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'Auth job not found' });
  }
  res.json({
    success: true,
    job: {
      id: job.id,
      alias: job.alias,
      instanceUrl: job.instanceUrl,
      status: job.status,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
      output: job.output,
      error: job.error,
      resultOrg: job.resultOrg
    }
  });
});

// 3.6 Cancel Web Login Job
app.post('/api/org/login/cancel/:jobId', (req, res) => {
  const cancelled = cancelAuthJob(req.params.jobId);
  res.json({ success: cancelled });
});

// 3.7 Authorize with Access Token
app.post('/api/org/login/token', (req, res) => {
  const { alias, instanceUrl, accessToken, setDefault } = req.body;
  if (!alias || !instanceUrl || !accessToken) {
    return res.status(400).json({ error: 'alias, instanceUrl, and accessToken are required' });
  }
  const result = loginWithAccessToken(alias, instanceUrl, accessToken, Boolean(setDefault));
  res.json(result);
});

// 3.8 Authorize with SFDX Auth URL
app.post('/api/org/login/sfdx-url', (req, res) => {
  const { alias, sfdxUrl, setDefault } = req.body;
  if (!alias || !sfdxUrl) {
    return res.status(400).json({ error: 'alias and sfdxUrl are required' });
  }
  const result = loginWithSfdxUrl(alias, sfdxUrl, Boolean(setDefault));
  res.json(result);
});

// 4. Get all SObjects from the connected org (Standard & Custom)
app.get('/api/org/objects', async (req, res) => {
  const targetOrg = (req.query.alias as string) || 'sandboxOrg';
  if (!isValidAlias(targetOrg)) return res.status(400).json({ success: false, error: 'Invalid alias' });
  const forceRefresh = req.query.refresh === 'true';
  const orgCacheFile = safeJoin(cacheDir, `objects-${targetOrg}.json`);

  if (!forceRefresh && fs.existsSync(orgCacheFile)) {
    try {
      const cached = JSON.parse(fs.readFileSync(orgCacheFile, 'utf-8'));
      return res.json({ success: true, ...cached, fromCache: true });
    } catch {
      // re-fetch
    }
  }

  try {
    const allObjects: string[] = await executeSfCommand(
      ['sobject', 'list', '-o', targetOrg, '--json'],
      { timeoutMs: 30000 }
    ) || [];

    // Filter out internal Salesforce noise
    const filtered = allObjects.filter(name =>
      !name.endsWith('ChangeEvent') &&
      !name.endsWith('History') &&
      !name.endsWith('Share') &&
      !name.endsWith('Feed') &&
      !name.endsWith('__e') &&
      !name.endsWith('__mdt')
    );

    const custom = filtered.filter(n => n.endsWith('__c')).sort();
    const standard = filtered.filter(n => !n.includes('__')).sort();

    // Standard Salesforce CRM objects first, followed by custom objects
    const priority = ['Account', 'Contact', 'Opportunity', 'Lead', 'Case', 'Task', 'Event', 'Campaign', 'User'];
    const rest = filtered.filter(n => !priority.includes(n));
    const sorted = [...priority.filter(p => filtered.includes(p)), ...rest];

    const data = {
      alias: targetOrg,
      totalCount: sorted.length,
      customCount: custom.length,
      standardCount: standard.length,
      objects: sorted,
      custom,
      standard
    };

    fs.writeFileSync(orgCacheFile, JSON.stringify(data, null, 2), 'utf-8');
    res.json({ success: true, ...data, fromCache: false });
  } catch (err: any) {
    res.json({
      success: true,
      alias: targetOrg,
      objects: ['Account', 'Contact', 'Opportunity', 'Lead', 'Case', 'Task', 'Event', 'Campaign', 'User'],
      custom: [],
      standard: ['Account', 'Contact', 'Opportunity', 'Lead', 'Case', 'Task', 'Event', 'Campaign', 'User'],
      fallback: true
    });
  }
});

// 4.5 Get Active Users for Persona Testing (with caching)
app.get('/api/org/users', async (req, res) => {
  const targetOrg = (req.query.alias as string) || 'sandboxOrg';
  if (!isValidAlias(targetOrg)) return res.status(400).json({ success: false, error: 'Invalid alias' });
  const forceRefresh = req.query.refresh === 'true';
  const orgUsersCacheFile = safeJoin(cacheDir, `users-${targetOrg}.json`);

  if (!forceRefresh && fs.existsSync(orgUsersCacheFile)) {
    try {
      const cached = JSON.parse(fs.readFileSync(orgUsersCacheFile, 'utf-8'));
      return res.json({ success: true, ...cached, fromCache: true });
    } catch {}
  }

  try {
    const result = await executeSfCommand(
      ['data', 'query', '-o', targetOrg, '-q', 'SELECT Id, Name, Username, Email, Profile.Name, UserRole.Name, IsActive FROM User WHERE IsActive = true ORDER BY Name LIMIT 60', '--json'],
      { timeoutMs: 30000 }
    );
    const records = result?.records || [];

    const defaultAdmin = {
      id: 'admin',
      name: 'Current Admin',
      username: process.env.SF_USERNAME || '',
      email: process.env.SF_USERNAME || '',
      profileName: 'System Administrator',
      roleName: 'System Admin',
      isDefault: true
    };

    const users = [
      defaultAdmin,
      ...records
        .filter((r: any) => !r.Username.startsWith('autoproc') && !r.Username.includes('security') && r.Username !== defaultAdmin.username)
        .map((r: any) => ({
          id: r.Id,
          name: r.Name,
          username: r.Username,
          email: r.Email,
          profileName: r.Profile?.Name || 'Standard User',
          roleName: r.UserRole?.Name || '',
          isDefault: false
        }))
    ];

    const data = {
      alias: targetOrg,
      count: users.length,
      users,
      timestamp: new Date().toISOString()
    };

    fs.writeFileSync(orgUsersCacheFile, JSON.stringify(data, null, 2), 'utf-8');
    res.json({ success: true, ...data, fromCache: false });
  } catch (err: any) {
    const fallbackUsers = [
      { id: 'admin', name: 'System Admin', username: 'admin@salesforce.com', email: 'admin@salesforce.com', profileName: 'System Administrator', roleName: 'System Admin', isDefault: true },
      { id: 'sales_rep', name: 'Sales Representative', username: 'sales.rep@salesforce.com', email: 'sales.rep@salesforce.com', profileName: 'Standard Sales User', roleName: 'Sales Rep', isDefault: false },
      { id: 'service_rep', name: 'Support Specialist', username: 'support@salesforce.com', email: 'support@salesforce.com', profileName: 'Standard Support User', roleName: 'Customer Support', isDefault: false },
      { id: 'marketing_user', name: 'Marketing Lead', username: 'marketing@salesforce.com', email: 'marketing@salesforce.com', profileName: 'Marketing User', roleName: 'Marketing Specialist', isDefault: false },
      { id: 'read_only', name: 'Auditor User', username: 'auditor@salesforce.com', email: 'auditor@salesforce.com', profileName: 'Read Only', roleName: 'Compliance', isDefault: false }
    ];
    res.json({ success: true, alias: targetOrg, count: fallbackUsers.length, users: fallbackUsers, fallback: true });
  }
});

// 3. Resolve Steps
app.post('/api/resolve', (req, res) => {
  try {
    const { rawSteps, objectContext, targetOrg } = req.body;
    if (!Array.isArray(rawSteps) || rawSteps.length === 0) {
      return res.status(400).json({ error: 'rawSteps must be a non-empty array of strings' });
    }

    const { lines } = expandBlocks(rawSteps.map((x: any) => String(x)));
    const { steps: resolved } = resolveWithCatalog(targetOrg || 'sandboxOrg', lines, objectContext);
    res.json({ success: true, steps: resolved });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to resolve test steps' });
  }
});

// 4. Generate Playwright Spec
app.post('/api/generate', (req, res) => {
  try {
    const { ticketNumber, testId, testName, project, objectName, steps, targetOrg, dataRows } = req.body;
    if (testId && !isValidTestId(testId)) {
      return res.status(400).json({ error: 'Test case ID may only contain letters, digits, _ and -' });
    }
    const built = buildSpec(
      {
        id: testId || 'TC_CUSTOM',
        ticketNumber,
        testName,
        project,
        objectName,
        steps: Array.isArray(steps) && typeof steps[0] === 'object' ? steps.map((st: any) => st.rawText) : steps,
        dataRows: Array.isArray(dataRows) ? dataRows : undefined
      },
      targetOrg || 'sandboxOrg'
    );
    res.json({ success: true, specCode: built.specCode, resolvedSteps: built.resolvedSteps, expandedSteps: built.expandedLines, blockErrors: built.blockErrors });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to generate Playwright spec' });
  }
});

// Step authoring (describe, step check, blocks, screenshots) and suite (history, tags, nightly) routes
registerStepRoutes(app);
registerSuiteRoutes(app);

if (!fs.existsSync(targetDir)) {
  fs.mkdirSync(targetDir, { recursive: true });
}
if (!fs.existsSync(definitionsDir)) {
  fs.mkdirSync(definitionsDir, { recursive: true });
}

// Seed default test case definitions
function ensureSeedDefinitions() {
  const seeds = [
    {
      id: 'TC_001',
      ticketNumber: 'CRM-101',
      testName: 'Create New Account with Industry and Billing Address',
      scenarioType: 'positive',
      project: 'Sales Cloud',
      objectName: 'Account',
      steps: `1. Log in to Salesforce
2. Navigate to Account tab
3. Click on New button
4. Enter "Acme Corporation" in Account Name
5. Select "Technology" from Industry picklist
6. Enter "100 Market St, San Francisco" in Billing Address
7. Click on Save button
8. Verify toast "Account was created"
9. Verify field "Account Name" equals "Acme Corporation"`,
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    },
    {
      id: 'TC_002',
      ticketNumber: 'CRM-102',
      testName: 'Create and Qualify Lead for Enterprise Sales',
      scenarioType: 'positive',
      project: 'Sales Cloud',
      objectName: 'Lead',
      steps: `1. Open Leads tab
2. Click on New button
3. Enter "Sarah" in First Name
4. Enter "Connor" in Last Name
5. Enter "Cyberdyne Systems" in Company
6. Select "Working - Contacted" from Lead Status
7. Click on Save button
8. Verify field "Lead Status" equals "Working - Contacted"`,
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    },
    {
      id: 'TC_003',
      ticketNumber: 'SRV-201',
      testName: 'Log Customer Support Case and Verify Priority Escalation',
      scenarioType: 'negative',
      project: 'Service Cloud',
      objectName: 'Case',
      steps: `1. Open Cases tab
2. Click on New button
3. Select "High" from Priority picklist
4. Select "Email" from Case Origin
5. Enter "API Gateway Timeout" in Subject
6. Click on Save button
7. Verify field "Priority" equals "High"`,
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    },
    {
      id: 'TC_004',
      ticketNumber: 'CRM-103',
      testName: 'Update Opportunity Stage to Closed Won and Validate Amount',
      scenarioType: 'positive',
      project: 'Sales Cloud',
      objectName: 'Opportunity',
      steps: `1. Open Opportunities tab
2. Click on recent Opportunity record
3. Click on Edit button
4. Select "Closed Won" from Stage picklist
5. Enter "250000" in Amount field
6. Click on Save button
7. Verify Stage is "Closed Won"`,
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    },
    {
      id: 'TC_005',
      ticketNumber: 'CRM-104',
      testName: 'Create New Contact Associated with Account',
      scenarioType: 'positive',
      project: 'Sales Cloud',
      objectName: 'Contact',
      steps: `1. Open Contacts tab
2. Click on New button
3. Enter "Alex" in First Name
4. Enter "Morgan" in Last Name
5. Enter "alex.morgan@example.com" in Email field
6. Click on Save button
7. Verify toast "Contact was created"`,
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    }
  ];

  for (const seed of seeds) {
    const filePath = path.join(definitionsDir, `${seed.id}.json`);
    if (!fs.existsSync(filePath)) {
      const specPath = path.join(targetDir, `${seed.id}.spec.ts`);
      let specCode = '';
      if (fs.existsSync(specPath)) {
        specCode = fs.readFileSync(specPath, 'utf-8');
      }
      fs.writeFileSync(filePath, JSON.stringify({ ...seed, specCode }, null, 2), 'utf-8');
    }
  }
}
ensureSeedDefinitions();

// 5. Save Spec File directly
app.post('/api/save', (req, res) => {
  try {
    const { testId, specCode } = req.body;
    if (!testId || !specCode) {
      return res.status(400).json({ error: 'testId and specCode are required' });
    }

    const filePath = path.join(targetDir, `${testId}.spec.ts`);
    fs.writeFileSync(filePath, specCode, 'utf-8');

    // Also update definition if exists
    const defPath = path.join(definitionsDir, `${testId}.json`);
    if (fs.existsSync(defPath)) {
      try {
        const def = JSON.parse(fs.readFileSync(defPath, 'utf-8'));
        def.specCode = specCode;
        def.updatedAt = new Date().toISOString();
        fs.writeFileSync(defPath, JSON.stringify(def, null, 2), 'utf-8');
      } catch {}
    }

    res.json({ success: true, filePath });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to save spec' });
  }
});

// 5.1 Test Cases CRUD Endpoints
// List all test cases
app.get('/api/testcases', (req, res) => {
  try {
    const files = fs.existsSync(definitionsDir) ? fs.readdirSync(definitionsDir).filter(f => f.endsWith('.json')) : [];
    const testCases: any[] = [];

    for (const f of files) {
      try {
        const content = fs.readFileSync(path.join(definitionsDir, f), 'utf-8');
        const parsed = JSON.parse(content);
        const specFile = path.join(targetDir, `${parsed.id}.spec.ts`);
        const hasSpec = fs.existsSync(specFile);
        const stepLines = typeof parsed.steps === 'string'
          ? parsed.steps.split('\n').filter((s: string) => s.trim().length > 0)
          : (Array.isArray(parsed.steps) ? parsed.steps : []);

        testCases.push({
          ...parsed,
          scenarioType: parsed.scenarioType || (
            (parsed.testName && (
              parsed.testName.toLowerCase().includes('negative') ||
              parsed.testName.toLowerCase().includes('validation') ||
              parsed.testName.toLowerCase().includes('escalat') ||
              parsed.testName.toLowerCase().includes('read-only') ||
              parsed.testName.toLowerCase().includes('error') ||
              parsed.testName.toLowerCase().includes('fail')
            ))
              ? 'negative'
              : 'positive'
          ),
          stepCount: stepLines.length,
          hasSpec,
          tags: Array.isArray(parsed.tags) ? parsed.tags : [],
          priority: parsed.priority || 'Medium',
          owner: parsed.owner || '',
          dataRowCount: Array.isArray(parsed.dataRows) ? parsed.dataRows.length : 0,
          history: summarizeHistory(parsed.id)
        });
      } catch {}
    }

    // Sort by updatedAt desc
    testCases.sort((a, b) => (b.updatedAt || '').localeCompare(a.updatedAt || '') || a.id.localeCompare(b.id));

    res.json({ success: true, testCases });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get single test case definition
app.get('/api/testcases/:id', (req, res) => {
  try {
    const { id } = req.params;
    if (!isValidTestId(id)) return res.status(400).json({ error: 'Invalid test case id' });
    const filePath = path.join(definitionsDir, `${id}.json`);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: `Test case ${id} not found` });
    }
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const specFile = path.join(targetDir, `${id}.spec.ts`);
    if (!parsed.specCode && fs.existsSync(specFile)) {
      parsed.specCode = fs.readFileSync(specFile, 'utf-8');
    }
    res.json({ success: true, testCase: parsed });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Save / update test case definition and spec
app.post('/api/testcases/save', (req, res) => {
  try {
    const { id, ticketNumber, testName, scenarioType, project, objectName, steps, specCode, lastStatus, lastDurationMs, tags, priority, owner, dataRows, codeEdited } = req.body;
    if (!id) {
      return res.status(400).json({ error: 'id is required' });
    }
    if (!isValidTestId(id)) {
      return res.status(400).json({ error: 'Test case ID may only contain letters, digits, _ and -' });
    }

    const filePath = path.join(definitionsDir, `${id}.json`);
    let existingData: any = {};
    if (fs.existsSync(filePath)) {
      try {
        existingData = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      } catch {}
    }

    const stepsStr = Array.isArray(steps) ? steps.join('\n') : (steps || '');

    const testCase = {
      ...existingData,
      id,
      ticketNumber: ticketNumber || existingData.ticketNumber || '',
      testName: testName || existingData.testName || id,
      scenarioType: scenarioType || existingData.scenarioType || 'positive',
      project: project || existingData.project || 'Sales Cloud',
      objectName: objectName || existingData.objectName || 'Account',
      steps: stepsStr,
      specCode: specCode || existingData.specCode || '',
      lastStatus: lastStatus !== undefined ? lastStatus : (existingData.lastStatus || 'idle'),
      lastDurationMs: lastDurationMs !== undefined ? lastDurationMs : existingData.lastDurationMs,
      tags: Array.isArray(tags)
        ? [...new Set(tags.map((t: any) => String(t).trim()).filter(Boolean))]
        : existingData.tags || [],
      priority: priority || existingData.priority || 'Medium',
      owner: owner !== undefined ? String(owner) : existingData.owner || '',
      dataRows: Array.isArray(dataRows) ? dataRows : existingData.dataRows || [],
      codeEdited: codeEdited !== undefined ? Boolean(codeEdited) : Boolean(existingData.codeEdited),
      updatedAt: new Date().toISOString()
    };

    fs.writeFileSync(filePath, JSON.stringify(testCase, null, 2), 'utf-8');

    // Also write spec file if provided
    if (specCode) {
      const specPath = path.join(targetDir, `${id}.spec.ts`);
      fs.writeFileSync(specPath, specCode, 'utf-8');
    }

    res.json({ success: true, testCase });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to save test case' });
  }
});

// Save multiple test cases (e.g. from Excel / CSV import)
app.post('/api/testcases/save-batch', (req, res) => {
  try {
    const { testCases } = req.body;
    if (!Array.isArray(testCases) || testCases.length === 0) {
      return res.status(400).json({ error: 'testCases array must not be empty' });
    }

    let savedCount = 0;
    const results: any[] = [];
    const failures: Array<{ id: string; error: string }> = [];

    for (const tc of testCases) {
      const id = String(tc.id || `TC_IMP_${Date.now()}_${savedCount + 1}`).trim();
      try {
        if (!isValidTestId(id)) throw new Error('Test ID may only contain letters, digits, _ and - (max 80)');
        const filePath = path.join(definitionsDir, `${id}.json`);
        let existingData: any = {};
        if (fs.existsSync(filePath)) {
          try { existingData = JSON.parse(fs.readFileSync(filePath, 'utf-8')); } catch {}
        }
        const stepsStr = Array.isArray(tc.steps) ? tc.steps.join('\n') : (tc.steps || '');
        const testCase: any = {
          ...existingData,
          id,
          ticketNumber: tc.ticketNumber || existingData.ticketNumber || id,
          testName: tc.testName || existingData.testName || id,
          scenarioType: tc.scenarioType || existingData.scenarioType || 'positive',
          project: tc.project || existingData.project || 'Sales Cloud',
          objectName: tc.objectName || existingData.objectName || 'Account',
          steps: stepsStr,
          specCode: tc.specCode || existingData.specCode || '',
          lastStatus: tc.lastStatus || existingData.lastStatus || 'idle',
          updatedAt: new Date().toISOString()
        };
        // Optional fields kept for Excel round-trips
        if (Array.isArray(tc.tags)) testCase.tags = tc.tags.map((t: any) => String(t).trim()).filter(Boolean);
        if (tc.priority && ['High', 'Medium', 'Low'].includes(tc.priority)) testCase.priority = tc.priority;
        if (typeof tc.owner === 'string' && tc.owner) testCase.owner = tc.owner;
        if (typeof tc.persona === 'string' && tc.persona) testCase.persona = tc.persona;
        if (Array.isArray(tc.dataRows)) testCase.dataRows = tc.dataRows;
        fs.writeFileSync(filePath, JSON.stringify(testCase, null, 2), 'utf-8');
        results.push(testCase);
        savedCount++;
      } catch (e: any) {
        failures.push({ id, error: e?.message || 'Could not save' });
      }
    }

    res.json({
      success: savedCount > 0 || failures.length === 0,
      count: savedCount,
      testCases: results,
      failures,
      error: savedCount === 0 && failures.length ? failures[0].error : undefined
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to save batch test cases' });
  }
});

const trashDir = path.resolve(ORGWRIGHT_DIR, 'trash');
try { fs.mkdirSync(trashDir, { recursive: true }); } catch {}

/** Moves a test definition (and its spec) to .orgwright/trash so it can be restored. */
function trashTestCase(id: string): boolean {
  const defFile = path.join(definitionsDir, `${id}.json`);
  const specFile = path.join(targetDir, `${id}.spec.ts`);
  let moved = false;
  if (fs.existsSync(defFile)) {
    fs.copyFileSync(defFile, path.join(trashDir, `${id}.json`));
    fs.unlinkSync(defFile);
    moved = true;
  }
  if (fs.existsSync(specFile)) {
    fs.copyFileSync(specFile, path.join(trashDir, `${id}.spec.ts`));
    fs.unlinkSync(specFile);
  }
  return moved;
}

// Delete test case (soft: moved to .orgwright/trash; POST /restore brings it back)
app.delete('/api/testcases/:id', (req, res) => {
  try {
    const { id } = req.params;
    if (!isValidTestId(id)) return res.status(400).json({ error: 'Invalid test case id' });
    trashTestCase(id);
    res.json({ success: true, message: `Deleted ${id}`, restorable: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Restore a soft-deleted test case
app.post('/api/testcases/:id/restore', (req, res) => {
  try {
    const { id } = req.params;
    if (!isValidTestId(id)) return res.status(400).json({ error: 'Invalid test case id' });
    const trashed = path.join(trashDir, `${id}.json`);
    if (!fs.existsSync(trashed)) return res.status(404).json({ success: false, error: `${id} is not in the trash` });
    fs.copyFileSync(trashed, path.join(definitionsDir, `${id}.json`));
    fs.unlinkSync(trashed);
    const trashedSpec = path.join(trashDir, `${id}.spec.ts`);
    if (fs.existsSync(trashedSpec)) {
      fs.copyFileSync(trashedSpec, path.join(targetDir, `${id}.spec.ts`));
      fs.unlinkSync(trashedSpec);
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// List distinct projects with count
app.get('/api/projects', (req, res) => {
  try {
    const files = fs.existsSync(definitionsDir) ? fs.readdirSync(definitionsDir).filter(f => f.endsWith('.json')) : [];
    const projectMap = new Map<string, number>();

    for (const f of files) {
      try {
        const content = fs.readFileSync(path.join(definitionsDir, f), 'utf-8');
        const parsed = JSON.parse(content);
        const p = parsed.project || 'General';
        projectMap.set(p, (projectMap.get(p) || 0) + 1);
      } catch {}
    }

    const projects = Array.from(projectMap.entries()).map(([name, count]) => ({ name, count }));
    res.json({ success: true, projects });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Batch Execution
app.post('/api/testcases/run-batch', async (req, res) => {
  let started = false;
  let startedBatchId = '';
  try {
    const { testIds, headed = true, browser = 'chrome', projectName, tag, targetOrg } = req.body;
    if ((!Array.isArray(testIds) || testIds.length === 0) && !tag) {
      return res.status(400).json({ error: 'Select test cases or a tag to run' });
    }
    // Validate the selection BEFORE telling the UI a run has started
    let candidates = readDefinitions();
    if (Array.isArray(testIds) && testIds.length) candidates = candidates.filter((d) => testIds.includes(d.id));
    if (tag) candidates = candidates.filter((d) => hasTag(d, tag));
    candidates = candidates.filter((d) => isValidTestId(d.id));
    if (candidates.length === 0) {
      return res.status(400).json({ error: 'No runnable test cases found for this selection' });
    }
    const batchId = `BATCH_${Date.now()}`;
    broadcast({ type: 'start', testId: batchId, browser, timestamp: new Date().toISOString(), isBatch: true, testIds });
    started = true;
    startedBatchId = batchId;

    // Specs are regenerated from steps (unless hand-edited) and every test gets its own result
    const summary = await runSuite({
      testIds: Array.isArray(testIds) && testIds.length ? testIds : undefined,
      tag,
      headed: Boolean(headed),
      browser,
      alias: targetOrg || 'sandboxOrg',
      source: 'batch',
      onLog: (line) => broadcast({ type: 'log', testId: batchId, line })
    });
    if (summary.total === 0) {
      broadcast({ type: 'stopped', testId: batchId, reason: 'No runnable test cases found for this selection' });
      return res.status(400).json({ error: 'No runnable test cases found for this selection' });
    }

    const status = summary.failed > 0 || summary.unknown > 0 ? 'failed' : 'passed';
    const fullReport = {
      testId: batchId,
      status,
      durationMs: summary.durationMs,
      startedAt: summary.startedAt,
      finishedAt: summary.finishedAt,
      title: projectName ? `${projectName} regression` : tag ? `${tag} suite` : 'Regression run',
      suite: summary
    };
    broadcast({ type: 'finished', testId: batchId, report: fullReport, isBatch: true });
    res.json({ success: true, report: fullReport });
  } catch (err: any) {
    // Always send a terminal event so the UI leaves the "running" state
    if (started) broadcast({ type: 'stopped', testId: startedBatchId, reason: err?.message || 'Batch run failed' });
    res.status(500).json({ error: err.message || 'Failed to run batch regression' });
  }
});

function createJiraTestProofDoc(reportData: any): string {
  try {
    const resultsDir = path.resolve(rootPlaywrightDir, 'test-results');
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }
    const testId = isValidTestId(reportData.testId) ? reportData.testId : 'TC_RUN';
    const jsonTmpPath = path.join(resultsDir, `${testId}-evidence-input.json`);
    const outDocxPath = path.join(resultsDir, `${testId}-Jira-Test-Proof.docx`);

    fs.writeFileSync(jsonTmpPath, JSON.stringify(reportData, null, 2), 'utf-8');

    const scriptPath = path.resolve(rootPlaywrightDir, 'orgwright', 'apps', 'studio', 'server', 'generate_jira_proof.py');
    execFileSync('python', [scriptPath, jsonTmpPath, outDocxPath], {
      timeout: 15000,
      encoding: 'utf-8',
      stdio: 'pipe'
    });

    try { fs.unlinkSync(jsonTmpPath); } catch {}
    return outDocxPath;
  } catch (err: any) {
    console.error('[Orgwright Studio] Failed generating Jira proof docx:', err.message);
    return '';
  }
}

// 6. Execute Test Live (Single Persona or Default)
app.post('/api/execute', async (req, res) => {
  try {
    const { testId, specCode, headed = true, ticketNumber, testName, browser = 'chrome', persona, pauseOnFailure = true, codeEdited } = req.body;
    const tid = testId || 'TC_RUN_' + Date.now();
    if (!isValidTestId(tid)) {
      return res.status(400).json({ error: 'Test case ID may only contain letters, digits, _ and -' });
    }
    // Results are reported against the steps that actually ran ("Use block" lines expanded)
    const steps = expandBlocks(toLines(req.body.steps || [])).lines;
    clearTelemetry(tid);
    
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    const specFileRel = `tests/generated/${tid}.spec.ts`;
    const specFilePath = path.join(rootPlaywrightDir, specFileRel);

    if (specCode) {
      fs.writeFileSync(specFilePath, specCode, 'utf-8');
    }

    let customEnv: Record<string, string> = {
      // Headed Studio runs pause on a failing step so the tester can repair it in the live browser
      ORGWRIGHT_PAUSE_ON_FAILURE: headed && pauseOnFailure ? '1' : '0',
      // Keep the final screen visible briefly in headed runs (replaces the fixed 3s wait in specs)
      ORGWRIGHT_HOLD_MS: headed ? '1500' : '0'
    };
    if (persona && persona.id && persona.id !== 'admin') {
      const personaStorage = path.resolve(rootPlaywrightDir, `.auth/user-${persona.id}.json`);
      customEnv = {
        ...customEnv,
        PLAYWRIGHT_STORAGE_STATE: personaStorage,
        PLAYWRIGHT_PERSONA_ID: persona.id,
        PLAYWRIGHT_PERSONA_NAME: persona.name || persona.id,
        PLAYWRIGHT_PERSONA_PROFILE: persona.profileName || ''
      };
    }

    broadcast({ type: 'start', testId: tid, browser, persona, timestamp: new Date().toISOString() });

    const report = await executePlaywrightTest({
      specFile: specFileRel,
      cwd: rootPlaywrightDir,
      headed: Boolean(headed),
      browserChannel: browser || 'chrome',
      project: 'salesforce',
      ticketNumber,
      testId: tid,
      testName: persona?.name ? `${testName || tid} [${persona.name}]` : testName,
      env: customEnv,
      onLog: (line) => {
        broadcast({ type: 'log', testId: tid, line });
      }
    });

    // Update definition lastStatus & duration
    const defPath = path.join(definitionsDir, `${tid}.json`);
    if (fs.existsSync(defPath)) {
      try {
        const def = JSON.parse(fs.readFileSync(defPath, 'utf-8'));
        def.lastStatus = report.status;
        def.lastDurationMs = report.durationMs;
        def.lastRunAt = new Date().toISOString();
        if (persona?.name) {
          def.lastPersona = `${persona.name} [${persona.profileName || 'User'}]`;
        }
        if (specCode) def.specCode = specCode;
        if (codeEdited !== undefined) def.codeEdited = Boolean(codeEdited);
        fs.writeFileSync(defPath, JSON.stringify(def, null, 2), 'utf-8');
      } catch {}
    }

    let failureAnalysis: FailureAnalysis | undefined;
    if (report.status === 'failed') {
      failureAnalysis = analyzeTestFailure(
        report.rawLogs || '',
        rootPlaywrightDir,
        tid,
        steps || []
      );
      lastFailureAnalysis = failureAnalysis;
    }

    // Capture step telemetry (per data row when data-driven) and Salesforce backend automation logs
    const outcome = readRunOutcome(tid);
    const stepTelemetry: any[] = outcome.telemetry;
    if (report.status === 'passed' || report.status === 'failed') {
      appendHistory(tid, {
        at: new Date().toISOString(),
        status: report.status,
        durationMs: report.durationMs,
        source: 'studio',
        failedStep: outcome.failedStep,
        error: outcome.error || failureAnalysis?.errorTitle,
        persona: persona?.name,
        rows: outcome.rows ? { passed: outcome.rows.passed, total: outcome.rows.total } : undefined
      });
    }

    const targetOrg = (req.body.targetOrg as string) || 'sandboxOrg';
    const recentSfLogs = fetchRecentSalesforceLogs(targetOrg, report.startedAt, 5);

    const executionLogReport: FullExecutionLogReport & { dataRows?: any } = buildExecutionLogReport(
      // Screenshots/telemetry are keyed by the row for data-driven runs (e.g. TC_001__row2)
      outcome.telemetryId || tid,
      steps || [],
      report.status === 'passed' ? 'passed' : 'failed',
      report.durationMs,
      stepTelemetry,
      recentSfLogs,
      failureAnalysis
    );
    if (outcome.rows) executionLogReport.dataRows = outcome.rows;
    lastExecutionLogReport = executionLogReport;

    try {
      const execLogPath = path.resolve(rootPlaywrightDir, 'test-results', `${tid}-execution-logs.json`);
      fs.writeFileSync(execLogPath, JSON.stringify(executionLogReport, null, 2), 'utf-8');
    } catch {}

    // Generate Executive Jira Test Proof Word Document
    let jiraProofPath = '';
    try {
      jiraProofPath = createJiraTestProofDoc({
        testId: tid,
        ticketNumber: ticketNumber || tid,
        testName: persona?.name ? `${testName || tid} [${persona.name}]` : (testName || tid),
        scenarioType: req.body.scenarioType || 'positive',
        projectName: req.body.projectName || 'Sales Cloud',
        targetOrg,
        persona,
        status: report.status,
        durationMs: report.durationMs,
        executedAt: new Date().toISOString(),
        steps: steps || [],
        stepTelemetry,
        recentSfLogs,
        failureAnalysis
      });
    } catch (err: any) {
      console.error('[Orgwright Studio] Jira proof generation notice:', err.message);
    }

    const jiraProofUrl = `/api/test-results/jira-proof?testId=${tid}`;
    const fullReport = { ...report, failureAnalysis, persona, executionLogReport, jiraProofUrl };
    broadcast({ type: 'finished', testId: tid, report: fullReport });
    broadcast({ type: 'execution_log_report', testId: tid, report: executionLogReport });
    broadcast({ type: 'jira_proof_ready', testId: tid, downloadUrl: jiraProofUrl });
    res.json({ success: true, report: fullReport, jiraProofUrl });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to execute test' });
  }
});

// 6.00 Download Jira Test Proof Word Document (.docx)
app.get('/api/test-results/jira-proof', (req, res) => {
  const testId = req.query.testId as string;
  if (!testId || !isValidTestId(testId)) {
    return res.status(400).json({ error: 'A valid testId is required to download a Jira test proof' });
  }
  const resultsDir = path.resolve(rootPlaywrightDir, 'test-results');
  const docxPath = path.join(resultsDir, `${testId}-Jira-Test-Proof.docx`);

  if (fs.existsSync(docxPath)) {
    res.setHeader('Content-Disposition', `attachment; filename="${testId}_Jira_Test_Proof.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    return res.sendFile(docxPath);
  }

  // Generate on demand from saved definition if available
  const defPath = path.join(definitionsDir, `${testId}.json`);
  let defData: any = {};
  if (fs.existsSync(defPath)) {
    try { defData = JSON.parse(fs.readFileSync(defPath, 'utf-8')); } catch {}
  }
  // Never fabricate a result: only generate proof for a test that has actually been executed
  const executedStatus = defData.lastStatus;
  if (executedStatus !== 'passed' && executedStatus !== 'failed') {
    return res.status(409).json({ error: `No stored execution result for ${testId}. Run the test before downloading a Jira proof.` });
  }
  const stepLines = typeof defData.steps === 'string' ? defData.steps.split('\n') : (defData.steps || []);

  const generatedPath = createJiraTestProofDoc({
    testId,
    ticketNumber: defData.ticketNumber || testId,
    testName: defData.testName || testId,
    scenarioType: defData.scenarioType || 'positive',
    projectName: defData.project || 'Sales Cloud',
    status: executedStatus,
    durationMs: typeof defData.lastDurationMs === 'number' ? defData.lastDurationMs : 0,
    steps: stepLines
  });

  if (generatedPath && fs.existsSync(generatedPath)) {
    res.setHeader('Content-Disposition', `attachment; filename="${testId}_Jira_Test_Proof.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    return res.sendFile(generatedPath);
  }

  res.status(404).json({ error: 'Jira test proof document not found' });
});

// 6.01 Stop Live Automation Execution (Single Test or Batch Regression)
app.post(['/api/execute/stop', '/api/stop'], (req, res) => {
  try {
    isExecutionPaused = false;
    const stopped = stopTestExecution();
    broadcast({
      type: 'stopped',
      message: 'Automation execution stopped by user',
      timestamp: new Date().toISOString()
    });
    broadcast({
      type: 'log',
      line: '\n[Orgwright Studio] â¹ Automation execution stopped by user.\n'
    });
    res.json({ success: true, stopped });
  } catch (err: any) {
    console.error('[Orgwright Studio] Failed to stop test execution:', err);
    res.status(500).json({ error: err.message || 'Failed to stop test execution' });
  }
});

// 6.02 Pause Live Automation Execution
app.post('/api/execute/pause', (req, res) => {
  try {
    isExecutionPaused = true;
    broadcast({
      type: 'pause_execution',
      timestamp: new Date().toISOString()
    });
    broadcast({
      type: 'execution_paused',
      message: 'Automation execution paused. Live screen is interactive.',
      timestamp: new Date().toISOString()
    });
    broadcast({
      type: 'log',
      line: '\n[Orgwright Studio] â¸ Automation execution paused by user. Live screen is interactive.\n'
    });
    res.json({ success: true, isPaused: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to pause test execution' });
  }
});

// Repair a failed step while the live run is paused on it:
//   action "clicked" = the tester already did the step in the live browser, continue
//   action "retry"   = run the edited step text instead
//   action "fail"    = give up and let the test fail
app.post('/api/execute/repair', (req, res) => {
  const { action, newStep } = req.body || {};
  if (!['clicked', 'retry', 'fail'].includes(action)) {
    return res.status(400).json({ error: 'action must be clicked, retry or fail' });
  }
  if (action === 'retry' && !String(newStep || '').trim()) {
    return res.status(400).json({ error: 'newStep is required to retry' });
  }
  broadcast({ type: 'repair_decision', action, newStep: newStep ? String(newStep) : undefined });
  broadcast({ type: 'log', line: `\n[Orgwright Studio] Repair: ${action}${newStep ? ` -> ${newStep}` : ''}\n` });
  res.json({ success: true });
});

// 6.03 Resume Live Automation Execution
app.post('/api/execute/resume', (req, res) => {
  try {
    const { steps, newSteps, pausedStepIndex } = req.body;
    isExecutionPaused = false;
    broadcast({
      type: 'resume_execution',
      steps,
      newSteps,
      pausedStepIndex,
      timestamp: new Date().toISOString()
    });
    broadcast({
      type: 'execution_resumed',
      message: 'Automation execution resumed.',
      timestamp: new Date().toISOString()
    });
    broadcast({
      type: 'log',
      line: '\n[Orgwright Studio] â–¶ Automation execution resumed by user.\n'
    });
    res.json({ success: true, isPaused: false });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to resume test execution' });
  }
});

// 6.04 Execution State Status
app.get('/api/execute/status', (req, res) => {
  res.json({
    isRunning: isTestRunning(),
    isPaused: isExecutionPaused
  });
});

// 6.05 Get Execution & Backend Automation Logs
app.get('/api/test-results/execution-logs', (req, res) => {
  const tid = req.query.testId as string;
  if (tid) {
    const execLogPath = path.resolve(rootPlaywrightDir, 'test-results', `${tid}-execution-logs.json`);
    if (fs.existsSync(execLogPath)) {
      try {
        const raw = fs.readFileSync(execLogPath, 'utf-8').replace(/^\uFEFF/, '');
        const data = JSON.parse(raw);
        return res.json({ success: true, report: data });
      } catch {}
    }
  }
  res.json({ success: true, report: lastExecutionLogReport });
});

// 6.06 petrkindlmann/qa-skills Catalog & Spec Audit
app.get('/api/qa-skills/catalog', (_req, res) => {
  res.json({ success: true, skills: getQASkillsCatalog() });
});

app.post('/api/qa-skills/audit', (req, res) => {
  const { specCode } = req.body;
  if (!specCode) {
    return res.status(400).json({ error: 'specCode is required' });
  }
  const audit = auditPlaywrightSpec(specCode);
  res.json({ success: true, audit });
});

app.get('/api/qa-skills/context', (_req, res) => {
  const contextPath = path.resolve(rootPlaywrightDir, '.agents', 'qa-project-context.md');
  if (fs.existsSync(contextPath)) {
    const content = fs.readFileSync(contextPath, 'utf-8');
    return res.json({ success: true, content });
  }
  res.json({ success: false, message: 'QA project context not initialized' });
});

// 6.1 Multi-User Matrix Execution Across Selected Personas
app.post('/api/testcases/run-matrix', async (req, res) => {
  try {
    const { testId, specCode, personas, headed = true, browser = 'chrome', ticketNumber, testName, steps } = req.body;
    if (!Array.isArray(personas) || personas.length === 0) {
      return res.status(400).json({ error: 'personas array must not be empty' });
    }

    const tid = testId || 'TC_002';
    const specFileRel = `tests/generated/${tid}.spec.ts`;
    const specFilePath = path.join(rootPlaywrightDir, specFileRel);

    if (specCode) {
      fs.writeFileSync(specFilePath, specCode, 'utf-8');
    }

    const matrixId = `MATRIX_${Date.now()}`;
    const startTime = Date.now();

    broadcast({
      type: 'matrix_start',
      matrixId,
      testId: tid,
      testName: testName || tid,
      totalPersonas: personas.length,
      personas,
      timestamp: new Date().toISOString()
    });

    const results: any[] = [];

    for (let i = 0; i < personas.length; i++) {
      const persona = personas[i];
      const personaRunId = `${tid}_${persona.id || i}`;

      broadcast({
        type: 'matrix_persona_start',
        matrixId,
        personaIndex: i,
        persona,
        totalPersonas: personas.length
      });

      let customEnv: Record<string, string> | undefined = undefined;
      if (persona.id && persona.id !== 'admin') {
        const personaStorage = path.resolve(rootPlaywrightDir, `.auth/user-${persona.id}.json`);
        customEnv = {
          PLAYWRIGHT_STORAGE_STATE: personaStorage,
          PLAYWRIGHT_PERSONA_ID: persona.id,
          PLAYWRIGHT_PERSONA_NAME: persona.name || persona.id,
          PLAYWRIGHT_PERSONA_PROFILE: persona.profileName || ''
        };
      }

      const rep = await executePlaywrightTest({
        specFile: specFileRel,
        cwd: rootPlaywrightDir,
        headed: Boolean(headed),
        browserChannel: browser || 'chrome',
        project: 'salesforce',
        ticketNumber,
        testId: personaRunId,
        testName: `${testName || tid} [${persona.name} (${persona.profileName || 'Profile'})]`,
        env: customEnv,
        onLog: (line) => {
          broadcast({ type: 'matrix_persona_log', matrixId, persona, line });
        }
      });

      let failureAnalysis: FailureAnalysis | undefined;
      if (rep.status === 'failed') {
        failureAnalysis = analyzeTestFailure(rep.rawLogs || '', rootPlaywrightDir, personaRunId, steps);
      }

      const personaResult = {
        persona,
        status: rep.status,
        durationMs: rep.durationMs,
        startedAt: rep.startedAt,
        finishedAt: rep.finishedAt,
        tracePath: rep.tracePath,
        videoPath: rep.videoPath,
        failureAnalysis,
        error: rep.status === 'failed' ? (failureAnalysis?.errorTitle || 'Execution failed or timeout') : undefined
      };

      results.push(personaResult);

      broadcast({
        type: 'matrix_persona_finish',
        matrixId,
        personaIndex: i,
        persona,
        status: rep.status,
        durationMs: rep.durationMs,
        error: personaResult.error
      });
    }

    const passedCount = results.filter(r => r.status === 'passed').length;
    const failedCount = results.filter(r => r.status === 'failed').length;
    const totalCount = results.length;
    const complianceRate = totalCount > 0 ? Math.round((passedCount / totalCount) * 100) : 0;
    const totalDurationMs = Date.now() - startTime;

    const summary = {
      matrixId,
      testId: tid,
      total: totalCount,
      passed: passedCount,
      failed: failedCount,
      complianceRate: `${complianceRate}%`,
      totalDurationMs,
      completedAt: new Date().toISOString()
    };

    broadcast({
      type: 'matrix_finish',
      matrixId,
      summary,
      results
    });

    res.json({ success: true, matrixId, summary, results });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to run persona matrix' });
  }
});

// Failure Analysis Endpoints
app.get('/api/test-results/last-failure', (req, res) => {
  res.json({ success: true, failureAnalysis: lastFailureAnalysis });
});

app.get('/api/test-results/screenshot', (req, res) => {
  if (lastFailureAnalysis?.screenshotPath && fs.existsSync(lastFailureAnalysis.screenshotPath)) {
    return res.sendFile(lastFailureAnalysis.screenshotPath);
  }
  const resultsDir = path.resolve(rootPlaywrightDir, 'test-results');
  if (fs.existsSync(resultsDir)) {
    try {
      const files = fs.readdirSync(resultsDir, { recursive: true }) as string[];
      const png = files.find(f => f.endsWith('test-failed-1.png') || f.endsWith('.png'));
      if (png) {
        return res.sendFile(path.join(resultsDir, png));
      }
    } catch {}
  }
  res.status(404).json({ error: 'No screenshot available' });
});

// 6.5 Full Regression Suite Endpoints
app.get('/api/regression/list', (req, res) => {
  try {
    const files = fs.existsSync(targetDir) ? fs.readdirSync(targetDir).filter(f => f.endsWith('.spec.ts')) : [];
    res.json({
      success: true,
      tests: files.map(f => ({
        id: path.basename(f, '.spec.ts'),
        fileName: f,
        filePath: path.join(targetDir, f)
      }))
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/regression/run', async (req, res) => {
  try {
    const { headed = false, browser = 'chrome' } = req.body;
    broadcast({ type: 'start', testId: 'REGRESSION_SUITE', browser, timestamp: new Date().toISOString() });

    const report = await executePlaywrightTest({
      specFile: 'tests/generated',
      cwd: rootPlaywrightDir,
      headed: Boolean(headed),
      browserChannel: browser || 'chrome',
      project: 'salesforce',
      ticketNumber: 'REGRESSION',
      testId: 'REGRESSION_SUITE',
      testName: 'Full Regression Suite',
      onLog: (line) => {
        broadcast({ type: 'log', testId: 'REGRESSION_SUITE', line });
      }
    });

    broadcast({ type: 'finished', testId: 'REGRESSION_SUITE', report });
    res.json({ success: true, report });
  } catch (err: any) {
    res.status(500).json({ error: err.message || 'Failed to execute regression suite' });
  }
});

// 6.6 Live Interactive Browser Recorder Endpoints
app.post('/api/record/start', async (req, res) => {
  try {
    const { objectName, browser = 'chrome', startUrl, instanceUrl } = req.body;
    const result = await startRecording({
      objectName,
      browserChannel: browser,
      startUrl,
      instanceUrl,
      broadcast
    });
    res.json(result);
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/record/stop', async (req, res) => {
  try {
    const result = await stopRecording();
    res.json({ success: true, steps: result.steps });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/record/status', (req, res) => {
  res.json({ success: true, ...getRecorderStatus() });
});

app.post('/api/record/click', async (req, res) => {
  try {
    const { x, y } = req.body;
    if (isTestRunning()) {
      broadcast({ type: 'interact_click', x, y });
    }
    if (getRecorderStatus().isRecording) {
      await dispatchRecorderClick(x, y);
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/record/type', async (req, res) => {
  try {
    const { text } = req.body;
    if (typeof text === 'string') {
      if (isTestRunning()) {
        broadcast({ type: 'interact_type', text });
      }
      if (getRecorderStatus().isRecording) {
        await dispatchRecorderType(text);
      }
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/record/key', async (req, res) => {
  try {
    const { key } = req.body;
    if (typeof key === 'string') {
      if (isTestRunning()) {
        broadcast({ type: 'interact_key', key });
      }
      if (getRecorderStatus().isRecording) {
        await dispatchRecorderKeyPress(key);
      }
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/record/scroll', async (req, res) => {
  try {
    const { deltaY } = req.body;
    const dy = deltaY || 350;
    if (isTestRunning()) {
      broadcast({ type: 'interact_scroll', deltaY: dy });
    }
    if (getRecorderStatus().isRecording) {
      await dispatchRecorderScroll(dy);
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/record/logs', (req, res) => {
  try {
    const logPath = path.resolve(ROOT_DIR, 'logs', 'recorder.log');
    if (fs.existsSync(logPath)) {
      const content = fs.readFileSync(logPath, 'utf-8');
      const lines = content.split('\n').filter(l => l.trim().length > 0);
      const recent = lines.slice(-100);
      return res.json({ success: true, count: recent.length, logs: recent });
    }
    res.json({ success: true, count: 0, logs: [] });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 7. Sample Test Cases
app.get('/api/samples', (req, res) => {
  res.json([
    {
      id: 'TC_001',
      ticketNumber: 'CRM-101',
      testName: 'Create New Account with Industry and Billing Address',
      project: 'Sales Cloud',
      objectName: 'Account',
      steps: [
        '1. Log in to Salesforce',
        '2. Navigate to Account tab',
        '3. Click on New button',
        '4. Enter "Acme Corporation" in Account Name',
        '5. Select "Technology" from Industry picklist',
        '6. Enter "100 Market St, San Francisco" in Billing Address',
        '7. Click on Save button',
        '8. Verify toast "Account was created"',
        '9. Verify field "Account Name" equals "Acme Corporation"'
      ]
    },
    {
      id: 'TC_002',
      ticketNumber: 'CRM-102',
      testName: 'Create and Qualify Lead for Enterprise Sales',
      project: 'Sales Cloud',
      objectName: 'Lead',
      steps: [
        '1. Open Leads tab',
        '2. Click on New button',
        '3. Enter "Sarah" in First Name',
        '4. Enter "Connor" in Last Name',
        '5. Enter "Cyberdyne Systems" in Company',
        '6. Select "Working - Contacted" from Lead Status',
        '7. Click on Save button',
        '8. Verify field "Lead Status" equals "Working - Contacted"'
      ]
    },
    {
      id: 'TC_003',
      ticketNumber: 'SRV-201',
      testName: 'Log Customer Support Case and Verify Priority Escalation',
      project: 'Service Cloud',
      objectName: 'Case',
      steps: [
        '1. Open Cases tab',
        '2. Click on New button',
        '3. Select "High" from Priority picklist',
        '4. Select "Email" from Case Origin',
        '5. Enter "API Gateway Timeout" in Subject',
        '6. Click on Save button',
        '7. Verify field "Priority" equals "High"'
      ]
    },
    {
      id: 'TC_004',
      ticketNumber: 'CRM-103',
      testName: 'Update Opportunity Stage to Closed Won and Validate Amount',
      project: 'Sales Cloud',
      objectName: 'Opportunity',
      steps: [
        '1. Open Opportunities tab',
        '2. Click on recent Opportunity record',
        '3. Click on Edit button',
        '4. Select "Closed Won" from Stage picklist',
        '5. Enter "250000" in Amount field',
        '6. Click on Save button',
        '7. Verify Stage is "Closed Won"'
      ]
    }
  ]);
});

// 8. Salesforce Standard Test Templates Library
app.get('/api/templates', (req, res) => {
  const category = req.query.category as string;
  const search = (req.query.search as string || '').toLowerCase();
  let list = SALESFORCE_STANDARD_TEMPLATES;
  if (category && category !== 'All') {
    list = list.filter((t) => t.category === category);
  }
  if (search) {
    list = list.filter(
      (t) =>
        t.testName.toLowerCase().includes(search) ||
        t.ticketNumber.toLowerCase().includes(search) ||
        t.objectName.toLowerCase().includes(search) ||
        t.description.toLowerCase().includes(search)
    );
  }
  res.json({
    success: true,
    total: SALESFORCE_STANDARD_TEMPLATES.length,
    count: list.length,
    templates: list
  });
});

app.post('/api/templates/import', (req, res) => {
  try {
    const { templateId } = req.body;
    const tmpl = SALESFORCE_STANDARD_TEMPLATES.find((t) => t.id === templateId) || req.body.template;
    if (!tmpl) {
      return res.status(404).json({ success: false, error: 'Template not found' });
    }
    const filePath = path.join(definitionsDir, `${tmpl.id}.json`);
    const testCase = {
      id: tmpl.id,
      ticketNumber: tmpl.ticketNumber,
      testName: tmpl.testName,
      project: tmpl.category || 'Sales Cloud',
      objectName: tmpl.objectName || 'Account',
      steps: tmpl.steps,
      specCode: '',
      lastStatus: 'idle',
      updatedAt: new Date().toISOString()
    };
    fs.writeFileSync(filePath, JSON.stringify(testCase, null, 2), 'utf-8');
    res.json({ success: true, testCase });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/templates/import-all', (req, res) => {
  // body: { dryRun?: boolean, mode?: 'skip' | 'replace' }
  try {
    const dryRun = Boolean(req.body?.dryRun);
    const mode: 'skip' | 'replace' = req.body?.mode === 'replace' ? 'replace' : 'skip';
    const toAdd: string[] = [];
    const existing: string[] = [];
    for (const tmpl of SALESFORCE_STANDARD_TEMPLATES) {
      (fs.existsSync(path.join(definitionsDir, `${tmpl.id}.json`)) ? existing : toAdd).push(tmpl.id);
    }
    if (dryRun) return res.json({ success: true, dryRun: true, add: toAdd, exist: existing });

    const created: string[] = [];
    const replaced: string[] = [];
    const skipped: string[] = [];
    const failed: Array<{ id: string; error: string }> = [];
    for (const tmpl of SALESFORCE_STANDARD_TEMPLATES) {
      const filePath = path.join(definitionsDir, `${tmpl.id}.json`);
      try {
        const exists = fs.existsSync(filePath);
        if (exists && mode === 'skip') {
          skipped.push(tmpl.id);
          continue;
        }
        // Keep the old version in the trash so Undo can restore it
        if (exists) trashTestCase(tmpl.id);
        const testCase = {
          id: tmpl.id,
          ticketNumber: tmpl.ticketNumber,
          testName: tmpl.testName,
          project: tmpl.category || 'Sales Cloud',
          objectName: tmpl.objectName || 'Account',
          steps: tmpl.steps,
          specCode: '',
          lastStatus: 'idle',
          updatedAt: new Date().toISOString()
        };
        fs.writeFileSync(filePath, JSON.stringify(testCase, null, 2), 'utf-8');
        (exists ? replaced : created).push(tmpl.id);
      } catch (e: any) {
        failed.push({ id: tmpl.id, error: e?.message || 'Could not write' });
      }
    }
    res.json({ success: true, importedCount: created.length + replaced.length, created, replaced, skipped, failed });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Undo an Import All: remove what was created, restore what was replaced
app.post('/api/templates/import-all/undo', (req, res) => {
  try {
    const created: string[] = Array.isArray(req.body?.created) ? req.body.created.filter(isValidTestId) : [];
    const replaced: string[] = Array.isArray(req.body?.replaced) ? req.body.replaced.filter(isValidTestId) : [];
    for (const id of created) {
      const f = path.join(definitionsDir, `${id}.json`);
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }
    for (const id of replaced) {
      const trashed = path.join(trashDir, `${id}.json`);
      if (fs.existsSync(trashed)) {
        fs.copyFileSync(trashed, path.join(definitionsDir, `${id}.json`));
        fs.unlinkSync(trashed);
      }
    }
    res.json({ success: true });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

server.listen(port, host, () => {
  console.log(`[Orgwright Studio Server] Running on http://${host}:${port}`);
  console.log(`[Orgwright Studio Server] WebSocket listening on ws://${host}:${port}/ws`);
});
