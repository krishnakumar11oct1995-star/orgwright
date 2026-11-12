import { spawn } from 'node:child_process';
import {
  SObjectDescribe,
  SObjectDescribeSchema,
  FieldDescribe,
  FieldDescribeSchema,
  ActionDescribe,
  TabDescribe,
  OrgMetadataCache,
  OrgMetadataCacheSchema,
  SALESFORCE_DEFAULT_RELEASE
} from '@orgwright/core';

export interface CliIntrospectOptions {
  orgAlias?: string;
  timeoutMs?: number;
  commandRunner?: (command: string, args: string[], options?: { timeout?: number }) => Promise<string>;
  includeFlexipages?: boolean;
  includeQuickActions?: boolean;
  mockData?: any;
}

export interface FlexipageDescribe {
  id: string;
  developerName: string;
  masterLabel: string;
  type: string;
  entityDefinitionId?: string;
}

export interface PicklistFieldDescribe {
  apiName: string;
  label: string;
  values: Array<{
    value: string;
    label: string;
    active: boolean;
    defaultValue: boolean;
  }>;
}

/** Quote a Windows argv value, then protect it from cmd.exe expansion. */
function quoteCmdArgument(value: string): string {
  const escaped = value
    .replace(/(\\*)"/g, (_match, slashes: string) => `${slashes}${slashes}\\"`)
    .replace(/(\\+)$/, '$1$1');
  return `"${escaped}"`.replace(/([()\][%!^"`<>&|;, *?])/g, '^$1');
}

/**
 * Execute Salesforce CLI command and parse JSON output.
 */
export async function executeSfCommand(
  args: string[],
  options?: CliIntrospectOptions
): Promise<any> {
  const timeoutMs = options?.timeoutMs ?? 60000;

  if (options?.mockData !== undefined) {
    return options.mockData;
  }

  if (options?.commandRunner) {
    const rawOutput = await options.commandRunner('sf', args, { timeout: timeoutMs });
    return parseSfJson(rawOutput);
  }

  return new Promise((resolve, reject) => {
    const isWindows = process.platform === 'win32';
    const command = isWindows ? process.env.ComSpec || 'cmd.exe' : 'sf';
    const commandArgs = isWindows
      ? ['/d', '/s', '/c', `"sf.cmd ${args.map(quoteCmdArgument).join(' ')}"`]
      : args;

    const child = spawn(command, commandArgs, {
      windowsVerbatimArguments: isWindows,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let stdout = '';
    let stderr = '';
    let isTimedOut = false;

    const timer = setTimeout(() => {
      isTimedOut = true;
      try {
        child.kill();
      } catch {
        // ignore
      }
      reject(new Error(`Salesforce CLI command timed out after ${timeoutMs}ms: sf ${args.join(' ')}`));
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', (err) => {
      clearTimeout(timer);
      if (!isTimedOut) {
        reject(
          new Error(
            `Failed to spawn Salesforce CLI ('sf'). Ensure 'sf' (Salesforce CLI) is installed and available in PATH.\nOriginal error: ${err.message}`
          )
        );
      }
    });

    child.on('close', (code) => {
      clearTimeout(timer);
      if (isTimedOut) return;

      if (stdout.trim().length === 0 && stderr.trim().length > 0) {
        return reject(
          new Error(`Salesforce CLI execution error (exit code ${code}): ${stderr.trim()}`)
        );
      }

      try {
        const parsed = parseSfJson(stdout);
        if (parsed.status !== undefined && parsed.status !== 0) {
          const errMsg = parsed.message || parsed.name || stderr.trim() || `CLI returned status ${parsed.status}`;
          return reject(new Error(`Salesforce CLI command failed: ${errMsg}`));
        }
        resolve(parsed.result !== undefined ? parsed.result : parsed);
      } catch (parseErr: any) {
        if (code !== 0) {
          return reject(
            new Error(
              `Salesforce CLI failed (exit code ${code}): ${stderr || stdout || 'Unknown error'}`
            )
          );
        }
        reject(new Error(`Failed to parse Salesforce CLI output as JSON: ${parseErr.message}\nOutput was: ${stdout.slice(0, 500)}`));
      }
    });
  });
}

function parseSfJson(raw: string): any {
  // Strip any non-JSON prefix/suffix if present
  const firstBrace = raw.indexOf('{');
  const lastBrace = raw.lastIndexOf('}');
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    const jsonStr = raw.substring(firstBrace, lastBrace + 1);
    return JSON.parse(jsonStr);
  }
  return JSON.parse(raw);
}

/**
 * Maps raw Salesforce CLI/Describe field types into Orgwright normalized FieldDescribe types.
 */
export function mapSfFieldType(sfType: string): FieldDescribe['type'] {
  const normalized = (sfType || '').toLowerCase();
  switch (normalized) {
    case 'string':
    case 'id':
      return 'string';
    case 'textarea':
      return 'textarea';
    case 'picklist':
      return 'picklist';
    case 'multipicklist':
      return 'multipicklist';
    case 'boolean':
      return 'boolean';
    case 'currency':
      return 'currency';
    case 'date':
      return 'date';
    case 'datetime':
      return 'datetime';
    case 'reference':
      return 'reference';
    case 'int':
    case 'integer':
    case 'double':
    case 'percent':
    case 'number':
      return 'number';
    case 'url':
      return 'url';
    case 'email':
      return 'email';
    case 'phone':
      return 'phone';
    default:
      return 'other';
  }
}

/**
 * Introspects an SObject definition using 'sf sobject describe -s <object> -o <alias> --json'.
 */
export async function introspectSObject(
  sObjectName: string,
  orgAlias?: string,
  options?: CliIntrospectOptions
): Promise<SObjectDescribe> {
  const alias = orgAlias || options?.orgAlias;
  const args = ['sobject', 'describe', '-s', sObjectName, '--json'];
  if (alias) {
    args.push('-o', alias);
  }

  const result = await executeSfCommand(args, options);

  const rawFields: any[] = Array.isArray(result?.fields) ? result.fields : [];
  const fieldsMap: Record<string, FieldDescribe> = {};

  for (const rawField of rawFields) {
    const apiName = rawField.name;
    const label = rawField.label || apiName;
    const type = mapSfFieldType(rawField.type);

    const picklistValues: string[] = Array.isArray(rawField.picklistValues)
      ? rawField.picklistValues
          .filter((pv: any) => pv.active !== false)
          .map((pv: any) => String(pv.value ?? ''))
      : [];

    const referenceTo: string[] = Array.isArray(rawField.referenceTo)
      ? rawField.referenceTo.map((ref: any) => String(ref))
      : [];

    const required = Boolean(!rawField.nillable && !rawField.defaultedOnCreate && !rawField.calculated);
    const readOnly = Boolean(!rawField.updateable && !rawField.createable);

    const fieldParsed = FieldDescribeSchema.parse({
      apiName,
      label,
      type,
      picklistValues,
      referenceTo,
      required,
      readOnly
    });

    fieldsMap[apiName] = fieldParsed;
  }

  // Parse standard and action overrides
  const actions: ActionDescribe[] = [];
  const standardActionNames = ['New', 'Edit', 'Delete', 'Clone', 'View'];
  for (const actName of standardActionNames) {
    actions.push({
      name: actName,
      label: actName,
      type: 'StandardButton'
    });
  }

  if (Array.isArray(result?.actionOverrides)) {
    for (const ao of result.actionOverrides) {
      if (ao.name && !actions.some((a) => a.name === ao.name)) {
        actions.push({
          name: ao.name,
          label: ao.name,
          type: 'CustomButton',
          target: ao.pageOrSObjectType
        });
      }
    }
  }

  // Optional: Query quick actions if requested and org alias provided
  if (options?.includeQuickActions && alias) {
    try {
      const quickActions = await queryQuickActions(sObjectName, alias, options);
      for (const qa of quickActions) {
        if (!actions.some((a) => a.name === qa.name)) {
          actions.push(qa);
        }
      }
    } catch {
      // Fallback silently if Tooling API permissions or query fails
    }
  }

  // Parse tabs or flexipage layout
  const tabs: TabDescribe[] = [];
  let flexipageTabsFound = false;

  if (options?.includeFlexipages && alias) {
    try {
      const flexipages = await introspectFlexipages(sObjectName, alias, options);
      if (flexipages.length > 0) {
        tabs.push({
          label: 'Details',
          isDefault: true,
          fields: Object.keys(fieldsMap).slice(0, 50)
        });
        tabs.push({
          label: 'Related',
          isDefault: false,
          fields: []
        });
        flexipageTabsFound = true;
      }
    } catch {
      // Fallback to default tabs
    }
  }

  if (!flexipageTabsFound) {
    tabs.push({
      label: 'Details',
      isDefault: true,
      fields: Object.keys(fieldsMap)
    });
    tabs.push({
      label: 'Related',
      isDefault: false,
      fields: []
    });
  }

  const sObjectData = {
    apiName: result.name || sObjectName,
    label: result.label || sObjectName,
    listUrl: `/lightning/o/${result.name || sObjectName}/list?filterName=Recent`,
    tabs,
    actions,
    fields: fieldsMap
  };

  return SObjectDescribeSchema.parse(sObjectData);
}

/**
 * Introspects picklist values for a specific field on an SObject.
 */
export async function introspectPicklist(
  sObjectName: string,
  fieldName: string,
  orgAlias?: string,
  options?: CliIntrospectOptions
): Promise<PicklistFieldDescribe> {
  const sobjectDescribe = await introspectSObject(sObjectName, orgAlias, options);
  const field = sobjectDescribe.fields[fieldName];

  if (!field) {
    throw new Error(`Field '${fieldName}' not found on SObject '${sObjectName}'.`);
  }

  return {
    apiName: field.apiName,
    label: field.label,
    values: field.picklistValues.map((v) => ({
      value: v,
      label: v,
      active: true,
      defaultValue: false
    }))
  };
}

/**
 * Introspects FlexiPages assigned or related to an SObject using Tooling API query.
 */
export async function introspectFlexipages(
  sObjectName: string,
  orgAlias?: string,
  options?: CliIntrospectOptions
): Promise<FlexipageDescribe[]> {
  const alias = orgAlias || options?.orgAlias;
  const soql = `SELECT Id, DeveloperName, MasterLabel, Type, EntityDefinitionId FROM FlexiPage WHERE EntityDefinitionId = '${sObjectName}'`;
  const args = ['data', 'query', '-t', '-q', soql, '--json'];
  if (alias) {
    args.push('-o', alias);
  }

  const result = await executeSfCommand(args, options);
  const records = Array.isArray(result?.records) ? result.records : [];

  return records.map((rec: any) => ({
    id: rec.Id,
    developerName: rec.DeveloperName,
    masterLabel: rec.MasterLabel,
    type: rec.Type,
    entityDefinitionId: rec.EntityDefinitionId
  }));
}

/**
 * Queries QuickActionDefinition via Tooling API for an SObject.
 */
async function queryQuickActions(
  sObjectName: string,
  orgAlias: string,
  options?: CliIntrospectOptions
): Promise<ActionDescribe[]> {
  const soql = `SELECT Id, DeveloperName, MasterLabel, ActionType, TargetSobjectType FROM QuickActionDefinition WHERE SobjectType = '${sObjectName}'`;
  const args = ['data', 'query', '-t', '-q', soql, '--json', '-o', orgAlias];

  const result = await executeSfCommand(args, options);
  const records = Array.isArray(result?.records) ? result.records : [];

  return records.map((rec: any) => ({
    name: rec.DeveloperName,
    label: rec.MasterLabel,
    type: 'QuickAction' as const,
    target: rec.TargetSobjectType || undefined
  }));
}

/**
 * Introspects multiple SObjects and compiles a full OrgMetadataCache snapshot.
 */
export async function introspectOrg(
  sObjectNames: string[],
  orgAlias: string,
  options?: CliIntrospectOptions & { release?: string }
): Promise<OrgMetadataCache> {
  const objects: Record<string, SObjectDescribe> = {};

  for (const sObjectName of sObjectNames) {
    const describe = await introspectSObject(sObjectName, orgAlias, options);
    objects[describe.apiName] = describe;
  }

  const cacheData = {
    orgAlias,
    release: options?.release || SALESFORCE_DEFAULT_RELEASE,
    updatedAt: new Date().toISOString(),
    apps: {},
    objects
  };

  return OrgMetadataCacheSchema.parse(cacheData);
}
