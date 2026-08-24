import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { environmentValue } from "./runtimeEnvironment.js";

const SAFE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const SOURCE_ROLE_LOCK = 781137242;
const BUSINESS_ROLE_LOCK = 781137243;
const TABLE_PRIVILEGES = Object.freeze([
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "TRUNCATE",
  "REFERENCES",
  "TRIGGER",
]);

const ROLE_SPECIFICATIONS = Object.freeze({
  source: Object.freeze([
    Object.freeze({
      role: "publication_publisher",
      connectionLimit: 8,
      controlledSchemas: Object.freeze(["crawler", "publication"]),
      schemas: Object.freeze(["crawler", "publication"]),
      tables: Object.freeze({
        "crawler.database_identity": Object.freeze(["SELECT"]),
        "publication.outbox": Object.freeze(["SELECT", "UPDATE"]),
        "publication.revision": Object.freeze(["SELECT"]),
      }),
      denied: Object.freeze({
        "crawler.channels": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        "publication.outbox": Object.freeze(["INSERT", "DELETE"]),
        "publication.revision": Object.freeze(["INSERT", "UPDATE", "DELETE"]),
      }),
    }),
  ]),
  business: Object.freeze([
    Object.freeze({
      role: "business_publication_ingress",
      connectionLimit: 16,
      controlledSchemas: Object.freeze(["public", "publication", "result"]),
      schemas: Object.freeze(["publication"]),
      tables: Object.freeze({
        "publication.channel_ownership": Object.freeze(["SELECT", "INSERT"]),
        "publication.database_identity": Object.freeze(["SELECT"]),
        "publication.inbox": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.inbox_conflict": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.quarantine": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.revision": Object.freeze(["SELECT", "INSERT"]),
        // PostgreSQL requires UPDATE privilege for SELECT ... FOR SHARE.
        "publication.stream": Object.freeze(["SELECT", "UPDATE"]),
      }),
      denied: Object.freeze({
        "public.channels": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        "publication.channel_ownership": Object.freeze(["UPDATE", "DELETE"]),
        "publication.revision": Object.freeze(["UPDATE", "DELETE"]),
        "result.entity_current": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
      }),
    }),
    Object.freeze({
      role: "business_publication_reconciler",
      connectionLimit: 24,
      controlledSchemas: Object.freeze(["public", "publication", "result"]),
      schemas: Object.freeze(["publication", "result"]),
      tables: Object.freeze({
        "publication.activation": Object.freeze(["INSERT"]),
        "publication.activation_item": Object.freeze(["SELECT", "INSERT"]),
        // PostgreSQL requires UPDATE privilege for SELECT ... FOR UPDATE.
        "publication.channel_ownership": Object.freeze(["SELECT", "UPDATE"]),
        "publication.consumer_cursor": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.database_identity": Object.freeze(["SELECT"]),
        "publication.inbox": Object.freeze(["SELECT"]),
        "publication.projection_outbox": Object.freeze(["SELECT", "INSERT"]),
        "publication.quarantine": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.reconciliation_state": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.revision": Object.freeze(["SELECT", "UPDATE"]),
        "result.agent_current": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "result.content_current": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "result.entity_current": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "result.video_current": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
      }),
      denied: Object.freeze({
        "public.channels": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        "publication.inbox": Object.freeze(["INSERT", "UPDATE", "DELETE"]),
        "publication.stream": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
      }),
    }),
    Object.freeze({
      role: "business_publication_projector",
      connectionLimit: 8,
      controlledSchemas: Object.freeze(["public", "publication", "result"]),
      schemas: Object.freeze(["public", "publication", "result"]),
      tables: Object.freeze({
        "public.category_taxonomy": Object.freeze(["SELECT"]),
        "public.channel_links": Object.freeze(["SELECT", "INSERT"]),
        "public.channel_metric_values": Object.freeze(["SELECT", "INSERT"]),
        "public.channel_profile_facts": Object.freeze(["SELECT", "INSERT"]),
        "public.channel_snapshots": Object.freeze(["SELECT", "INSERT"]),
        "public.channels": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "public.content_items": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "public.content_snapshots": Object.freeze(["SELECT", "INSERT"]),
        "public.content_type_taxonomy": Object.freeze(["SELECT"]),
        "public.creator_search_active": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        "public.creator_search_current": Object.freeze(["SELECT", "INSERT", "UPDATE", "DELETE"]),
        "public.creator_search_live": Object.freeze(["SELECT", "INSERT", "DELETE"]),
        "public.creator_search_releases": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "public.import_batches": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.activation_item": Object.freeze(["SELECT"]),
        "publication.channel_ownership": Object.freeze(["SELECT", "UPDATE"]),
        "publication.consumer_cursor": Object.freeze(["SELECT"]),
        "publication.creator_search_changes": Object.freeze(["SELECT", "INSERT"]),
        "publication.creator_search_storage_state": Object.freeze(["SELECT", "UPDATE"]),
        "publication.database_identity": Object.freeze(["SELECT"]),
        "publication.projection_batch": Object.freeze(["SELECT", "INSERT", "UPDATE"]),
        "publication.projection_batch_item": Object.freeze(["SELECT", "INSERT"]),
        "publication.projection_cutover": Object.freeze(["SELECT", "UPDATE"]),
        "publication.projection_outbox": Object.freeze(["SELECT", "UPDATE"]),
        "publication.revision": Object.freeze(["SELECT"]),
        "result.agent_current": Object.freeze(["SELECT"]),
        "result.content_current": Object.freeze(["SELECT"]),
        "result.entity_current": Object.freeze(["SELECT"]),
        "result.video_current": Object.freeze(["SELECT"]),
      }),
      functions: Object.freeze([
        "public.refresh_creator_search_release_v9(text,text[],text[])",
        "public.replay_creator_search_release_v9(text)",
        "public.restore_creator_search_live_from_legacy_v1(text)",
      ]),
      denied: Object.freeze({
        "publication.inbox": Object.freeze(["INSERT", "UPDATE", "DELETE"]),
        "publication.revision": Object.freeze(["INSERT", "UPDATE", "DELETE"]),
        "result.entity_current": Object.freeze(["INSERT", "UPDATE", "DELETE"]),
      }),
    }),
  ]),
});

function requiredString(environment, name) {
  const value = String(environment[name] ?? "").trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

function safeKey(environment, name) {
  const value = requiredString(environment, name);
  if (!SAFE_KEY.test(value)) {
    throw new TypeError(`${name} must use only letters, numbers, dot, underscore, or hyphen`);
  }
  return value;
}

function expectedCount(environment, name) {
  const raw = requiredString(environment, name);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return value;
}

function parseRuntimeCredential(raw, { name, expectedRole, expectedDatabase }) {
  let url;
  try {
    url = new URL(raw);
  } catch (error) {
    throw new TypeError(`${name} must be a valid PostgreSQL URL`, { cause: error });
  }
  if (!new Set(["postgres:", "postgresql:"]).has(url.protocol)) {
    throw new TypeError(`${name} must use postgres:// or postgresql://`);
  }
  const role = decodeURIComponent(url.username);
  const password = decodeURIComponent(url.password);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  if (role !== expectedRole) throw new TypeError(`${name} must use role ${expectedRole}`);
  if (database !== expectedDatabase) {
    throw new TypeError(`${name} must target database ${expectedDatabase}`);
  }
  if (!url.hostname || !password) throw new TypeError(`${name} must include a host and password`);
  return { role, password, database };
}

export function publicationRuntimeRoleConfig(environment = process.env) {
  const expectedCrawlerDatabase = safeKey(environment, "EXPECTED_CRAWLER_DATABASE");
  const expectedBusinessDatabase = safeKey(environment, "EXPECTED_BUSINESS_DATABASE");
  const forbiddenCrawlerDatabase = safeKey({
    FORBIDDEN_CRAWLER_DATABASE:
      environment.FORBIDDEN_CRAWLER_DATABASE || "bullmq_crawler_migration",
  }, "FORBIDDEN_CRAWLER_DATABASE");
  const forbiddenBusinessDatabase = safeKey({
    FORBIDDEN_BUSINESS_DATABASE:
      environment.FORBIDDEN_BUSINESS_DATABASE || "yewu_business",
  }, "FORBIDDEN_BUSINESS_DATABASE");
  if (expectedCrawlerDatabase === forbiddenCrawlerDatabase) {
    throw new TypeError(`refusing forbidden Crawler database ${expectedCrawlerDatabase}`);
  }
  if (expectedBusinessDatabase === forbiddenBusinessDatabase) {
    throw new TypeError(`refusing forbidden Business database ${expectedBusinessDatabase}`);
  }
  if (expectedCrawlerDatabase === expectedBusinessDatabase) {
    throw new TypeError("Crawler and Business database names must be different");
  }
  return {
    crawlerAdminDatabaseUrl: environmentValue("CRAWLER_ADMIN_DATABASE_URL", { environment }),
    businessAdminDatabaseUrl: environmentValue("BUSINESS_ADMIN_DATABASE_URL", { environment }),
    expectedCrawlerDatabase,
    expectedBusinessDatabase,
    expectedCrawlerChannelCount: expectedCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedBusinessChannelCount: expectedCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    credentials: {
      publication_publisher: parseRuntimeCredential(
        environmentValue("PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL", { environment }),
        {
          name: "PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL",
          expectedRole: "publication_publisher",
          expectedDatabase: expectedCrawlerDatabase,
        },
      ),
      business_publication_ingress: parseRuntimeCredential(
        environmentValue("PUBLICATION_BUSINESS_INGRESS_DATABASE_URL", { environment }),
        {
          name: "PUBLICATION_BUSINESS_INGRESS_DATABASE_URL",
          expectedRole: "business_publication_ingress",
          expectedDatabase: expectedBusinessDatabase,
        },
      ),
      business_publication_reconciler: parseRuntimeCredential(
        environmentValue("PUBLICATION_BUSINESS_RECONCILER_DATABASE_URL", { environment }),
        {
          name: "PUBLICATION_BUSINESS_RECONCILER_DATABASE_URL",
          expectedRole: "business_publication_reconciler",
          expectedDatabase: expectedBusinessDatabase,
        },
      ),
      business_publication_projector: parseRuntimeCredential(
        environmentValue("PUBLICATION_BUSINESS_PROJECTOR_DATABASE_URL", { environment }),
        {
          name: "PUBLICATION_BUSINESS_PROJECTOR_DATABASE_URL",
          expectedRole: "business_publication_projector",
          expectedDatabase: expectedBusinessDatabase,
        },
      ),
    },
  };
}

function canonicalSpecifications() {
  return JSON.stringify(ROLE_SPECIFICATIONS);
}

export const PUBLICATION_RUNTIME_ROLE_SPEC_HASH = `sha256:${createHash("sha256")
  .update(canonicalSpecifications())
  .digest("hex")}`;

export function publicationRuntimeRoleSpecifications() {
  return ROLE_SPECIFICATIONS;
}

export function publicationRuntimeRoleConfirmation(config) {
  return [
    "PROVISION_PUBLICATION_RUNTIME_ROLES",
    config.expectedCrawlerDatabase,
    config.expectedBusinessDatabase,
    String(config.expectedCrawlerChannelCount),
    String(config.expectedBusinessChannelCount),
    PUBLICATION_RUNTIME_ROLE_SPEC_HASH,
  ].join(":");
}

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function tableParts(table) {
  const parts = table.split(".");
  if (parts.length !== 2 || parts.some((part) => !SAFE_KEY.test(part))) {
    throw new TypeError(`invalid role specification table: ${table}`);
  }
  return parts;
}

function qualifiedTable(table) {
  return tableParts(table).map(quoteIdentifier).join(".");
}

function qualifiedFunction(signature) {
  if (!/^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\([a-z0-9_,\[\] ]*\)$/.test(signature)) {
    throw new TypeError(`invalid role specification function: ${signature}`);
  }
  return signature;
}

function expectedPrivilegeRows(specification) {
  return Object.entries(specification.tables)
    .flatMap(([table, privileges]) => privileges.map((privilege) => `${table}:${privilege}`))
    .sort();
}

async function databasePreflight(client, {
  side,
  expectedDatabase,
  expectedChannelCount,
}) {
  const channelTable = side === "source" ? "crawler.channels" : "public.channels";
  const schemaChecks = side === "source"
    ? `to_regclass('publication.revision') IS NOT NULL AS revision_ready,
       to_regclass('publication.outbox') IS NOT NULL AS outbox_ready`
    : `to_regclass('publication.inbox') IS NOT NULL AS inbox_ready,
       to_regclass('publication.reconciliation_state') IS NOT NULL AS reconciliation_ready,
       to_regclass('result.entity_current') IS NOT NULL AS current_ready`;
  const result = await client.query(
    `SELECT current_database() AS database_name,
            to_regclass('${channelTable}') IS NOT NULL AS channels_ready,
            ${schemaChecks},
            (SELECT count(*)::int FROM ${qualifiedTable(channelTable)}) AS channel_count`,
  );
  const state = result.rows[0] ?? {};
  if (state.database_name !== expectedDatabase) {
    throw new Error(`${side} database mismatch: ${state.database_name}`);
  }
  if (Number(state.channel_count) !== expectedChannelCount) {
    throw new Error(`${side} Channel count mismatch: ${state.channel_count}`);
  }
  const missing = Object.entries(state)
    .filter(([name, ready]) => name.endsWith("_ready") && ready !== true)
    .map(([name]) => name);
  if (missing.length > 0) throw new Error(`${side} Publication schema preflight failed: ${missing.join(",")}`);
  return {
    database: state.database_name,
    channel_count: Number(state.channel_count),
  };
}

async function inspectRole(client, specification) {
  const roleResult = await client.query(
    `SELECT rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,
            rolinherit,rolbypassrls,rolconnlimit
     FROM pg_roles WHERE rolname=$1`,
    [specification.role],
  );
  const attributes = roleResult.rows[0] ?? null;
  if (!attributes) {
    return {
      role: specification.role,
      exists: false,
      ready: false,
      memberships: [],
      schema_usage: [],
      table_privileges: [],
      sequence_privileges: [],
      function_privileges: [],
      denied_privileges: [],
    };
  }
  const memberships = (await client.query(
    `SELECT parent.rolname
     FROM pg_auth_members AS membership
     JOIN pg_roles AS parent ON parent.oid=membership.roleid
     JOIN pg_roles AS member ON member.oid=membership.member
     WHERE member.rolname=$1 ORDER BY parent.rolname`,
    [specification.role],
  )).rows.map((row) => row.rolname);
  const schemaUsage = [];
  for (const schema of specification.schemas) {
    const allowed = (await client.query(
      "SELECT has_schema_privilege($1,$2,'USAGE') AS allowed",
      [specification.role, schema],
    )).rows[0]?.allowed === true;
    if (allowed) schemaUsage.push(schema);
  }
  for (const privileges of Object.values(specification.tables)) {
    for (const privilege of privileges) {
      if (!TABLE_PRIVILEGES.includes(privilege)) {
        throw new TypeError(`unsupported table privilege in role specification: ${privilege}`);
      }
    }
  }
  const actualPrivileges = (await client.query(
    `SELECT namespace.nspname AS table_schema,relation.relname AS table_name,
            privilege.name AS privilege
     FROM pg_class AS relation
     JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
     CROSS JOIN unnest($3::text[]) AS privilege(name)
     WHERE namespace.nspname=ANY($2::text[])
       AND relation.relkind IN ('r','p','v','m','f')
       AND has_table_privilege($1,relation.oid,privilege.name)
     ORDER BY namespace.nspname,relation.relname,privilege.name`,
    [specification.role, specification.controlledSchemas, TABLE_PRIVILEGES],
  )).rows.map((row) => `${row.table_schema}.${row.table_name}:${row.privilege}`);
  const sequencePrivileges = (await client.query(
    `SELECT namespace.nspname AS sequence_schema,sequence.relname AS sequence_name,
            privilege.name AS privilege
     FROM pg_class AS sequence
     JOIN pg_namespace AS namespace ON namespace.oid=sequence.relnamespace
     CROSS JOIN unnest(ARRAY['USAGE','SELECT','UPDATE']::text[]) AS privilege(name)
     WHERE namespace.nspname=ANY($2::text[])
       AND sequence.relkind='S'
       AND has_sequence_privilege($1,sequence.oid,privilege.name)
     ORDER BY namespace.nspname,sequence.relname,privilege.name`,
    [specification.role, specification.controlledSchemas],
  )).rows.map((row) => `${row.sequence_schema}.${row.sequence_name}:${row.privilege}`);
  const functionPrivileges = [];
  for (const signature of specification.functions ?? []) {
    const allowed = (await client.query(
      "SELECT has_function_privilege($1,$2,'EXECUTE') AS allowed",
      [specification.role, qualifiedFunction(signature)],
    )).rows[0]?.allowed === true;
    if (allowed) functionPrivileges.push(signature);
  }
  const deniedPrivileges = [];
  for (const [table, privileges] of Object.entries(specification.denied)) {
    for (const privilege of privileges) {
      const allowed = (await client.query(
        "SELECT has_table_privilege($1,$2,$3) AS allowed",
        [specification.role, table, privilege],
      )).rows[0]?.allowed === true;
      if (allowed) deniedPrivileges.push(`${table}:${privilege}`);
    }
  }
  const expectedAttributes = {
    rolcanlogin: true,
    rolsuper: false,
    rolcreatedb: false,
    rolcreaterole: false,
    rolreplication: false,
    rolinherit: false,
    rolbypassrls: false,
    rolconnlimit: specification.connectionLimit,
  };
  const actualAttributes = Object.fromEntries(
    Object.keys(expectedAttributes).map((name) => [name, attributes[name]]),
  );
  const expectedPrivileges = expectedPrivilegeRows(specification);
  actualPrivileges.sort();
  return {
    role: specification.role,
    exists: true,
    ready: isDeepStrictEqual(actualAttributes, expectedAttributes)
      && memberships.length === 0
      && isDeepStrictEqual(schemaUsage.sort(), [...specification.schemas].sort())
      && isDeepStrictEqual(actualPrivileges, expectedPrivileges)
      && sequencePrivileges.length === 0
      && isDeepStrictEqual(functionPrivileges, [...(specification.functions ?? [])])
      && deniedPrivileges.length === 0,
    attributes: actualAttributes,
    memberships,
    schema_usage: schemaUsage,
    table_privileges: actualPrivileges,
    sequence_privileges: sequencePrivileges,
    function_privileges: functionPrivileges,
    denied_privileges: deniedPrivileges.sort(),
  };
}

async function inspectSide(client, config, side) {
  const source = side === "source";
  const database = await databasePreflight(client, {
    side,
    expectedDatabase: source ? config.expectedCrawlerDatabase : config.expectedBusinessDatabase,
    expectedChannelCount: source
      ? config.expectedCrawlerChannelCount
      : config.expectedBusinessChannelCount,
  });
  const roles = [];
  for (const specification of ROLE_SPECIFICATIONS[side]) {
    roles.push(await inspectRole(client, specification));
  }
  return { ...database, roles, ready: roles.every((role) => role.ready) };
}

async function removeMemberships(client, role) {
  const target = quoteIdentifier(role);
  await client.query(
    `DO $memberships$
     DECLARE granted_role name;
     BEGIN
       FOR granted_role IN
         SELECT parent.rolname
         FROM pg_auth_members AS membership
         JOIN pg_roles AS parent ON parent.oid=membership.roleid
         JOIN pg_roles AS member ON member.oid=membership.member
         WHERE member.rolname='${role}'
       LOOP
         EXECUTE format('REVOKE %I FROM ${target}',granted_role);
       END LOOP;
     END
     $memberships$`,
  );
}

async function provisionRole(client, specification, password) {
  const role = quoteIdentifier(specification.role);
  await client.query(
    `DO $create_role$
     BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='${specification.role}') THEN
         EXECUTE 'CREATE ROLE ${role}';
       END IF;
     END
     $create_role$`,
  );
  await client.query(
    `ALTER ROLE ${role} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
     NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT ${specification.connectionLimit}`,
  );
  await removeMemberships(client, specification.role);
  await client.query(
    "SELECT set_config('publication.runtime_role_password',$1,true)",
    [password],
  );
  await client.query(
    `DO $role_password$
     BEGIN
       EXECUTE format(
         'ALTER ROLE ${role} PASSWORD %L',
         current_setting('publication.runtime_role_password')
       );
     END
     $role_password$`,
  );
  await client.query(
    `DO $database_grant$
     BEGIN
       EXECUTE format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM ${role}',current_database());
       EXECUTE format('GRANT CONNECT ON DATABASE %I TO ${role}',current_database());
     END
     $database_grant$`,
  );
  for (const schema of specification.controlledSchemas) {
    const identifier = quoteIdentifier(schema);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${identifier} FROM ${role}`);
    await client.query(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${identifier} FROM ${role}`);
    await client.query(`REVOKE ALL PRIVILEGES ON SCHEMA ${identifier} FROM ${role}`);
  }
  for (const schema of specification.schemas) {
    const identifier = quoteIdentifier(schema);
    await client.query(`GRANT USAGE ON SCHEMA ${identifier} TO ${role}`);
  }
  for (const [table, privileges] of Object.entries(specification.tables)) {
    await client.query(
      `GRANT ${privileges.join(",")} ON TABLE ${qualifiedTable(table)} TO ${role}`,
    );
  }
  for (const signature of specification.functions ?? []) {
    const target = qualifiedFunction(signature);
    await client.query(`REVOKE ALL PRIVILEGES ON FUNCTION ${target} FROM ${role}`);
    await client.query(`GRANT EXECUTE ON FUNCTION ${target} TO ${role}`);
  }
}

async function transaction(client, lockId, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
    begun = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock($1)", [lockId]);
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function readOnlyTransaction(client, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    begun = true;
    await client.query("SET LOCAL statement_timeout = '120s'");
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

export class PublicationRuntimeRoleAdministrator {
  constructor({ crawlerClient, businessClient, config }) {
    if (!crawlerClient?.query || !businessClient?.query) {
      throw new TypeError("Crawler and Business PostgreSQL clients are required");
    }
    this.crawler = crawlerClient;
    this.business = businessClient;
    this.config = config;
  }

  async inspectReadOnly() {
    const [source, business] = await Promise.all([
      readOnlyTransaction(this.crawler, () => inspectSide(this.crawler, this.config, "source")),
      readOnlyTransaction(this.business, () => inspectSide(this.business, this.config, "business")),
    ]);
    return { source, business };
  }

  async apply() {
    const source = await transaction(this.crawler, SOURCE_ROLE_LOCK, async () => {
      await databasePreflight(this.crawler, {
        side: "source",
        expectedDatabase: this.config.expectedCrawlerDatabase,
        expectedChannelCount: this.config.expectedCrawlerChannelCount,
      });
      for (const specification of ROLE_SPECIFICATIONS.source) {
        await provisionRole(
          this.crawler,
          specification,
          this.config.credentials[specification.role].password,
        );
      }
      const state = await inspectSide(this.crawler, this.config, "source");
      if (!state.ready) throw new Error("Crawler Runtime role verification failed");
      return state;
    });
    const business = await transaction(this.business, BUSINESS_ROLE_LOCK, async () => {
      await databasePreflight(this.business, {
        side: "business",
        expectedDatabase: this.config.expectedBusinessDatabase,
        expectedChannelCount: this.config.expectedBusinessChannelCount,
      });
      for (const specification of ROLE_SPECIFICATIONS.business) {
        await provisionRole(
          this.business,
          specification,
          this.config.credentials[specification.role].password,
        );
      }
      const state = await inspectSide(this.business, this.config, "business");
      if (!state.ready) throw new Error("Business Runtime role verification failed");
      return state;
    });
    return { source, business };
  }
}

export function publicationRuntimeRoleSummary(state) {
  return {
    role_spec_hash: PUBLICATION_RUNTIME_ROLE_SPEC_HASH,
    crawler: state.source,
    business: state.business,
    ready: state.source.ready && state.business.ready,
  };
}
