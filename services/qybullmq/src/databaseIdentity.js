function requiredName(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${name} is required`);
  return normalized;
}

function assertWriterIdentity(row, {
  kind,
  expectedDatabase,
  forbiddenDatabase,
}) {
  const expected = requiredName(expectedDatabase, `EXPECTED_${kind.toUpperCase()}_DATABASE`);
  const forbidden = requiredName(forbiddenDatabase, `FORBIDDEN_${kind.toUpperCase()}_DATABASE`);
  const database = String(row?.database_name ?? "").trim();
  const user = String(row?.database_user ?? "").trim();

  if (database === forbidden) {
    throw new Error(`refusing forbidden ${kind} database ${database}`);
  }
  if (database !== expected) {
    throw new Error(`refusing unexpected ${kind} database ${database || "unknown"}; expected ${expected}`);
  }
  if (row?.transaction_read_only !== "off") {
    throw new Error(`${kind} Writer database must be writable`);
  }
  if (
    row?.schema_ready !== true
    || row?.identity_kind !== kind.toLowerCase()
    || row?.identity_database !== database
  ) {
    throw new Error(`refusing uninitialized or mismatched ${kind} database ${database}`);
  }
  if (!user) throw new Error(`${kind} database user identity is unavailable`);
  return { database, user };
}

export function assertCrawlerWriterIdentity(row, config = {}) {
  return assertWriterIdentity(row, { kind: "Crawler", ...config });
}

export function assertBusinessWriterIdentity(row, config = {}) {
  return assertWriterIdentity(row, { kind: "Business", ...config });
}

export function assertMigrationSourceIdentity(row, {
  expectedDatabase,
  expectedDatabaseOid,
  expectedUser,
  targetDatabase,
} = {}) {
  const expected = requiredName(expectedDatabase, "EXPECTED_MIGRATION_DATABASE");
  const expectedOid = requiredName(expectedDatabaseOid, "EXPECTED_MIGRATION_DATABASE_OID");
  const expectedRole = requiredName(expectedUser, "EXPECTED_MIGRATION_DATABASE_USER");
  const target = requiredName(targetDatabase, "EXPECTED_CRAWLER_DATABASE");
  if (expected === target) throw new Error("Migration Source and Target databases must differ");

  const database = String(row?.database_name ?? "").trim();
  const databaseOid = String(row?.database_oid ?? "").trim();
  const user = String(row?.database_user ?? "").trim();
  if (database !== expected) {
    throw new Error(`refusing unexpected Migration Source database ${database || "unknown"}; expected ${expected}`);
  }
  if (databaseOid !== expectedOid) {
    throw new Error(`refusing unexpected Migration Source database OID ${databaseOid || "unknown"}; expected ${expectedOid}`);
  }
  if (user !== expectedRole) {
    throw new Error(`refusing unexpected Migration Source database user ${user || "unknown"}; expected ${expectedRole}`);
  }
  if (row?.transaction_read_only !== "on") {
    throw new Error("Migration Source must be used inside a read-only transaction");
  }
  if (row?.default_transaction_read_only !== "on") {
    throw new Error("Migration Source role must default to read-only");
  }
  if (row?.candidates_ready !== true || row?.channels_ready !== true) {
    throw new Error(`Migration Source schema is not ready in ${database}`);
  }
  if (row?.candidate_write !== false || row?.channel_write !== false) {
    throw new Error("Migration Source role must not have write privileges on source tables");
  }
  return { database, databaseOid, user };
}

const CRAWLER_WRITER_IDENTITY_SQL = `
  SELECT current_database() AS database_name,
         current_user AS database_user,
         current_setting('transaction_read_only') AS transaction_read_only,
         identity.database_kind AS identity_kind,
         identity.database_name AS identity_database,
         to_regclass('crawler.database_identity') IS NOT NULL AS schema_ready
  FROM crawler.database_identity AS identity
  WHERE identity.singleton=true
`;

const BUSINESS_WRITER_IDENTITY_SQL = `
  SELECT current_database() AS database_name,
         current_user AS database_user,
         current_setting('transaction_read_only') AS transaction_read_only,
         identity.database_kind AS identity_kind,
         identity.database_name AS identity_database,
         to_regclass('publication.database_identity') IS NOT NULL AS schema_ready
  FROM publication.database_identity AS identity
  WHERE identity.singleton=true
`;

const MIGRATION_SOURCE_IDENTITY_SQL = `
  SELECT current_database() AS database_name,
         database_state.oid::text AS database_oid,
         current_user AS database_user,
         current_setting('default_transaction_read_only')
           AS default_transaction_read_only,
         current_setting('transaction_read_only') AS transaction_read_only,
         to_regclass('crawler.channel_candidates') IS NOT NULL AS candidates_ready,
         to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
         (
           has_table_privilege(current_user,'crawler.channel_candidates','INSERT')
           OR has_any_column_privilege(current_user,'crawler.channel_candidates','INSERT')
           OR has_table_privilege(current_user,'crawler.channel_candidates','UPDATE')
           OR has_any_column_privilege(current_user,'crawler.channel_candidates','UPDATE')
           OR has_table_privilege(current_user,'crawler.channel_candidates','DELETE')
           OR has_table_privilege(current_user,'crawler.channel_candidates','TRUNCATE')
         ) AS candidate_write,
         (
           has_table_privilege(current_user,'crawler.channels','INSERT')
           OR has_any_column_privilege(current_user,'crawler.channels','INSERT')
           OR has_table_privilege(current_user,'crawler.channels','UPDATE')
           OR has_any_column_privilege(current_user,'crawler.channels','UPDATE')
           OR has_table_privilege(current_user,'crawler.channels','DELETE')
           OR has_table_privilege(current_user,'crawler.channels','TRUNCATE')
         ) AS channel_write
  FROM pg_database AS database_state
  WHERE database_state.datname=current_database()
`;

async function identityRow(query, statement, kind) {
  if (typeof query !== "function") throw new TypeError("database query function is required");
  const result = await query(statement);
  if (result?.rows?.length !== 1) {
    throw new Error(`${kind} database identity marker is missing or ambiguous`);
  }
  return result.rows[0];
}

export async function verifyCrawlerWriterDatabase(query, environment = process.env) {
  const row = await identityRow(query, CRAWLER_WRITER_IDENTITY_SQL, "Crawler");
  return assertCrawlerWriterIdentity(row, {
    expectedDatabase: environment.EXPECTED_CRAWLER_DATABASE,
    forbiddenDatabase: environment.FORBIDDEN_CRAWLER_DATABASE || "bullmq_crawler_migration",
  });
}

export async function verifyBusinessWriterDatabase(query, environment = process.env) {
  const row = await identityRow(query, BUSINESS_WRITER_IDENTITY_SQL, "Business");
  return assertBusinessWriterIdentity(row, {
    expectedDatabase: environment.EXPECTED_BUSINESS_DATABASE,
    forbiddenDatabase: environment.FORBIDDEN_BUSINESS_DATABASE || "yewu_business",
  });
}

export async function verifyMigrationSourceDatabase(query, environment = process.env) {
  const row = await identityRow(query, MIGRATION_SOURCE_IDENTITY_SQL, "Migration Source");
  return assertMigrationSourceIdentity(row, {
    expectedDatabase: environment.EXPECTED_MIGRATION_DATABASE,
    expectedDatabaseOid: environment.EXPECTED_MIGRATION_DATABASE_OID,
    expectedUser: environment.EXPECTED_MIGRATION_DATABASE_USER,
    targetDatabase: environment.EXPECTED_CRAWLER_DATABASE,
  });
}
