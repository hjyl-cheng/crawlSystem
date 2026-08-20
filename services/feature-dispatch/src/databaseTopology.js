export function assertSharedFeatureDatabase(row, {
  expectedDatabase,
  expectedUser,
} = {}) {
  if (!expectedDatabase) throw new Error("EXPECTED_FEATURE_DATABASE is required");
  if (!expectedUser) throw new Error("EXPECTED_FEATURE_DATABASE_USER is required");
  if (row?.database_name !== expectedDatabase
      || row?.database_user !== expectedUser
      || row?.outbox_ready !== true
      || row?.crawler_ready !== true
      || row?.channels_ready !== true
      || row?.crawler_channels_readable !== false
      || row?.timezone_utc !== true) {
    throw new Error(
      `refusing to publish from unexpected or unmigrated shared Crawler/Feature database: ${row?.database_name}`,
    );
  }
}
