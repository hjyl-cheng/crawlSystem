// Keep migration lifecycle controls available while unrelated writes are disabled.
export function controlledMigrationGuard(environment = process.env) {
  return (req, res, next) => {
    const controlled = String(environment.CONTROLLED_MIGRATION_ONLY || '').toLowerCase() === 'true';
    const readMethod = ['GET', 'HEAD', 'OPTIONS'].includes(req.method);
    const controlledWrite = req.method === 'POST' && (
      req.path.startsWith('/api/migration/channels')
      || /^\/api\/migration\/system-retries\/[^/]+\/retry$/.test(req.path)
      || /^\/api\/migration\/batches\/[^/]+\/(pause|resume|stop)\/?$/.test(req.path)
    );
    if (!controlled || readMethod || controlledWrite) return next();
    return res.status(423).json({
      ok: false,
      code: 'controlled_migration_only',
      error: 'non-Migration writes are disabled during the controlled canary',
    });
  };
}
