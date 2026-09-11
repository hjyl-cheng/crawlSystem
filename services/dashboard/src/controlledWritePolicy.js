export function allowDashboardRequestDuringControlledMigration(method, path) {
  const normalizedMethod = String(method || "").toUpperCase();
  if (normalizedMethod === "GET" || normalizedMethod === "HEAD" || normalizedMethod === "OPTIONS") {
    return true;
  }
  if (String(path || "").startsWith("/migration-channels")) {
    return true;
  }
  // Registration and explicit bootstrap stay separate from crawler deployment.
  if (normalizedMethod === "POST" && path === "/api/server-nodes") return true;
  // Bootstrap affects only a manually registered execution node, never queues.
  if (normalizedMethod === "POST" && /^\/api\/server-nodes\/[0-9a-f-]{36}\/initialize$/.test(String(path))) return true;
  if (normalizedMethod === "POST" && /^\/api\/server-nodes\/[0-9a-f-]{36}\/prepare-runtime$/.test(String(path))) return true;
  // Explicit deployment starts only waiting nodes; it cannot activate collection.
  if (normalizedMethod === "POST" && /^\/api\/server-nodes\/[0-9a-f-]{36}\/deploy-workers$/.test(String(path))) return true;
  if (["PUT", "DELETE"].includes(normalizedMethod) && /^\/api\/server-nodes\/[0-9a-f-]{36}$/.test(String(path))) return true;
  return normalizedMethod === "POST" && path === "/youtube-api";
}
