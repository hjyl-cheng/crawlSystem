export function allowDashboardRequestDuringControlledMigration(method, path) {
  const normalizedMethod = String(method || "").toUpperCase();
  if (normalizedMethod === "GET" || normalizedMethod === "HEAD" || normalizedMethod === "OPTIONS") {
    return true;
  }
  if (String(path || "").startsWith("/migration-channels")) {
    return true;
  }
  // These exact endpoints save node metadata only; deployment stays separate.
  if (normalizedMethod === "POST" && path === "/api/server-nodes") return true;
  if (["PUT", "DELETE"].includes(normalizedMethod) && /^\/api\/server-nodes\/[0-9a-f-]{36}$/.test(String(path))) return true;
  return normalizedMethod === "POST" && path === "/youtube-api";
}
