export function allowDashboardRequestDuringControlledMigration(method, path) {
  const normalizedMethod = String(method || "").toUpperCase();
  if (normalizedMethod === "GET" || normalizedMethod === "HEAD" || normalizedMethod === "OPTIONS") {
    return true;
  }
  if (String(path || "").startsWith("/migration-channels")) {
    return true;
  }
  return normalizedMethod === "POST" && path === "/youtube-api";
}
