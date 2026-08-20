import { environmentValue } from "./runtimeEnvironment.js";

export function databaseUrl(environment = process.env) {
  return environmentValue("DATABASE_URL", { environment, required: false }) || [
    "postgres://",
    encodeURIComponent(environment.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(environment.POSTGRES_PASSWORD || "bullmq"),
    "@",
    environment.POSTGRES_HOST || "127.0.0.1",
    ":",
    environment.POSTGRES_PORT || "5432",
    "/",
    environment.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}
