#!/usr/bin/env node

import { chmod, open, readFile, rename, stat, unlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";

function required(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function safePort(value) {
  const output = required(value, "port");
  const parsed = Number(output);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new TypeError("port must be an integer between 1 and 65535");
  }
  return String(parsed);
}

export function rewriteDatabaseUrl(value, {
  host,
  port,
  database,
  expectedUser,
} = {}) {
  const url = new URL(required(value, "database URL"));
  if (!new Set(["postgres:", "postgresql:"]).has(url.protocol)) {
    throw new TypeError("database URL must use postgres or postgresql");
  }
  const user = decodeURIComponent(url.username);
  if (user !== required(expectedUser, "expected user")) {
    throw new Error(`database URL user must be ${expectedUser}`);
  }
  url.hostname = required(host, "host");
  url.port = safePort(port);
  url.pathname = `/${encodeURIComponent(required(database, "database"))}`;
  return url.toString();
}

export async function rewriteDatabaseUrlFile(file, options) {
  const target = required(file, "file");
  const metadata = await stat(target);
  const current = (await readFile(target, "utf8")).trim();
  const rewritten = `${rewriteDatabaseUrl(current, options)}\n`;
  const temporary = `${target}.tmp.${process.pid}`;
  let handle;
  try {
    handle = await open(temporary, "wx", metadata.mode & 0o777);
    await handle.writeFile(rewritten, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await chmod(temporary, metadata.mode & 0o777);
    await rename(temporary, target);
  } finally {
    await handle?.close().catch(() => {});
    await unlink(temporary).catch(() => {});
  }
}

function command(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 2) {
    const option = argv[index];
    const value = argv[index + 1];
    if (!option?.startsWith("--") || value == null) {
      throw new TypeError("options must be provided as --name value pairs");
    }
    values[option.slice(2)] = value;
  }
  return {
    file: values.file,
    options: {
      host: values.host,
      port: values.port,
      database: values.database,
      expectedUser: values["expected-user"],
    },
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const input = command(process.argv.slice(2));
  rewriteDatabaseUrlFile(input.file, input.options).catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
