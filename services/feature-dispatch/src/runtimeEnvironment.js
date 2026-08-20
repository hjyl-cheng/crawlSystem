import { readFileSync } from "node:fs";

export function environmentValue(name, {
  environment = process.env,
  required = true,
} = {}) {
  let value = String(environment[name] || "").trim();
  const filePath = String(environment[`${name}_FILE`] || "").trim();
  if (value && filePath) throw new Error(`${name} and ${name}_FILE cannot both be set`);
  if (filePath) {
    try {
      value = readFileSync(filePath, "utf8").trim();
    } catch (error) {
      throw new Error(`cannot read ${name}_FILE`, { cause: error });
    }
  }
  if (required && !value) throw new Error(`${name} or ${name}_FILE is required`);
  return value || null;
}
