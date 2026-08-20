import { UnrecoverableError } from "bullmq";

export const PARSER_CONTRACT_ERROR = "PARSER_CONTRACT_ERROR";

function text(value, maxLength = 1000) {
  const output = String(value ?? "");
  return output.length <= maxLength ? output : `${output.slice(0, maxLength)}...`;
}

export class ParserContractError extends UnrecoverableError {
  constructor({
    field,
    value,
    locale = null,
    source = null,
    reason = "unsupported_localized_format",
    context = null,
  } = {}) {
    const details = {
      code: PARSER_CONTRACT_ERROR,
      field: text(field || "unknown", 120),
      locale: locale ? text(locale, 80) : null,
      source: source ? text(source, 160) : null,
      reason: text(reason, 160),
      raw_value: text(value),
      context: context && typeof context === "object" ? context : null,
    };
    super(`[${PARSER_CONTRACT_ERROR}] ${JSON.stringify(details)}`);
    this.name = "ParserContractError";
    this.code = PARSER_CONTRACT_ERROR;
    this.field = details.field;
    this.locale = details.locale;
    this.source = details.source;
    this.reason = details.reason;
    this.rawValue = details.raw_value;
    this.context = details.context;
  }

  toJSON() {
    return {
      code: this.code,
      field: this.field,
      locale: this.locale,
      source: this.source,
      reason: this.reason,
      raw_value: this.rawValue,
      context: this.context,
    };
  }
}

export function isParserContractError(error) {
  return error?.code === PARSER_CONTRACT_ERROR
    || error?.name === "ParserContractError"
    || String(error?.message ?? error ?? "").includes(`[${PARSER_CONTRACT_ERROR}]`);
}

export function parserContractDetails(error) {
  if (!isParserContractError(error)) return null;
  if (typeof error?.toJSON === "function") return error.toJSON();
  const message = String(error?.message ?? error ?? "");
  const marker = `[${PARSER_CONTRACT_ERROR}] `;
  const index = message.indexOf(marker);
  if (index >= 0) {
    try {
      return JSON.parse(message.slice(index + marker.length));
    } catch {
      // Fall through to the stable fields below.
    }
  }
  return {
    code: PARSER_CONTRACT_ERROR,
    field: error?.field ?? null,
    locale: error?.locale ?? null,
    source: error?.source ?? null,
    reason: error?.reason ?? null,
    raw_value: error?.rawValue ?? null,
    context: error?.context ?? null,
  };
}
