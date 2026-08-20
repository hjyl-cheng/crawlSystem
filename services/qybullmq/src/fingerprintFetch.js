import {
  assertChannelExecutionIdentity,
  currentChannelExecution,
  currentClientProfile,
} from "./channelExecutionContext.js";

export function fingerprintTransportRequired(env = process.env) {
  return ["1", "true", "yes", "on"].includes(
    String(env.FINGERPRINT_TRANSPORT_REQUIRED || "false").trim().toLowerCase(),
  );
}

export async function fetchWithFingerprint(engine, input, init, fallback) {
  const context = currentChannelExecution();
  if (!context?.fingerprint_gateway) {
    if (fingerprintTransportRequired() && context) {
      throw new Error("fingerprint transport is required for channel execution");
    }
    return fallback();
  }
  const profile = currentClientProfile(engine);
  if (!profile) throw new Error(`missing fingerprint profile for ${engine}`);
  assertChannelExecutionIdentity();
  const response = await context.fingerprint_gateway.fetch(profile, input, init);
  assertChannelExecutionIdentity();
  return response;
}
