const DYNAMIC_SLOT_ROLES = new Set(["channel", "discover", "query_quality"]);

function requiredSecret(value, name) {
  const normalized = String(value || "");
  if (normalized.length < 12) {
    throw new Error(`${name} must contain at least 12 characters`);
  }
  return normalized;
}

export function dynamicRotaProxyConfig({
  slotRole,
  controlUrl,
  controlToken,
  proxyPassword,
} = {}) {
  const role = String(slotRole || "").trim().toLowerCase();
  if (!role) return null;
  if (!DYNAMIC_SLOT_ROLES.has(role)) {
    throw new Error(`unsupported PROXY_SLOT_ROLE: ${role}`);
  }

  const normalizedControlUrl = String(controlUrl || "").trim().replace(/\/+$/, "");
  if (!normalizedControlUrl) {
    throw new Error("ROTA_PROXY_CONTROL_URL is required for a dynamic Rota Slot");
  }
  const parsed = new URL(normalizedControlUrl);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error(`unsupported ROTA_PROXY_CONTROL_URL protocol: ${parsed.protocol}`);
  }
  requiredSecret(controlToken, "ROTA_PROXY_CONTROL_TOKEN");
  requiredSecret(proxyPassword, "ROTA_BULLMQ_PROXY_PASSWORD");

  return Object.freeze({
    role,
    controlUrl: normalizedControlUrl,
  });
}

export function fixedRotaProxyConfig({
  proxyUser,
  proxyPassword,
  baseUrl = "http://youtube-rota-qy-core:8000",
  slotRole = "",
  role = "channel",
} = {}) {
  const user = String(proxyUser || "").trim();
  if (!user) return null;
  if (String(slotRole || "").trim()) {
    throw new Error("ROTA_FIXED_PROXY_USER and PROXY_SLOT_ROLE cannot be used together");
  }
  if (!String(proxyPassword || "")) {
    throw new Error("ROTA_BULLMQ_PROXY_PASSWORD is required for a fixed Rota proxy user");
  }

  const url = new URL(String(baseUrl || "http://youtube-rota-qy-core:8000"));
  if (!["http:", "https:"].includes(url.protocol)) {
    throw new Error(`unsupported Rota proxy protocol: ${url.protocol}`);
  }
  url.username = user;
  url.password = String(proxyPassword);

  return {
    proxyUrl: url.toString(),
    identity: {
      role: String(role || "channel"),
      slot_name: user,
      proxy_id: null,
    },
  };
}
