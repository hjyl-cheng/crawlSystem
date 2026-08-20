function folded(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function decision(kind, accessStatus, availability, reasonCode, retryMode = "none") {
  return Object.freeze({
    kind,
    access_status: accessStatus,
    availability,
    reason_code: reasonCode,
    retry_mode: retryMode,
  });
}

export class YoutubeCollectionFailureError extends Error {
  constructor(message, {
    reasonCode = "collection_failure",
    retryMode = "new_identity",
    source = null,
    videoId = null,
    reason = null,
  } = {}) {
    super(message);
    this.name = "YoutubeCollectionFailureError";
    this.reason_code = reasonCode;
    this.retry_mode = retryMode;
    this.source = source;
    this.video_id = videoId;
    this.youtube_collection_failure = true;
    this.youtube_failure_evidence = {
      status: null,
      body: String(reason ?? message ?? ""),
      source: String(source ?? "youtube_player"),
      target_url: videoId
        ? `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`
        : null,
      client: null,
    };
  }
}

export function isYoutubeCollectionFailureError(error) {
  return error?.youtube_collection_failure === true
    || error?.name === "YoutubeCollectionFailureError";
}

export function assertYoutubeContentObservation(detail, {
  videoId = null,
  source = null,
} = {}) {
  if (detail?.playability_kind !== "collection_failure") return detail;
  const reasonCode = String(detail?.playability_reason_code || "collection_failure");
  const reasonLabel = reasonCode.replaceAll("_", " ");
  const reason = String(detail?.playability_reason || reasonCode);
  throw new YoutubeCollectionFailureError(
    `YouTube collection failure (${reasonLabel}): ${reason}`,
    {
      reasonCode,
      retryMode: String(detail?.playability_retry_mode || "new_identity"),
      source: source ?? detail?.source ?? null,
      videoId: videoId ?? detail?.id ?? null,
      reason,
    },
  );
}

export function resolveYoutubePlayability({ status = null, reason = null } = {}) {
  const code = String(status ?? "").trim().toUpperCase();
  const text = folded(reason);

  if (
    /not a bot|nao (?:e|sou) (?:um )?(?:robo|bot)|no (?:eres|soy) (?:un )?robot|captcha|unusual traffic|verify you are human/.test(text)
  ) {
    return decision("collection_failure", "unknown", null, "bot_challenge", "new_identity");
  }
  if (/member|subscriber.only|premium|join this channel|membro|miembro/.test(text)) {
    return decision("content", "members_only", "subscriber_only", "members_only");
  }
  if (/private video|video is private|video privado|video particular/.test(text)) {
    return decision("content", "private", "private", "private");
  }
  if (/confirm your age|verify your age|age.restricted|confirme sua idade|verifica tu edad|confirma tu edad/.test(text)) {
    return decision("content", "public", "age_restricted", "age_restricted");
  }
  if (/not (?:made )?(?:this )?video available in your country|not available in your country|country where you are located|region restricted|bloqueado en tu pais|nao esta disponivel no seu pais/.test(text)) {
    return decision("content", "public", "region_restricted", "region_restricted", "alternate_region");
  }
  if (/removed by (?:the )?uploader|uploader has removed|removido pelo uploader|eliminado por quien lo subio/.test(text)) {
    return decision("content", "unavailable", "unavailable", "uploader_removed");
  }
  if (/copyright|direitos autorais|derechos de autor/.test(text)) {
    return decision("content", "unavailable", "unavailable", "copyright_removed");
  }
  if (/account (?:has been )?terminated|associated youtube account.*terminated|conta.*encerrad|cuenta.*cancelad/.test(text)) {
    return decision("content", "unavailable", "unavailable", "account_terminated");
  }
  if (/video (?:does not exist|not found)|video.*no existe|video.*nao existe/.test(text)) {
    return decision("content", "unavailable", "unavailable", "not_found");
  }
  if (/has been removed|video was removed|video foi removido|video ha sido eliminado/.test(text)) {
    return decision("content", "unavailable", "unavailable", "removed");
  }

  if (code === "OK") return decision("content", "public", "public", "playable");
  if (code === "LOGIN_REQUIRED") {
    return decision(
      "inconclusive",
      "unknown",
      null,
      "unsupported_login_required_reason",
      "alternate_client",
    );
  }
  if (
    ["UNPLAYABLE", "ERROR"].includes(code)
    && (!text || /^video unavailable[.!]?$/.test(text))
  ) {
    return decision("inconclusive", "unknown", null, "generic_video_unavailable", "alternate_client");
  }
  if (["UNPLAYABLE", "ERROR"].includes(code)) {
    return decision("inconclusive", "unknown", null, "unsupported_playability_reason", "alternate_client");
  }
  return decision("inconclusive", "unknown", null, "playability_unresolved", "alternate_client");
}
