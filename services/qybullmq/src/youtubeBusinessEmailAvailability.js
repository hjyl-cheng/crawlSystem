const AVAILABLE = "available";
const NOT_AVAILABLE = "not_available";
const UNKNOWN = "unknown";

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function renderedText(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value.trim() || null;
  try {
    const output = value.toString();
    return output && output !== "[object Object]" ? output.trim() || null : null;
  } catch {
    return null;
  }
}

function result(available, status) {
  return Object.freeze({ available, status });
}

export function observeYoutubeBusinessEmail(aboutResult, { aboutObserved = false } = {}) {
  if (aboutObserved !== true) return result(null, UNKNOWN);
  const response = object(aboutResult);
  if (!response) return result(null, UNKNOWN);
  const metadata = object(response.metadata);
  const directAbout = metadata ?? (
    Object.hasOwn(response, "sign_in_for_business_email")
      || Object.hasOwn(response, "signInForBusinessEmail")
      ? response
      : null
  );
  if (!directAbout) return result(null, UNKNOWN);
  const marker = renderedText(
    directAbout.sign_in_for_business_email ?? directAbout.signInForBusinessEmail,
  );
  return marker ? result(true, AVAILABLE) : result(false, NOT_AVAILABLE);
}

export function normalizeYoutubeBusinessEmailCurrent(availableValue, statusValue, {
  aboutObserved = true,
} = {}) {
  if (aboutObserved !== true) return result(null, UNKNOWN);
  if (availableValue === true && statusValue === AVAILABLE) return result(true, AVAILABLE);
  if (availableValue === false && statusValue === NOT_AVAILABLE) {
    return result(false, NOT_AVAILABLE);
  }
  return result(null, UNKNOWN);
}

export const YOUTUBE_BUSINESS_EMAIL_STATUSES = Object.freeze({
  AVAILABLE,
  NOT_AVAILABLE,
  UNKNOWN,
});
