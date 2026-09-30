// Imported Query terms may name only a base language (`pt`) and no country.
// Such a Query searches with the single Discover Identity Policy of that
// language. A regional language or an explicit country must match exactly.
export function resolveDiscoverQueryLocale({ language, country } = {}, policies = []) {
  const requestedLanguage = String(language ?? "").trim();
  const requestedCountry = String(country ?? "").trim().toUpperCase();
  const regional = requestedLanguage.includes("-");
  const base = requestedLanguage.toLowerCase().split("-")[0];
  const matches = base ? policies.filter((policy) => {
    const policyLanguage = String(policy?.youtube_language ?? "").trim().toLowerCase();
    return String(policy?.role ?? "").trim().toLowerCase() === "discover"
      && (regional ? policyLanguage === requestedLanguage.toLowerCase() : policyLanguage.split("-")[0] === base)
      && (!requestedCountry || String(policy?.youtube_country ?? "").trim().toUpperCase() === requestedCountry);
  }) : [];
  if (matches.length !== 1) return { language: requestedLanguage || null, country: requestedCountry || null };
  return { language: matches[0].youtube_language, country: String(matches[0].youtube_country).trim().toUpperCase() };
}
