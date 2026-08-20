from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Iterable, Sequence

from .comment_features import CommentEvidence
from .contracts import AGE_RANGES, AnalysisPolicy, ChannelSnapshot, FieldResult
from .creator_evidence import CreatorEvidence, extract_creator_evidence
from .hamilton import apportion
from .priors import PriorCatalog
from .taxonomy import CHANNEL_CATEGORY_TREE
from .text_features import LanguageEvidence, corpus, normalize_text, normalize_weights, snapshot_text_units, words


BASELINE_VERSION = "deterministic-public-signal-v1"
POLICY_VERSION = "shadow-policy-v1"


def unavailable(reason: str, *, model_version: str = BASELINE_VERSION) -> FieldResult:
    return FieldResult(
        value=None,
        source_type="public_signal_model",
        truth_status="unavailable",
        evidence_strength="unavailable",
        model_confidence=0.0,
        evidence_confidence=0.0,
        evidence_refs=(reason,),
        abstained=True,
        model_version=model_version,
        decision_policy_version=POLICY_VERSION,
        metadata={"reason": reason},
    )


def analyze_language(
    evidence: LanguageEvidence,
    policy: AnalysisPolicy,
    *,
    model_version: str = "language-heuristic-v1",
    pretrained_model: bool = False,
    fallback_language: str | None = None,
) -> FieldResult:
    sparse_country_supported_text = bool(
        fallback_language
        and policy is AnalysisPolicy.COMPLETE_ESTIMATE
        and evidence.effective_characters < 50
        and evidence.source_count <= 2
    )
    if (
        not evidence.probabilities
        or evidence.effective_characters < 15
        or sparse_country_supported_text
    ):
        if policy is AnalysisPolicy.COMPLETE_ESTIMATE and fallback_language:
            return FieldResult(
                value=fallback_language,
                source_type="public_prior_estimate",
                truth_status="estimated",
                evidence_strength="prior_only",
                model_confidence=0.35,
                evidence_confidence=0.2,
                candidates=({"value": fallback_language, "probability": 1.0},),
                evidence_refs=("country_primary_language_fallback",),
                model_version="language-country-fallback-v1",
                decision_policy_version=POLICY_VERSION,
                metadata={
                    "fallback_used": True,
                    "fallback_reason": (
                        "sparse_text_with_explicit_country"
                        if sparse_country_supported_text
                        else "insufficient_text"
                    ),
                    "effective_characters": evidence.effective_characters,
                    "source_count": evidence.source_count,
                },
            )
        return unavailable("insufficient_text_for_language")
    ranked = sorted(evidence.probabilities.items(), key=lambda item: (-item[1], item[0]))
    top, probability = ranked[0]
    if policy is AnalysisPolicy.EVIDENCE_FIRST and (probability < 0.55 or evidence.top_margin < 0.12):
        return unavailable("language_sources_conflict")
    strength = "strong" if probability >= 0.75 and evidence.source_count >= 3 else "weak"
    return FieldResult(
        value=top,
        source_type="public_signal_model" if pretrained_model else "rule_inferred",
        truth_status="estimated",
        evidence_strength=strength,
        model_confidence=round(probability, 6),
        evidence_confidence=round(min(0.95, evidence.effective_characters / 1000), 6),
        candidates=tuple({"value": key, "probability": round(value, 6)} for key, value in ranked[:5]),
        evidence_refs=(f"language_text_units:{evidence.source_count}",),
        model_version=model_version,
        decision_policy_version=POLICY_VERSION,
        metadata={"effective_characters": evidence.effective_characters},
    )


_COUNTRY_ALIASES = {
    "BR": "Brazil", "BRA": "Brazil", "MX": "Mexico", "MEX": "Mexico",
    "US": "United States", "USA": "United States", "PT": "Portugal",
    "ES": "Spain", "AR": "Argentina", "CO": "Colombia", "IN": "India",
    "GB": "United Kingdom", "UK": "United Kingdom", "CA": "Canada",
    "AU": "Australia", "FR": "France", "DE": "Germany", "IT": "Italy",
    "JP": "Japan", "KR": "South Korea", "VN": "Vietnam", "ID": "Indonesia",
    "CN": "China", "CHN": "China", "TW": "Taiwan", "TWN": "Taiwan",
    "HK": "Hong Kong", "HKG": "Hong Kong", "MO": "Macau", "MAC": "Macau",
    "SG": "Singapore", "SGP": "Singapore", "MY": "Malaysia", "MYS": "Malaysia",
}

_COUNTRY_NAME_ALIASES = {
    "brasil": "Brazil", "méxico": "Mexico", "mexico": "Mexico",
    "españa": "Spain", "espana": "Spain", "estados unidos": "United States",
    "reino unido": "United Kingdom", "corea del sur": "South Korea",
    "coréia do sul": "South Korea", "台灣": "Taiwan", "台湾": "Taiwan",
    "臺灣": "Taiwan", "香港": "Hong Kong", "澳門": "Macau", "澳门": "Macau",
}

_COUNTRY_PRIMARY_LANGUAGE = {
    "Argentina": "Spanish", "Australia": "English", "Austria": "German",
    "Belgium": "French", "Brazil": "Portuguese", "Canada": "English",
    "Chile": "Spanish", "Colombia": "Spanish", "France": "French",
    "Germany": "German", "India": "Hindi", "Indonesia": "Indonesian",
    "Italy": "Italian", "Japan": "Japanese", "Mexico": "Spanish",
    "Mozambique": "Portuguese", "Portugal": "Portuguese", "Russia": "Russian",
    "South Korea": "Korean", "Spain": "Spanish", "Switzerland": "German",
    "Thailand": "Thai", "Turkey": "Turkish", "United Kingdom": "English",
    "United States": "English", "Vietnam": "Vietnamese",
    "China": "Chinese", "Taiwan": "Chinese", "Hong Kong": "Chinese",
    "Macau": "Chinese", "Singapore": "English", "Malaysia": "Malay",
}


def _canonical_country(value: object, code: str = "") -> str:
    raw = str(value or "").strip()
    normalized_code = str(code or "").upper().strip()
    if normalized_code in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[normalized_code]
    if not raw:
        return ""
    if raw.upper() in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[raw.upper()]
    return _COUNTRY_NAME_ALIASES.get(normalize_text(raw), raw)


def primary_language_hint(channel: dict[str, Any]) -> str | None:
    code = str(channel.get("country_code") or "").upper().strip()
    country = _canonical_country(
        channel.get("country_canonical_name")
        or channel.get("country")
        or "",
        code,
    )
    return _COUNTRY_PRIMARY_LANGUAGE.get(country)

_COUNTRY_SIGNAL_WEIGHTS = {
    "self_location": 6.0,
    "phone_country_code": 5.0,
    "country_domain": 4.5,
    "country_specific_currency": 2.0,
}

_COUNTRY_PATTERNS: dict[str, dict[str, tuple[str, ...]]] = {
    "Brazil": {
        "self_location": (r"\b(?:sou|somos|moro|moramos|vivo|vivemos) (?:do|no|na|em) brasil\b",),
        "phone_country_code": (r"(?<!\d)\+55(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+(?:com\.)?br(?:[/\s]|$)",),
    },
    "Mexico": {
        "self_location": (r"\b(?:soy|somos|vivo|vivimos|estoy|estamos) (?:de|en) m[eé]xico\b",),
        "phone_country_code": (r"(?<!\d)\+52(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+mx(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)mxn(?!\w)",),
    },
    "Portugal": {
        "self_location": (r"\b(?:sou|somos|moro|moramos|vivo|vivemos) (?:de|em) portugal\b",),
        "phone_country_code": (r"(?<!\d)\+351(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+pt(?:[/\s]|$)",),
    },
    "Spain": {
        "self_location": (r"\b(?:soy|somos|vivo|vivimos|estoy|estamos) (?:de|en) españa\b",),
        "phone_country_code": (r"(?<!\d)\+34(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+es(?:[/\s]|$)",),
    },
    "Argentina": {
        "self_location": (r"\b(?:soy|somos|vivo|vivimos|estoy|estamos) (?:de|en) argentina\b",),
        "phone_country_code": (r"(?<!\d)\+54(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+ar(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)ars(?!\w)",),
    },
    "Colombia": {
        "self_location": (r"\b(?:soy|somos|vivo|vivimos|estoy|estamos) (?:de|en) colombia\b",),
        "phone_country_code": (r"(?<!\d)\+57(?=[\s().-]?\d)",),
        "country_specific_currency": (r"(?<!\w)cop(?!\w)",),
    },
    "United States": {
        "self_location": (r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) (?:the )?(?:usa|u\.s\.a\.|united states)\b",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+us(?:[/\s]|$)",),
    },
    "United Kingdom": {
        "self_location": (r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) (?:the )?(?:uk|u\.k\.|united kingdom|england|scotland|wales|northern ireland)\b",),
        "phone_country_code": (r"(?<!\d)\+44(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+uk(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)(?:gbp|£)(?!\w)",),
    },
    "Canada": {
        "self_location": (r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) canada\b",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+ca(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)cad(?!\w)",),
    },
    "Australia": {
        "self_location": (r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) australia\b",),
        "phone_country_code": (r"(?<!\d)\+61(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+au(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)aud(?!\w)",),
    },
    "India": {
        "self_location": (r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) india\b",),
        "phone_country_code": (r"(?<!\d)\+91(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+in(?:[/\s]|$)",),
    },
    "Taiwan": {
        "self_location": (
            r"(?:我(?:們|们)?(?:來自|来自|住在|位於|位于|人在)|(?:頻道|频道|團隊|团队)(?:位於|位于))\s*(?:台灣|台湾|臺灣)",
            r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) taiwan\b",
        ),
        "phone_country_code": (r"(?<!\d)\+886(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+tw(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)(?:twd|nt\$)(?!\w)",),
    },
    "Hong Kong": {
        "self_location": (
            r"(?:我(?:們|们)?(?:來自|来自|住在|位於|位于|人在)|(?:頻道|频道|團隊|团队)(?:位於|位于))\s*香港",
            r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) hong kong\b",
        ),
        "phone_country_code": (r"(?<!\d)\+852(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+hk(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)(?:hkd|hk\$)(?!\w)",),
    },
    "Macau": {
        "self_location": (
            r"(?:我(?:們|们)?(?:來自|来自|住在|位於|位于|人在)|(?:頻道|频道|團隊|团队)(?:位於|位于))\s*(?:澳門|澳门)",
            r"\b(?:i am|i'm|we are|we're|based|live|located) (?:from|in) (?:macau|macao)\b",
        ),
        "phone_country_code": (r"(?<!\d)\+853(?=[\s().-]?\d)",),
        "country_domain": (r"(?<![\w.])(?:[\w-]+\.)+mo(?:[/\s]|$)",),
        "country_specific_currency": (r"(?<!\w)(?:mop|mop\$)(?!\w)",),
    },
}


def _external_link_text(channel: dict[str, Any]) -> str:
    values: list[str] = []
    for item in channel.get("external_links") or []:
        if isinstance(item, dict):
            values.extend(str(value) for value in item.values() if isinstance(value, (str, int)))
        else:
            values.append(str(item))
    return " ".join(values)


def analyze_country(
    snapshot: ChannelSnapshot,
    languages: dict[str, float],
    catalog: PriorCatalog,
    policy: AnalysisPolicy,
    *,
    channel_text: str | None = None,
) -> FieldResult:
    channel = snapshot.channel
    explicit = channel.get("country_canonical_name") or channel.get("country")
    code = str(channel.get("country_code") or "").upper().strip()
    explicit_value = _canonical_country(explicit)
    code_value = _canonical_country("", code)
    if explicit_value and code_value and explicit_value != code_value:
        candidates = (
            {"value": explicit_value, "probability": 0.5},
            {"value": code_value, "probability": 0.5},
        )
        if policy is AnalysisPolicy.EVIDENCE_FIRST:
            return FieldResult(
                value=None,
                source_type="observed",
                truth_status="unavailable",
                evidence_strength="unavailable",
                model_confidence=0.5,
                evidence_confidence=0.0,
                candidates=candidates,
                evidence_refs=("crawler.channels.country", "crawler.channels.country_code"),
                abstained=True,
                model_version="country-structured-conflict-v1",
                decision_policy_version=POLICY_VERSION,
                metadata={
                    "reason": "structured_country_fields_conflict",
                    "country_value": explicit_value,
                    "country_code_value": code_value,
                },
            )
        return FieldResult(
            value=explicit_value,
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.5,
            evidence_confidence=0.4,
            candidates=candidates,
            evidence_refs=("crawler.channels.country", "crawler.channels.country_code"),
            model_version="country-structured-conflict-v1",
            decision_policy_version=POLICY_VERSION,
            metadata={
                "structured_conflict": True,
                "country_value": explicit_value,
                "country_code_value": code_value,
                "forced_for_legacy_completeness": True,
            },
        )
    value = explicit_value or code_value
    if value:
        return FieldResult(
            value=value,
            source_type="observed",
            truth_status="observed",
            evidence_strength="explicit",
            model_confidence=1.0,
            evidence_confidence=1.0,
            evidence_refs=("crawler.channels.country",),
            model_version="country-explicit-v1",
            decision_policy_version=POLICY_VERSION,
        )

    channel_text = (channel_text if channel_text is not None else corpus(snapshot, include_content=False))
    channel_text += " " + normalize_text(_external_link_text(channel))
    scores: Counter[str] = Counter()
    evidence_groups: defaultdict[str, set[str]] = defaultdict(set)
    evidence_refs: list[str] = []
    for country, grouped_patterns in _COUNTRY_PATTERNS.items():
        for group, patterns in grouped_patterns.items():
            if any(re.search(pattern, channel_text, re.IGNORECASE) for pattern in patterns):
                # Repeated mentions from the same source family do not become
                # independent evidence. Each group contributes at most once.
                scores[country] += _COUNTRY_SIGNAL_WEIGHTS[group]
                evidence_groups[country].add(group)
                evidence_refs.append(f"country_signal:{country}:{group}")
    if scores:
        probabilities = normalize_weights(dict(scores))
        ranked = sorted(probabilities.items(), key=lambda item: (-item[1], item[0]))
        top, probability = ranked[0]
        second_probability = ranked[1][1] if len(ranked) > 1 else 0.0
        margin = probability - second_probability
        top_groups = evidence_groups[top]
        decisive_groups = {"self_location", "phone_country_code", "country_domain"}
        strong = bool(top_groups & decisive_groups) and probability >= 0.65 and margin >= 0.15
        if policy is AnalysisPolicy.EVIDENCE_FIRST and not strong:
            return FieldResult(
                value=None,
                source_type="rule_inferred",
                truth_status="unavailable",
                evidence_strength="unavailable",
                model_confidence=round(probability, 6),
                evidence_confidence=0.0,
                candidates=tuple(
                    {"value": key, "probability": round(score, 6)}
                    for key, score in ranked[:5]
                ),
                evidence_refs=tuple(sorted(set(evidence_refs))),
                abstained=True,
                model_version="country-independent-signals-v2",
                decision_policy_version=POLICY_VERSION,
                metadata={
                    "reason": "country_public_signals_are_weak_or_conflicted",
                    "evidence_groups": {
                        key: sorted(evidence_groups[key]) for key, _ in ranked[:5]
                    },
                    "top_margin": round(margin, 6),
                },
            )
        confidence_by_group = {
            "self_location": 0.9,
            "phone_country_code": 0.88,
            "country_domain": 0.84,
            "country_specific_currency": 0.55,
        }
        evidence_confidence = max(confidence_by_group[group] for group in top_groups)
        if len(top_groups) >= 2:
            evidence_confidence = min(0.97, evidence_confidence + 0.05)
        evidence_confidence *= 0.6 + 0.4 * probability
        return FieldResult(
            value=top,
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="strong" if strong else "weak",
            model_confidence=round(probability, 6),
            evidence_confidence=round(evidence_confidence, 6),
            candidates=tuple({"value": key, "probability": round(score, 6)} for key, score in ranked[:5]),
            evidence_refs=tuple(sorted(set(evidence_refs))),
            model_version="country-independent-signals-v2",
            decision_policy_version=POLICY_VERSION,
            metadata={
                "evidence_groups": {
                    key: sorted(evidence_groups[key]) for key, _ in ranked[:5]
                },
                "top_margin": round(margin, 6),
                "language_used_as_country_evidence": False,
            },
        )

    market_scores: Counter[str] = Counter()
    for language, language_probability in languages.items():
        if language == "Other" or language not in catalog.data["language_markets"]:
            continue
        for country, weight in catalog.language_market(language).items():
            if country != "Other":
                market_scores[country] += language_probability * weight
    ranked = sorted(normalize_weights(dict(market_scores)).items(), key=lambda item: (-item[1], item[0]))
    if not ranked or policy is AnalysisPolicy.EVIDENCE_FIRST:
        return unavailable("country_has_no_explicit_or_strong_public_evidence")
    top, probability = ranked[0]
    return FieldResult(
        value=top,
        source_type="public_prior_estimate",
        truth_status="estimated",
        evidence_strength="prior_only",
        model_confidence=round(probability, 6),
        evidence_confidence=0.2,
        candidates=tuple({"value": key, "probability": round(score, 6)} for key, score in ranked[:5]),
        evidence_refs=(f"prior_catalog:{catalog.version}:language_markets",),
        model_version="country-language-prior-v1",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "prior_production_eligible": catalog.production_eligible,
            "language_only": True,
            "forced_for_legacy_completeness": True,
            "warning": "Language defines candidate markets but does not identify creator country.",
        },
    )


_STRONG_TEAM_PATTERNS = (
    r"\b(?:we are|we're|n[oó]s somos|nosotros somos)\b",
    r"\bsomos\s+(?:uma?|un[ao]?)\s+(?:equipe|empresa|ag[eê]ncia|marca|produtora|time|grupo|canal)\b",
    r"\b(?:our|nosso|nossa|nuestro|nuestra) (?:team|equipe|equipo|family|fam[ií]lia|canal|channel)\b",
    r"\b(?:dupla|trio|casal|irm[aã]os|irm[aã]s|brothers|sisters|collective|coletivo|colectivo)\b",
    r"\b(?:we are|we're|n[oó]s somos|nosotros somos)\s+(?:an? |uma? |un[ao]? )?"
    r"(?:empresa|company|ag[eê]ncia|agency|record label|gravadora|newsroom|redacci[oó]n)\b",
)
_TEAM_MARKERS = (
    "official channel", "canal oficial",
    "equipe", "equipo", "team",
    "clube",
    "records", "ministry", "igreja", "church",
)
_MALE_SELF = (
    r"\b(?:eu )?sou (?:um )?(?:homem|pai|papai|marido|rapaz|ator|cantor|blogueiro|influenciador|empreendedor|criador)\b",
    r"\b(?:eu )?sou o (?!maior|melhor|unico|único|novo|primeiro|canal|tipo)[\wà-ÿ'’-]+",
    r"\b(?:eu )?sou (?:muito )?(?:apaixonado|grato|cansado|formado|casado|divorciado|separado|brasileiro|americano|m[eé]dico)\b",
    r"\b(?:criador de conte[uú]do|fundador|propriet[aá]rio)\b",
    r"\b(?:um pai|sou pai|pai de fam[ií]lia)\b",
    r"\bsoy (?:un |el )?(?:hombre|padre|pap[aá]|esposo|chico|actor|bloguero|influenciador|emprendedor|creador)\b",
    r"\bi(?:'m| am) (?:a )?(?:man|father|dad|husband|male creator|actor|businessman)\b",
    r"\b(?:father|dad|husband|pai|papai|padre|pap[aá]) of\b",
)
_FEMALE_SELF = (
    r"\b(?:eu )?sou (?:uma )?(?:mulher|m[aã]e|mam[aã]e|esposa|garota|atriz|cantora|blogueira|influenciadora|empreendedora|criadora)\b",
    r"\b(?:eu )?sou a (?!maior|melhor|unica|única|nova|primeira|favor|prova|mesma|pessoa|canal|tipo)[\wà-ÿ'’-]+",
    r"\b(?:eu )?sou (?:muito )?(?:apaixonada|grata|cansada|formada|casada|divorciada|separada|gr[aá]vida|russa|brasileira|americana|m[eé]dica|gaiteira)\b",
    r"\b(?:maquiadora|criadora de conte[uú]do|criadora\s+d[aeo]s?|apresentadora|professora|doutora|advogada|enfermeira|fundadora|propriet[aá]ria|gaiteira)\b",
    r"\b(?:uma m[aã]e|sou m[aã]e|m[aã]e de fam[ií]lia)\b",
    r"\bsoy (?:una |la )?(?:mujer|madre|mam[aá]|esposa|chica|actriz|bloguera|influenciadora|emprendedora|creadora)\b",
    r"\bi(?:'m| am) (?:a )?(?:woman|mother|mom|wife|female creator|actress|businesswoman|makeup artist)\b",
    r"\b(?:mother|mom|wife|m[aã]e|mam[aã]e|madre|mam[aá]) of\b",
    r"\bobrigada\b",
)
_MALE_CREATOR_ATTRIBUTION = (
    r"\bcriad[oa]\s+(?:pelo|por um)\s+"
    r"(?!(?:grupo|time|empresa|ag[eê]ncia|equipe|marca|produtora|organiza[cç][aã]o|coletivo)\b)"
    r"(?:prof(?:essor)?\.?|senhor|sr\.?|doutor|dr\.?|homem|rapaz|garoto|cabeludo|"
    r"criador|apresentador|ator|cantor|chef|jogador|gamer)\b",
    r"\bcread[oa]\s+por un\s+"
    r"(?!(?:grupo|equipo|empresa|agencia|marca|productora|organizaci[oó]n|colectivo)\b)"
    r"(?:profesor|se[nñ]or|doctor|hombre|chico|creador|presentador|actor|cantante|chef|jugador|gamer)\b",
)
_FEMALE_CREATOR_ATTRIBUTION = (
    r"\bcriad[oa]\s+(?:pela|por uma)\s+"
    r"(?!(?:empresa|ag[eê]ncia|equipe|marca|produtora|organiza[cç][aã]o|comunidade|fam[ií]lia)\b)"
    r"(?:professora|senhora|sra\.?|doutora|dra\.?|mulher|garota|criadora|apresentadora|"
    r"atriz|cantora|chef|jogadora|gamer)\b",
    r"\bcread[oa]\s+por una\s+"
    r"(?!(?:empresa|agencia|marca|productora|organizaci[oó]n|comunidad|familia)\b)"
    r"(?:profesora|se[nñ]ora|doctora|mujer|chica|creadora|presentadora|actriz|cantante|chef|jugadora|gamer)\b",
)
_MALE_NAMES = {
    "joao", "joão", "jose", "josé", "carlos", "pedro", "lucas", "gabriel",
    "rafael", "bruno", "felipe", "diego", "marcos", "ricardo", "fernando",
    "miguel", "juan", "luis", "luís", "andres", "andrés", "alejandro", "daniel",
    "rodrigo", "david", "flavio", "flávio", "adriano", "peter", "davi",
    "maicon", "marcio", "márcio", "antonio", "antônio", "paulo", "thiago",
    "tiago", "matheus", "mateus", "guilherme", "leonardo", "eduardo",
    "roberto", "andre", "andré", "fábio", "fabio", "igor", "claudio",
    "cláudio", "tarcisio", "tarcísio", "patricio", "patrício",
}
_FEMALE_NAMES = {
    "maria", "ana", "juliana", "fernanda", "camila", "larissa", "beatriz",
    "gabriela", "leticia", "letícia", "lorrayne", "carla", "paula", "mariana",
    "sofia", "sofía", "valentina", "isabella", "laura", "daniela", "lucia",
    "luisa", "luísa", "joana", "sabrina", "nicolly", "vania", "vânia",
    "herika", "hérika", "heri", "héri", "dayene", "gabi", "dana", "thali",
    "thalita", "patricia", "patrícia", "aline", "amanda", "bruna", "carolina",
    "isabela", "daiana", "dayana", "tatiane", "tati", "geovana", "giovana",
    "bella", "poliane",
}
_TITLE_NAME_SKIP = {
    "canal", "oficial", "official", "tv", "dr", "dra", "prof", "professor",
    "professora", "dj", "mc", "podcast", "the", "por", "com", "with", "by",
    "da", "do", "de", "e", "escola", "studio", "films", "film", "mestre",
    "pastor", "padre", "frei", "sr", "sra", "pe",
}


def _lexicon_gender(name: str) -> str | None:
    token = normalize_text(name).split(" ")[0]
    if token in _MALE_NAMES:
        return "male"
    if token in _FEMALE_NAMES:
        return "female"
    return None


def _lexicon_gender_from_texts(*texts: str) -> tuple[str | None, str, bool]:
    found: dict[str, str] = {}
    for text in texts:
        if not text:
            continue
        for token in words(text):
            if token in _TITLE_NAME_SKIP or len(token) < 3:
                continue
            gender = _lexicon_gender(token)
            if gender:
                found[token] = gender
    genders = set(found.values())
    if len(genders) == 1:
        token = next(iter(found))
        return next(iter(genders)), token, False
    if len(genders) > 1:
        return None, "", True
    return None, "", False


def _marker_hits(text: str, markers: tuple[str, ...]) -> list[str]:
    hits = []
    for marker in markers:
        if re.search(rf"(?<![.\w]){re.escape(marker)}(?!\w)", text):
            hits.append(marker)
    return hits


def _team_gender_result(hits: list[str], *, strong: bool) -> FieldResult:
    confidence = 0.82 if strong else 0.62
    return FieldResult(
        value="brand_team",
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength="strong" if strong else "weak",
        model_confidence=confidence,
        evidence_confidence=0.72 if strong else 0.4,
        candidates=(
            {"value": "brand_team", "probability": confidence},
            {"value": "male", "probability": round((1 - confidence) / 2, 6)},
            {"value": "female", "probability": round((1 - confidence) / 2, 6)},
        ),
        evidence_refs=("channel_identity:team_or_brand_marker",),
        model_version="creator-gender-entity-v3",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "team_markers": hits[:8],
            "evidence_tier": "A" if strong else "C",
        },
    )


def _name_gender_result(value: str, token: str, *, source: str) -> FieldResult:
    return FieldResult(
        value=value,
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength="weak",
        model_confidence=0.58,
        evidence_confidence=0.3,
        candidates=(
            {"value": value, "probability": 0.58},
            {"value": "brand_team", "probability": 0.42},
        ),
        evidence_refs=(source,),
        model_version="creator-gender-name-baseline-v2",
        decision_policy_version=POLICY_VERSION,
        metadata={"name_token": token, "evidence_tier": "C"},
    )


def analyze_gender(
    snapshot: ChannelSnapshot,
    policy: AnalysisPolicy,
    *,
    comments: CommentEvidence | None = None,
    identity: CreatorEvidence | None = None,
) -> FieldResult:
    identity = identity or extract_creator_evidence(snapshot)
    if identity.gender_events:
        candidate = identity.gender_candidate
        if candidate is None:
            if policy is AnalysisPolicy.EVIDENCE_FIRST:
                return unavailable("creator_gender_explicit_evidence_conflict")
        else:
            events = tuple(
                event for event in identity.gender_events if event.candidate == candidate
            )
            return FieldResult(
                value=candidate,
                source_type="rule_inferred",
                truth_status="estimated",
                evidence_strength="strong" if not identity.gender_conflict else "weak",
                model_confidence=0.94 if not identity.gender_conflict else 0.55,
                evidence_confidence=0.92 if not identity.gender_conflict else 0.4,
                candidates=(
                    {"value": candidate, "probability": 0.94},
                ),
                evidence_refs=tuple(event.source_ref for event in events[:3]),
                model_version="creator-identity-evidence-v1",
                decision_policy_version=POLICY_VERSION,
                metadata={
                    "account_entity_type": identity.account_entity_type,
                    "primary_creator_status": identity.primary_creator_status,
                    "primary_creator_subject": identity.primary_creator_subject,
                    "evidence_tier": "A",
                    "evidence_claim_types": sorted({event.claim_type for event in events}),
                    "deduplicated_evidence_count": len(events),
                    "raw_evidence_count": sum(event.duplicate_count for event in events),
                    "template_duplicates_removed": sum(
                        event.duplicate_count - 1 for event in events
                    ),
                },
            )
    channel = snapshot.channel
    title = normalize_text(channel.get("title"))
    about = normalize_text(" ".join((str(channel.get("summary") or ""), str(channel.get("about_description") or ""))))
    entity_text = f"{title} {about}"
    strong_team_hits = [pattern for pattern in _STRONG_TEAM_PATTERNS if re.search(pattern, entity_text)]
    weak_team_hits = _marker_hits(entity_text, _TEAM_MARKERS)
    male_self_hits = [pattern for pattern in _MALE_SELF if re.search(pattern, about)]
    female_self_hits = [pattern for pattern in _FEMALE_SELF if re.search(pattern, about)]
    male_attribution_hits = [
        pattern for pattern in _MALE_CREATOR_ATTRIBUTION if re.search(pattern, about)
    ]
    female_attribution_hits = [
        pattern for pattern in _FEMALE_CREATOR_ATTRIBUTION if re.search(pattern, about)
    ]
    male_hits = [*male_self_hits, *male_attribution_hits]
    female_hits = [*female_self_hits, *female_attribution_hits]
    if male_hits or female_hits:
        value = "male" if len(male_hits) > len(female_hits) else "female"
        conflict = bool(male_hits and female_hits)
        if conflict and policy is AnalysisPolicy.EVIDENCE_FIRST:
            return unavailable("creator_gender_self_reference_conflict")
        return FieldResult(
            value=value,
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="strong" if not conflict else "weak",
            model_confidence=0.9 if not conflict else 0.55,
            evidence_confidence=0.85 if not conflict else 0.4,
            evidence_refs=(
                "channel_about:single_creator_attribution"
                if male_attribution_hits or female_attribution_hits
                else "channel_about:self_identification",
            ),
            model_version="creator-gender-explicit-identity-v3",
            decision_policy_version=POLICY_VERSION,
        )
    named_primary = bool(identity.primary_creator_subject)
    if strong_team_hits and not named_primary:
        return _team_gender_result(strong_team_hits, strong=True)
    allow_comment_gender = not (
        identity.account_entity_type == "brand_or_team" and not named_primary
    )
    if allow_comment_gender and comments and comments.creator_gender_probabilities:
        ranked_comment_gender = sorted(
            comments.creator_gender_probabilities.items(),
            key=lambda item: (-item[1], item[0]),
        )
        value, probability = ranked_comment_gender[0]
        support = comments.creator_gender_support.get(value, {})
        author_count = int(support.get("author_count", 0))
        video_count = int(support.get("video_count", 0))
        consensus_gate_passed = (
            author_count >= 5 and video_count >= 3 and probability >= 0.8
        )
        if (
            policy is AnalysisPolicy.COMPLETE_ESTIMATE
            or consensus_gate_passed
        ):
            return FieldResult(
                value=value,
                source_type="public_signal_model",
                truth_status="estimated",
                evidence_strength="weak",
                model_confidence=round(min(0.78, 0.45 + probability * 0.25 + min(0.08, author_count / 100)), 6),
                evidence_confidence=round(min(0.6, 0.25 + author_count / 40), 6),
                candidates=tuple(
                    {"value": label, "probability": round(score, 6)}
                    for label, score in ranked_comment_gender
                ),
                evidence_refs=(
                    f"comment_creator_address_authors:{author_count}",
                    f"comment_creator_address_videos:{video_count}",
                ),
                model_version="creator-gender-comment-consensus-v2",
                decision_policy_version=POLICY_VERSION,
                metadata={
                    "comment_sample_bias": "top_comments_first_page",
                    "comment_signal_role": "viewer_address_consensus",
                    "evidence_tier": "B",
                    "consensus_gate_passed": consensus_gate_passed,
                    "support": support,
                    "account_entity_type": "unknown",
                    "primary_creator_status": (
                        "single_stable" if consensus_gate_passed else "unknown"
                    ),
                },
            )
    name_gender, name_token, name_conflict = _lexicon_gender_from_texts(
        identity.primary_creator_subject,
        str(channel.get("title") or ""),
    )
    if name_conflict and policy is AnalysisPolicy.EVIDENCE_FIRST:
        return unavailable("creator_gender_name_signal_conflict")
    if name_gender:
        if policy is AnalysisPolicy.EVIDENCE_FIRST:
            return unavailable("creator_gender_only_has_name_signal")
        source = (
            "primary_subject:name_lexicon"
            if identity.primary_creator_subject and name_token in words(identity.primary_creator_subject)
            else "channel_title:given_name_lexicon"
        )
        return _name_gender_result(name_gender, name_token, source=source)
    if weak_team_hits and not named_primary:
        if policy is AnalysisPolicy.EVIDENCE_FIRST:
            return unavailable("creator_gender_only_has_weak_team_marker")
        return _team_gender_result(weak_team_hits, strong=False)
    if policy is AnalysisPolicy.EVIDENCE_FIRST:
        return unavailable("creator_gender_has_no_reliable_subject_evidence")
    return FieldResult(
        value="brand_team",
        source_type="public_prior_estimate",
        truth_status="estimated",
        evidence_strength="prior_only",
        model_confidence=0.34,
        evidence_confidence=0.1,
        candidates=tuple(
            {"value": value, "probability": probability}
            for value, probability in (("brand_team", 0.34), ("male", 0.33), ("female", 0.33))
        ),
        evidence_refs=("compatibility_unknown_proxy:brand_team",),
        model_version="creator-gender-compat-fallback-v1",
        decision_policy_version=POLICY_VERSION,
        metadata={"compatibility_warning": "brand_team also represents unresolved legacy output"},
    )


_LIFE_STAGE_INTERVALS = (
    (re.compile(r"\b(?:sou estudante|soy estudiante|i am a student|i'm a student)\b", re.I), (16, 30), "student"),
    (re.compile(r"\b(?:sou aposentad[oa]|soy jubilad[oa]|i am retired|i'm retired)\b", re.I), (55, 80), "retired"),
    (re.compile(r"\b(?:sou universit[aá]ri[oa]|estudiante universitari[oa]|college student)\b", re.I), (18, 30), "university_student"),
)


def analyze_age(
    snapshot: ChannelSnapshot,
    category: str,
    catalog: PriorCatalog,
    policy: AnalysisPolicy,
    *,
    identity: CreatorEvidence | None = None,
) -> FieldResult:
    identity = identity or extract_creator_evidence(snapshot)
    age_candidates: list[tuple[int, int, object]] = []
    for event in identity.age_events:
        if event.claim_type == "explicit_age":
            elapsed_years = max(
                0,
                snapshot.as_of.year
                - event.observed_at.year
                - (
                    (snapshot.as_of.month, snapshot.as_of.day)
                    < (event.observed_at.month, event.observed_at.day)
                ),
            )
            age = event.value + elapsed_years
            if 13 <= age <= 90:
                age_candidates.append((age, age, event))
        elif event.claim_type == "birth_year":
            upper = snapshot.as_of.year - event.value
            lower = upper - 1
            if 13 <= lower <= upper <= 90:
                age_candidates.append((lower, upper, event))
    if age_candidates:
        lower = max(candidate[0] for candidate in age_candidates)
        upper = min(candidate[1] for candidate in age_candidates)
        conflict = lower > upper
        if conflict and policy is AnalysisPolicy.EVIDENCE_FIRST:
            return unavailable("creator_age_explicit_evidence_conflict")
        if conflict:
            lower = min(candidate[0] for candidate in age_candidates)
            upper = max(candidate[1] for candidate in age_candidates)
        selected_event = max(
            (candidate[2] for candidate in age_candidates),
            key=lambda event: (event.observed_at, event.source_ref),
        )
        value = round((lower + upper) / 2)
        return FieldResult(
            value=value,
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="explicit" if not conflict else "weak",
            model_confidence=0.98 if not conflict else 0.55,
            evidence_confidence=0.95 if not conflict else 0.4,
            evidence_refs=tuple(
                candidate[2].source_ref for candidate in age_candidates[:3]
            ),
            model_version="creator-age-evidence-v2",
            decision_policy_version=POLICY_VERSION,
            metadata={
                "age_interval": [lower, upper],
                "evidence_basis": selected_event.claim_type,
                "evidence_tier": "A",
                "deduplicated_evidence_count": len(identity.age_events),
                "raw_evidence_count": sum(
                    event.duplicate_count for event in identity.age_events
                ),
                "compatibility_point_from_interval": lower != upper,
                "as_of": snapshot.as_of.isoformat().replace("+00:00", "Z"),
            },
        )
    channel_text = " ".join(
        (str(snapshot.channel.get("summary") or ""), str(snapshot.channel.get("about_description") or ""))
    )
    if policy is AnalysisPolicy.EVIDENCE_FIRST:
        return unavailable("creator_age_has_no_explicit_evidence")
    lower, upper = catalog.creator_age_interval(category)
    life_stage_refs: list[str] = []
    for pattern, (stage_lower, stage_upper), label in _LIFE_STAGE_INTERVALS:
        if pattern.search(channel_text):
            intersect_lower = max(lower, stage_lower)
            intersect_upper = min(upper, stage_upper)
            if intersect_lower <= intersect_upper:
                lower, upper = intersect_lower, intersect_upper
            life_stage_refs.append(label)
    age = (lower + upper) // 2
    return FieldResult(
        value=age,
        source_type="public_prior_estimate",
        truth_status="estimated",
        evidence_strength="prior_only",
        model_confidence=round(1.0 / max(2, upper - lower + 1), 6),
        evidence_confidence=0.18 if life_stage_refs else 0.1,
        evidence_refs=(
            f"prior_catalog:{catalog.version}:creator_age_intervals:{category}",
            *(f"channel_about:life_stage:{label}" for label in life_stage_refs),
        ),
        model_version="creator-age-public-evidence-v2",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "age_interval": [lower, upper],
            "evidence_basis": "prior_only",
            "compatibility_point_from_interval": True,
            "life_stage_signals": life_stage_refs,
            "prior_production_eligible": catalog.production_eligible,
            "warning": "Content category is a population prior, not evidence of creator age.",
        },
    )


_CATEGORY_RULES: dict[tuple[str, str], tuple[str, ...]] = {
    ("Beauty Creators", "Handsome Men"): ("men's grooming", "male grooming", "barba", "barbearia", "cuidados masculinos"),
    ("Beauty Creators", "Beautiful Women"): ("beauty creator", "beleza feminina", "belleza femenina", "beauty tips"),
    ("Fashion", "Makeup"): ("makeup", "maquiagem", "maquillaje", "cosmetic"),
    ("Fashion", "Skincare"): ("skincare", "pele", "cuidados com a pele", "cuidado de la piel"),
    ("Fashion", "Clothing"): ("roupa", "clothing", "outfit", "moda", "fashion", "costura", "costurar"),
    ("Fashion", "Hair & Wigs"): ("cabelo", "cabello", "hair", "wig", "peruca"),
    ("Fashion", "Other Fashion"): ("fashion tips", "dicas de moda", "consejos de moda", "fashion style"),
    ("Fashion", "Fashion News"): ("fashion news", "notícias de moda", "noticias de moda", "fashion week"),
    ("Fashion", "Fashion & Accessories"): ("fashion accessories", "acessórios de moda", "accesorios de moda"),
    ("Fashion", "Footwear"): ("footwear", "shoes", "sapatos", "tênis", "tenis", "calçados", "calzado"),
    ("Fashion", "Bags & Luggage"): ("handbag", "bags", "bolsas", "bagagem", "maletas", "luggage"),
    ("Fashion", "Accessories"): ("accessories", "acessórios", "accesorios", "joias", "joyas", "jewelry"),
    ("Fashion", "Underwear & Loungewear"): ("underwear", "lingerie", "roupa íntima", "ropa interior", "loungewear"),
    ("Fashion", "Tattoo"): ("tattoo", "tatuagem", "tatuaje"),
    ("Parenting", "Parenting Life"): ("maternidade", "paternidade", "parenting", "mãe", "mom life"),
    ("Parenting", "Children"): ("criança", "crianças", "kids", "children", "bebê", "baby"),
    ("Parenting", "Maternity & Baby"): ("pregnancy", "gravidez", "embarazo", "maternity", "gestante", "newborn"),
    ("Food", "Cooking Tutorials"): (
        "receita", "receitas", "receta", "recetas", "recipe", "cooking", "cozinha", "cocina",
    ),
    ("Food", "Food Reviews"): ("food review", "provando", "taste test", "restaurante"),
    ("Food", "Mukbang"): ("mukbang", "asmr eating"),
    ("Food", "Wilderness Cooking"): ("wilderness cooking", "outdoor cooking", "cozinha na natureza", "cocina al aire libre"),
    ("Food", "Food Presentation"): ("food presentation", "plating", "empratamento", "decoração de pratos"),
    ("Food", "Food Knowledge"): ("food science", "food facts", "gastronomia", "conhecimento culinário", "historia de la comida"),
    ("Home", "DIY Crafts"): ("diy", "artesanato", "craft", "faça você mesmo"),
    ("Home", "Gardening & Flowers"): ("jardinagem", "garden", "plantas", "flowers"),
    ("Home", "Life Hacks"): ("life hack", "dicas de casa", "truques", "tips and tricks"),
    ("Home", "Furniture & Appliances"): ("furniture", "móveis", "moveis", "electrodomésticos", "appliances"),
    ("Home", "Interior Design"): ("interior design", "decoração", "decoracao", "diseño interior", "home decor"),
    ("Home", "Tools & Hardware"): ("tools", "ferramentas", "herramientas", "hardware store", "marcenaria", "construção civil", "engenharia civil"),
    ("Home", "Kitchen Appliances & Dining"): ("kitchen appliance", "eletrodoméstico", "electrodoméstico", "cookware", "utensílios de cozinha"),
    ("Casual Vlogs", "Lifestyle Vlogs"): ("vlog", "rotina", "daily life", "meu dia", "minha vida"),
    ("Casual Vlogs", "Photography"): ("photography", "fotografia", "camera", "câmera"),
    ("Casual Vlogs", "Portrait Clips"): ("portrait video", "retrato", "ensaio fotográfico", "portrait photography"),
    ("Casual Vlogs", "Screen Recordings"): ("screen recording", "gravação de tela", "grabación de pantalla", "screencast"),
    ("Music", "Singing"): (
        "music", "música", "musica", "song", "cover", "cantor", "cantora",
        "pagode", "rap", "funk", "nursery rhymes", "kids songs", "músicas infantis",
    ),
    ("Music", "Traditional Instruments"): ("violão", "guitar", "piano", "instrumental"),
    ("Music", "Western Instruments"): ("electric guitar", "drums", "bateria", "saxophone", "saxofone", "violin", "violino"),
    ("Music", "Music Knowledge"): ("music theory", "teoria musical", "história da música", "music production", "produção musical"),
    ("Dance", "Dance Styles"): ("dance", "dança", "danca", "coreografia", "choreography"),
    ("Dance", "Square Dance"): ("square dance", "quadrilha", "dança quadrada", "danza cuadrada"),
    ("Dance", "Hand Dance"): ("hand dance", "dança de mãos", "danca de maos", "finger dance"),
    # QY taxonomy has no dedicated animation branch. Anime and manga are
    # therefore represented by the closest valid culture category while their
    # specific meaning remains available through controlled channel tags.
    ("General Humanities & Society", "Arts & Culture"): (
        "history", "história", "cultura", "culture", "artes", "sociedade",
        "anime", "manga", "otaku", "paróquia", "homilia",
    ),
    ("Travel", "Travel Vlogs"): ("travel vlog", "viagem", "viaje", "tour", "turismo"),
    ("Travel", "Travel Guides"): ("travel guide", "guia de viagem", "o que fazer em", "itinerary"),
    ("Travel", "Tour Guide Content"): ("tour guide", "guia turístico", "guia turistico", "city tour", "walking tour"),
    ("Travel", "Travel Photography"): ("travel photography", "fotografia de viagem", "fotografía de viaje"),
    ("Travel", "Hotels & Stays"): ("hotel review", "hotel tour", "resort", "hospedagem", "alojamiento", "airbnb"),
    ("Pets & Animals", "Dogs"): ("dog", "dogs", "cachorro", "cachorros", "cão"),
    ("Pets & Animals", "Cats"): ("cat", "cats", "gato", "gatos"),
    ("Pets & Animals", "Other Animals"): ("wildlife", "animais selvagens", "fauna", "zoológico", "zoo animals"),
    ("Pets & Animals", "Animal Welfare"): ("animal rescue", "resgate animal", "bem-estar animal", "adoption", "adoção"),
    ("Pets & Animals", "Pet News"): ("pet news", "notícias pet", "noticias de mascotas"),
    ("Pets & Animals", "Other Pets"): (
        "hamster", "rabbit", "coelho", "bird pet", "pássaro", "pássaros",
        "birds", "canário", "réptil", "reptile",
    ),
    ("Pets & Animals", "Pet Supplies"): ("pet supplies", "pet shop", "ração", "ração para", "pet products"),
    ("Self Improvement", "Emotions & Psychology"): ("psicologia", "psychology", "ansiedade", "mental health", "relacionamento"),
    ("Self Improvement", "Career Skills"): (
        "career", "carreira", "emprego", "productivity", "produtividade",
        "empreendedorismo", "speech-language",
    ),
    ("Education", "K-12 Education"): ("educação", "education", "aula", "escola", "professor", "matemática"),
    ("Education", "Exams & Certifications"): ("concurso", "exam", "vestibular", "enem", "certification"),
    ("Education", "Primary & Secondary School"): ("ensino fundamental", "ensino médio", "primary school", "secondary school", "high school"),
    ("Education", "School News"): ("school news", "notícias da escola", "noticias escolares"),
    ("Education", "Other Campus Content"): (
        "campus", "universidade", "university", "faculdade", "college",
        "pronunciation", "english lesson", "aula de inglês", "how to pronounce",
    ),
    ("Education", "Campus Life"): ("campus life", "vida universitária", "vida universitaria", "college life"),
    ("Education", "Campus Activities"): ("campus activities", "atividade escolar", "student event", "evento universitário"),
    ("Tech", "Mobile Tech"): ("smartphone", "celular", "iphone", "android", "mobile tech"),
    ("Tech", "Computers & PCs"): ("computer", "computador", "pc gamer", "notebook", "laptop"),
    ("Tech", "Tech News"): ("tech news", "tecnologia", "technology", "inovação", "innovation"),
    ("Tech", "Consumer Electronics"): ("consumer electronics", "eletrônicos", "eletronicos", "electrónica de consumo"),
    ("Tech", "Gadgets & Devices"): ("gadgets", "gadget", "dispositivos", "wearable", "smartwatch"),
    ("Tech", "Digital Devices"): ("digital devices", "dispositivo digital", "tablet", "e-reader"),
    ("Tech", "Photography Equipment"): ("camera lens", "photography gear", "equipamento fotográfico", "câmera mirrorless", "dslr"),
    ("Gaming", "Strategy Games"): ("strategy game", "jogo de estratégia", "crusader kings", "eu4", "stellaris", "victoria 3"),
    ("Gaming", "Adventure Games"): (
        "adventure game", "jogo de aventura", "escape", "minecraft", "steam", "conquistas na steam",
    ),
    ("Gaming", "Action Games"): ("action game", "fps", "shooter", "fortnite", "free fire", "gta"),
    ("Gaming", "Role-Playing Games"): ("rpg", "role-playing", "role playing"),
    ("Gaming", "Casual Games"): ("roblox", "casual game", "gameplay", "jogando", "gaming"),
    ("Gaming", "Mobile Games"): ("mobile game", "jogo mobile", "free fire", "clash royale"),
    ("Gaming", "Gaming Hardware & Accessories"): ("gaming hardware", "gaming mouse", "gaming keyboard", "controle gamer", "headset gamer", "console review"),
    ("Health & Wellness", "Personal Health & Care"): ("saúde", "health", "bem-estar", "wellness", "medicina"),
    ("Health & Wellness", "Supplements"): ("suplemento", "supplement", "vitamina", "vitamin"),
    ("Health & Wellness", "First Aid Supplies"): ("first aid", "primeiros socorros", "primeros auxilios", "medical kit"),
    ("Health & Wellness", "Oral Care"): ("oral care", "saúde bucal", "salud dental", "dentista", "dental care"),
    ("Health & Wellness", "Sexual Health & Wellness"): ("sexual health", "saúde sexual", "salud sexual", "sex education"),
    ("Sports & Outdoors", "Ball Sports"): ("football", "futebol", "soccer", "basketball", "basquete", "volei", "vôlei"),
    ("Sports & Outdoors", "Fitness"): (
        "fitness", "academia", "workout", "treino", "musculação",
        "educação física", "educacao fisica", "physical education",
        "boxe", "boxing", "muay thai",
    ),
    ("Sports & Outdoors", "Fishing"): ("fishing", "pesca", "pescaria"),
    ("Sports & Outdoors", "Swimming & Water Sports"): ("swimming", "natação", "natacao", "surf", "diving", "mergulho"),
    ("Sports & Outdoors", "Winter Sports"): ("skiing", "snowboard", "winter sports", "esqui", "esportes de inverno"),
    ("Sports & Outdoors", "Cycling"): ("cycling", "ciclismo", "bicicleta", "bike ride", "mountain bike"),
    ("Sports & Outdoors", "Outdoor Adventure"): ("hiking", "trilha", "camping", "outdoor adventure", "escalada", "trekking"),
    ("Automotive", "Cars & Vehicles"): ("carro", "carros", "cars", "vehicle", "automotive", "garage"),
    ("Automotive", "Auto Repair"): ("mecânica", "mechanic", "auto repair", "conserto de carro"),
    ("Automotive", "Motorcycles"): ("moto", "motorcycle", "motocicleta"),
    ("Automotive", "Car Care & Styling"): ("car detailing", "estética automotiva", "car care", "lavagem automotiva", "car styling"),
    ("Automotive", "Auto Parts & Accessories"): ("auto parts", "peças automotivas", "acessórios automotivos", "car accessories"),
    ("Automotive", "Car Electronics"): ("car audio", "som automotivo", "car electronics", "multimídia automotiva", "dashcam"),
    ("Software & Internet", "Artificial Intelligence"): ("artificial intelligence", "inteligência artificial", "chatgpt", "machine learning", " ia "),
    ("Software & Internet", "Computer Software"): (
        "software", "programming", "programação", "python", "coding",
        "web hosting", "wordpress", "hospedagem de site",
    ),
    ("Software & Internet", "Mobile Apps"): ("app", "aplicativo", "mobile app"),
    ("Software & Internet", "Web3"): ("web3", "crypto", "bitcoin", "blockchain"),
}


def _compile_marker_pattern(markers: Iterable[str]) -> re.Pattern[str]:
    normalized = sorted(
        {normalize_text(marker) for marker in markers if normalize_text(marker)},
        key=lambda marker: (-len(marker), marker),
    )
    alternatives = "|".join(re.escape(marker) for marker in normalized)
    return re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)")


_CATEGORY_MARKER_KEYS: defaultdict[str, list[tuple[str, str]]] = defaultdict(list)
for _category_key, _category_markers in _CATEGORY_RULES.items():
    for _category_marker in _category_markers:
        _CATEGORY_MARKER_KEYS[normalize_text(_category_marker)].append(_category_key)
_CATEGORY_MARKER_PATTERN = _compile_marker_pattern(_CATEGORY_MARKER_KEYS)


def category_marker_values() -> tuple[str, ...]:
    """Return frozen weak-label triggers for leakage-resistant model training."""

    return tuple(sorted(_CATEGORY_MARKER_KEYS))


def _matched_marker_counts(text: str, pattern: re.Pattern[str]) -> Counter[str]:
    return Counter(normalize_text(match.group(0)) for match in pattern.finditer(text))


_BROAD_CATEGORY_MARKERS = {
    "music", "música", "musica", "gaming", "gameplay", "fashion", "moda",
    "travel", "tour", "education", "educação", "technology", "tecnologia",
    "health", "saúde", "fitness", "family", "família", "familia", "kids",
    "children", "camera", "câmera", "podcast", "tools", "app", "cultura",
    "song",
}


def _category_marker_weight(marker: str) -> float:
    if marker in _BROAD_CATEGORY_MARKERS:
        return 0.45
    if " " in marker or len(marker) >= 12:
        return 1.35
    return 1.0


_PE_PHRASE = re.compile(r"(?<!\w)(?:educa[cç][aã]o f[ií]sica|physical education)(?!\w)")
_SCHOOL_SUBJECT_PHRASE = re.compile(
    r"(?<!\w)(?:escola|enem|vestibular|matem[aá]tica|ensino (?:fundamental|m[eé]dio)|k-12)(?!\w)"
)
_GAMEPLAY_PHRASE = re.compile(
    r"(?<!\w)(?:gameplay|jogar|joguei|jogando|jogos?|game|games|gaming)(?!\w)"
)
_MOBILE_PLATFORM_PHRASE = re.compile(r"(?<!\w)(?:android|ios|iphone|celular)(?!\w)")
_HAIR_OR_MAKEUP_PHRASE = re.compile(
    r"(?<!\w)(?:maquiagem|makeup|skincare|tonalizante|tintura|cabelo|hair dye|hair care)(?!\w)"
)
_LANGUAGE_TEACHING_PHRASE = re.compile(
    r"(?<!\w)(?:pronunciation|pron[uú]ncia|speech-language|esl|"
    r"english (?:academy|lesson|sound|r sound)|aula de ingl[eê]s|how to pronounce)(?!\w)"
)
_RELIGIOUS_ORG_PHRASE = re.compile(
    r"(?<!\w)(?:par[oó]quia|homilia|missa ao vivo|nossa senhora)(?!\w)"
)
_CONSTRUCTION_INDUSTRY_PHRASE = re.compile(
    r"(?<!\w)(?:ind[uú]stria da constru[cç][aã]o|engenharia civil|constru[cç][aã]o civil)(?!\w)"
)
_WEB_HOSTING_PHRASE = re.compile(
    r"(?<!\w)(?:web hosting|hospedagem (?:de )?(?:site|sites|web)|wordpress|vps hosting)(?!\w)"
)
_KIDS_MUSIC_PHRASE = re.compile(
    r"(?<!\w)(?:nursery rhymes|kids songs|m[uú]sicas infantis|cantigas infantis)(?!\w)"
)
_F1_PHRASE = re.compile(r"(?<!\w)(?:f1|formula 1|fórmula 1|formula one)(?!\w)")
_STEAM_PHRASE = re.compile(r"(?<!\w)(?:steam|conquistas na steam|platinar)(?!\w)")
_ENTREPRENEURSHIP_PHRASE = re.compile(r"(?<!\w)empreendedorismo(?!\w)")


def _apply_category_boundary_adjustments(
    scores: defaultdict[tuple[str, str], float],
    combined_text: str,
    *,
    profile_text: str = "",
) -> None:
    """Keep keyword scores from crossing known taxonomy boundaries."""

    text = normalize_text(combined_text)
    profile = normalize_text(profile_text or combined_text)
    if _PE_PHRASE.search(text) and not _SCHOOL_SUBJECT_PHRASE.search(text):
        scores[("Education", "K-12 Education")] *= 0.15
        scores[("Education", "Other Campus Content")] *= 0.35
        scores[("Sports & Outdoors", "Fitness")] += 3.0
    gaming_score = sum(score for (level_1, _), score in scores.items() if level_1 == "Gaming")
    if _GAMEPLAY_PHRASE.search(text) and _MOBILE_PLATFORM_PHRASE.search(text) and gaming_score > 0:
        scores[("Tech", "Mobile Tech")] *= 0.12
        scores[("Tech", "Tech News")] *= 0.4
        scores[("Software & Internet", "Mobile Apps")] *= 0.35
    if _HAIR_OR_MAKEUP_PHRASE.search(text):
        scores[("Beauty Creators", "Beautiful Women")] *= 0.25
        scores[("Beauty Creators", "Handsome Men")] *= 0.25
    recipes = (
        scores[("Food", "Cooking Tutorials")]
        + scores[("Food", "Food Knowledge")]
        + scores[("Food", "Food Reviews")]
    )
    appliances = scores[("Home", "Kitchen Appliances & Dining")]
    if recipes > 0 and appliances > 0 and recipes >= appliances:
        scores[("Home", "Kitchen Appliances & Dining")] *= 0.35
    if _LANGUAGE_TEACHING_PHRASE.search(text):
        scores[("Pets & Animals", "Cats")] *= 0.08
        scores[("Pets & Animals", "Dogs")] *= 0.08
        scores[("Pets & Animals", "Other Pets")] *= 0.08
        scores[("Home", "Tools & Hardware")] *= 0.12
        scores[("Education", "Other Campus Content")] += 4.0
        scores[("Self Improvement", "Career Skills")] += 3.0
    if _RELIGIOUS_ORG_PHRASE.search(text):
        scores[("Parenting", "Parenting Life")] *= 0.12
        scores[("Parenting", "Children")] *= 0.12
        scores[("General Humanities & Society", "Arts & Culture")] += 4.0
    if _CONSTRUCTION_INDUSTRY_PHRASE.search(text):
        for key in list(scores):
            if key[0] == "Sports & Outdoors":
                scores[key] *= 0.08
        scores[("Home", "Tools & Hardware")] += 4.0
    if _WEB_HOSTING_PHRASE.search(text) or (
        re.search(r"(?<!\w)hospedagem(?!\w)", text)
        and re.search(r"(?<!\w)(?:site|web|wordpress|host)(?!\w)", text)
    ):
        scores[("Travel", "Hotels & Stays")] *= 0.12
        scores[("Software & Internet", "Computer Software")] += 4.0
    if _KIDS_MUSIC_PHRASE.search(text):
        scores[("Music", "Singing")] += 5.0
        scores[("Parenting", "Children")] *= 0.35
    if _F1_PHRASE.search(text):
        if re.search(r"(?<!\w)(?:racing|podcast|not[ií]cias|news|audi[eê]ncia)(?!\w)", text):
            scores[("Sports & Outdoors", "Outdoor Adventure")] += 6.0
            scores[("Automotive", "Cars & Vehicles")] *= 0.35
        else:
            scores[("Automotive", "Cars & Vehicles")] += 5.0
        scores[("Sports & Outdoors", "Fitness")] *= 0.2
    if re.search(
        r"(?<!\w)(?:animated films|movie scenes|boxoffice|escenas|pel[ií]culas?)(?!\w)",
        text,
    ):
        scores[("Pets & Animals", "Other Pets")] *= 0.08
        scores[("Pets & Animals", "Dogs")] *= 0.08
        scores[("Pets & Animals", "Cats")] *= 0.08
        scores[("Casual Vlogs", "Screen Recordings")] += 4.0
        scores[("General Humanities & Society", "Arts & Culture")] += 3.0
    if _ENTREPRENEURSHIP_PHRASE.search(profile) and not re.search(
        r"(?<!\w)(?:escola|concurso|professor|enem|vestibular)(?!\w)",
        profile,
    ):
        scores[("Self Improvement", "Career Skills")] += 8.0
        scores[("Fashion", "Clothing")] *= 0.2
        scores[("Automotive", "Cars & Vehicles")] *= 0.4
    if _STEAM_PHRASE.search(text):
        scores[("Gaming", "Adventure Games")] += 5.0
        scores[("Tech", "Mobile Tech")] *= 0.2
        scores[("Tech", "Tech News")] *= 0.35



def analyze_categories(
    snapshot: ChannelSnapshot,
    policy: AnalysisPolicy,
    *,
    channel_text: str | None = None,
    comments: CommentEvidence | None = None,
) -> tuple[FieldResult, dict[tuple[str, str], float]]:
    channel_text = channel_text if channel_text is not None else corpus(snapshot, include_content=False)
    scores: defaultdict[tuple[str, str], float] = defaultdict(float)
    profile_scores: defaultdict[tuple[str, str], float] = defaultdict(float)
    content_support: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    for marker, count in _matched_marker_counts(channel_text, _CATEGORY_MARKER_PATTERN).items():
        for key in _CATEGORY_MARKER_KEYS[marker]:
            contribution = min(3, count) * 3.0 * _category_marker_weight(marker)
            scores[key] += contribution
            profile_scores[key] += contribution
    for content in snapshot.contents:
        text = normalize_text(" ".join((content.title, *content.keywords, *content.hashtags)))
        if not text:
            continue
        published_at = content.published_at or content.first_seen_at
        age_days = (
            max(0.0, (snapshot.as_of - published_at).total_seconds() / 86400.0)
            if published_at else 365.0
        )
        recency = max(0.35, math.pow(0.5, age_days / 180.0))
        performance = 1.0 + min(0.35, math.log1p(content.view_count or 0) / 50.0)
        for marker, count in _matched_marker_counts(text, _CATEGORY_MARKER_PATTERN).items():
            for key in _CATEGORY_MARKER_KEYS[marker]:
                scores[key] += (
                    recency
                    * performance
                    * _category_marker_weight(marker)
                    * (1.0 + 0.25 * min(3, count - 1))
                )
                content_support[key].add(content.source_content_id)
    content_text = " ".join(
        " ".join((content.title, *content.keywords, *content.hashtags))
        for content in snapshot.contents
    )
    _apply_category_boundary_adjustments(
        scores,
        f"{channel_text} {content_text}",
        profile_text=channel_text,
    )
    scores = defaultdict(float, {key: score for key, score in scores.items() if score > 0})
    if not scores:
        if policy is AnalysisPolicy.EVIDENCE_FIRST:
            return unavailable("category_taxonomy_has_no_matching_signal"), scores
        value = {"level_1": "Uncategorized", "level_2": ["Uncategorized"]}
        return FieldResult(
            value=value,
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.0,
            evidence_confidence=0.1,
            evidence_refs=("taxonomy:no_rule_match",),
            model_version="category-controlled-rules-v1",
            decision_policy_version=POLICY_VERSION,
        ), scores
    level_scores: Counter[str] = Counter()
    for (level_1, _), score in scores.items():
        level_scores[level_1] += score
    ranked_levels = sorted(level_scores.items(), key=lambda item: (-item[1], item[0]))
    level_1, top_score = ranked_levels[0]
    level_2 = [
        subcategory
        for (parent, subcategory), _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))
        if parent == level_1
    ][:3]
    probability = top_score / sum(level_scores.values())
    top_keys = [key for key in scores if key[0] == level_1]
    matched_content_count = len({
        content_id for key in top_keys for content_id in content_support.get(key, set())
    })
    profile_support = sum(profile_scores.get(key, 0.0) for key in top_keys)
    comment_support = 0.0
    comment_reliability = 0.0
    comment_author_count = 0
    strong = bool(
        probability >= 0.65
        and (
            profile_support >= 7.0
            or (matched_content_count >= 3 and top_score >= 4.0)
            or (
                len(snapshot.contents) >= 2
                and matched_content_count == len(snapshot.contents)
                and top_score >= 2.5
            )
        )
    )
    if policy is AnalysisPolicy.EVIDENCE_FIRST and not strong:
        return unavailable("category_signal_is_weak_or_conflicted"), scores
    value = {"level_1": level_1, "level_2": level_2}
    return FieldResult(
        value=value,
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength="strong" if strong else "weak",
        model_confidence=round(probability, 6),
        evidence_confidence=round(min(0.9, top_score / 20), 6),
        candidates=tuple(
            {"value": name, "probability": round(score / sum(level_scores.values()), 6)}
            for name, score in ranked_levels[:5]
        ),
        evidence_refs=tuple(
            f"taxonomy_rule:{item}" for item in level_2
        ),
        model_version="category-controlled-coverage-rules-v6-boundaries",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "matched_content_count": matched_content_count,
            "content_count": len(snapshot.contents),
            "profile_support": round(profile_support, 6),
            "comment_support": round(comment_support, 6),
            "comment_reliability": round(comment_reliability, 6),
            "comment_sample_bias": None,
            "top_score": round(top_score, 6),
        },
    ), scores


_KNOWN_TAGS = {
    "Anime": ("anime", "animes",),
    "Anime Commentary": (
        "anime commentary", "anime review", "anime recap", "anime analysis",
        "comentário de anime", "comentario de anime", "análise de anime",
        "analise de anime", "resumo anime", "resumo do anime",
    ),
    "Anime Reactions": (
        "anime reaction", "anime reactions", "reagindo a anime",
        "reação a anime", "reacao a anime",
    ),
    "Manga": ("manga", "mangá", "mangás", "manhwa",),
    "Otaku Culture": ("otaku", "weeb", "cultura otaku",),
    "Football": ("football", "futebol", "soccer"),
    "News Commentary": ("news", "notícias", "noticias", "atualidades"),
    "Roblox": ("roblox",),
    "Gameplay": ("gameplay", "gaming", "jogando"),
    "Strategy Games": ("strategy", "estratégia", "stellaris", "crusader kings", "eu4"),
    "Cooking Tutorials": ("receita", "receitas", "receta", "recetas", "recipe", "cooking", "cozinha", "cocina"),
    "Makeup Tutorials": ("makeup", "maquiagem", "maquillaje"),
    "Skincare": ("skincare", "cuidados com a pele"),
    "Technology": ("technology", "tecnologia", "tech"),
    "Artificial Intelligence": ("artificial intelligence", "inteligência artificial", "chatgpt"),
    "Music": ("music", "música", "musica", "pagode", "song"),
    "Fitness": ("fitness", "workout", "treino", "academia"),
    "Travel": ("travel", "viagem", "turismo"),
    "Lifestyle Vlogs": ("vlog", "rotina", "daily life"),
    "Automotive": ("carro", "cars", "automotive", "garage"),
    "Education": ("education", "educação", "aula", "escola"),
    "Comedy": ("comedy", "comédia", "humor", "engraçado"),
    "Challenges": ("challenge", "desafio"),
    "Product Reviews": ("review", "reviews", "análise", "testando"),
    "Tutorials": ("tutorial", "como fazer", "how to"),
    "Mystery Stories": ("mystery", "mistério", "misterio", "misterio"),
    "Suspense Stories": ("suspense", "suspenso"),
    "Horror Stories": ("horror", "terror", "assombração", "assombracao"),
    "Family Life": ("family", "família", "familia"),
    "Storytelling": ("story", "stories", "história", "historia", "relato", "cuento"),
    "Reaction Videos": ("reaction", "reagindo", "reação", "reacao", "reacción", "reaccion"),
    "Documentary": ("documentary", "documentário", "documentario", "documental"),
    "Science Facts": ("science facts", "fatos científicos", "fatos cientificos"),
    "Curiosities": ("curiosities", "curiosidade", "curiosidades", "curioso"),
    "True Crime": ("true crime", "crime real", "crimes reais"),
    "3 AM Challenges": ("3am", "3 am", "três da manhã", "tres da manha"),
    "Brazilian Politics": (
        "política brasileira", "politica brasileira", "brazilian politics",
        "congresso nacional", "governo brasileiro",
    ),
    "Current Events": ("current events", "acontecimentos atuais", "notícias de hoje", "noticias de hoje"),
    "Minecraft": ("minecraft", "jogo de bloco quadrado"),
    "Minecraft Series": ("minecraft series", "série de minecraft", "serie de minecraft"),
    "Creative Squad": ("creative squad", "creative squad cs", "criativi squad"),
    "Film Production": (
        "film production", "video production", "produção de filmes", "producao de filmes",
        "produção de vídeo", "produção de videos", "producao de video", "produtora de vídeos",
        "produtora de videos",
    ),
    "Audiovisual Services": (
        "audiovisual services", "serviços audiovisuais", "servicos audiovisuais",
        "produção audiovisual", "producao audiovisual",
    ),
    "Video Editing": ("video editing", "edição de vídeo", "edição de videos", "edicao de video"),
    "Commercial Films": ("commercial film", "commercial films", "filme publicitário", "filme publicitario"),
    "Event Videos": ("event video", "event videos", "vídeo de evento", "videos de eventos", "pre-wedding"),
    "Media Company": ("media company", "produtora de vídeo", "produtora de videos"),
    "Hair Care": ("hair care", "cuidados com o cabelo", "cuidado capilar", "tratamento capilar"),
    "Hair Care Products": (
        "hair care products", "produtos para cabelo", "produtos capilares", "shampoo", "condicionador",
    ),
    "Curl Definition Routines": (
        "curl definition", "definição de cachos", "definicao de cachos", "cabelo cacheado",
    ),
    "Hair Styling Tutorials": ("hair styling", "penteado", "tutorial de cabelo", "tutorial capilar"),
    "Natural Hair Care": ("natural hair care", "cabelo natural", "transição capilar", "transicao capilar"),
    "Women Fitness": ("women fitness", "fitness feminino", "treino feminino", "exercício para mulheres"),
    "Home Workouts": ("home workout", "treino em casa", "exercício em casa", "exercicios em casa"),
    "Weight Loss for Women": (
        "weight loss for women", "emagrecimento feminino", "emagrecer para mulheres",
    ),
    "Body Transformation": ("body transformation", "transformação corporal", "transformacao corporal"),
}

# Every controlled level-2 category is also a valid tag. Reusing the same
# multilingual marker catalog prevents the category and tag systems from
# drifting into different meanings.
for (_tag_parent, _tag_subcategory), _tag_markers in _CATEGORY_RULES.items():
    _KNOWN_TAGS.setdefault(_tag_subcategory, _tag_markers)

_PRESENTATION_TAGS = {
    "Video Commentary": ("commentary", "comentário", "comentario", "opinião", "opinion", "analisando"),
    "How-to Content": ("how to", "como fazer", "passo a passo", "step by step", "tutorial"),
    "Educational Content": ("explained", "explicando", "aprenda", "learn", "aula", "lesson", "facts"),
    "Product Demonstrations": ("unboxing", "demonstração", "demonstration", "testando", "hands on"),
    "Community Discussions": ("podcast", "interview", "entrevista", "debate", "q&a", "perguntas e respostas"),
    "Digital Culture": ("meme", "viral", "internet", "tiktok", "trend", "trending"),
    "Channel Series": ("episode", "episódio", "episodio", "parte", "part", "capítulo", "capitulo", " ep "),
}

_CONTROLLED_FORMAT_TAGS = (
    "Long-form Video",
    "Short-form Video",
    "Live Streaming",
    "Storytelling",
    "Video Commentary",
    "How-to Content",
    "Educational Content",
    "Product Demonstrations",
    "Community Discussions",
    "Digital Culture",
    "Channel Series",
)
_FORMAT_ONLY_TAGS = {"Long-form Video", "Short-form Video", "Live Streaming"}
_FORMAT_TAG_SET = set(_CONTROLLED_FORMAT_TAGS)
_FORMAT_TAG_RELEVANCE_FACTOR = 0.25


def _tag_sort_key(tag: str, score: float) -> tuple[float, str]:
    factor = _FORMAT_TAG_RELEVANCE_FACTOR if tag in _FORMAT_TAG_SET else 1.0
    return (-score * factor, tag.casefold())


def _rank_controlled_tags(
    scores: Counter[str],
    supported_tags: set[str],
    fallback_tags: Sequence[str],
    *,
    limit: int = 10,
) -> list[str]:
    """Prefer evidenced topics over format/filler tags in the first five slots."""

    fallback_set = set(fallback_tags)
    ranked = [
        tag
        for tag, _ in sorted(scores.items(), key=lambda item: _tag_sort_key(item[0], item[1]))
    ]
    topic_supported = [
        tag for tag in ranked if tag in supported_tags and tag not in _FORMAT_TAG_SET
    ]
    topic_other = [
        tag
        for tag in ranked
        if tag not in supported_tags
        and tag not in _FORMAT_TAG_SET
        and tag not in fallback_set
    ]
    format_supported = [
        tag for tag in ranked if tag in _FORMAT_TAG_SET and tag in supported_tags
    ]
    fillers = [
        tag
        for tag in ranked
        if tag not in topic_supported
        and tag not in topic_other
        and tag not in format_supported
    ]
    top_five = (topic_supported + topic_other)[:5]
    if len(top_five) < 5:
        top_five.extend(format_supported[: 5 - len(top_five)])
    if len(top_five) < 5:
        top_five.extend(tag for tag in fillers if tag not in top_five)
        top_five = top_five[:5]
    selected = list(top_five)
    for pool in (topic_supported, topic_other, format_supported, fillers):
        for tag in pool:
            if tag not in selected:
                selected.append(tag)
            if len(selected) == limit:
                return selected
    return selected[:limit]


def controlled_tag_names() -> tuple[str, ...]:
    category_names = {
        value
        for level_1, level_2 in CHANNEL_CATEGORY_TREE.items()
        for value in (level_1, *level_2)
        if value != "Uncategorized"
    }
    fixed = {*_KNOWN_TAGS, *_CONTROLLED_FORMAT_TAGS}
    return tuple(sorted(category_names | fixed))


_CONTROLLED_TAG_PROJECTION_CACHE: tuple[
    tuple[tuple[re.Pattern[str], tuple[str, ...]], ...],
    tuple[tuple[re.Pattern[str], tuple[tuple[str, str], ...]], ...],
    tuple[tuple[str, re.Pattern[str]], ...],
] | None = None


def project_tags_to_controlled_vocabulary(values: Iterable[str]) -> tuple[str, ...]:
    """Project free-form reference tags for evaluation, never for training labels."""

    global _CONTROLLED_TAG_PROJECTION_CACHE
    vocabulary = {normalize_text(name): name for name in controlled_tag_names()}
    projected: set[str] = set()
    if _CONTROLLED_TAG_PROJECTION_CACHE is None:
        known_patterns = tuple(
            (_compile_marker_pattern((marker,)), tuple(tags))
            for marker, tags in _KNOWN_MARKER_TAGS.items()
        )
        category_patterns = tuple(
            (_compile_marker_pattern((marker,)), tuple(category_keys))
            for marker, category_keys in _CATEGORY_MARKER_KEYS.items()
        )
        format_patterns = tuple(
            (name, _compile_marker_pattern(markers))
            for name, markers in {
                "Short-form Video": ("short", "shorts"),
                "Live Streaming": ("live", "stream", "streaming"),
                "Video Commentary": ("commentary", "comentário", "comentario"),
                "Storytelling": ("story", "stories", "história", "historia", "cuento"),
            }.items()
        )
        _CONTROLLED_TAG_PROJECTION_CACHE = (
            known_patterns,
            category_patterns,
            format_patterns,
        )
    known_patterns, category_patterns, format_patterns = _CONTROLLED_TAG_PROJECTION_CACHE
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        exact = vocabulary.get(text)
        if exact:
            projected.add(exact)
        for pattern, tags in known_patterns:
            if pattern.search(text):
                projected.update(tags)
        for pattern, category_keys in category_patterns:
            if pattern.search(text):
                for level_1, level_2 in category_keys:
                    projected.update((level_1, level_2))
        for name, pattern in format_patterns:
            if pattern.search(text):
                projected.add(name)
    return tuple(sorted(projected))


_KNOWN_MARKER_TAGS: defaultdict[str, list[str]] = defaultdict(list)
for _known_tag, _known_markers in _KNOWN_TAGS.items():
    for _known_marker in _known_markers:
        _KNOWN_MARKER_TAGS[normalize_text(_known_marker)].append(_known_tag)
_KNOWN_TAG_PATTERN = _compile_marker_pattern(_KNOWN_MARKER_TAGS)
_TAG_STOPWORDS = {
    "video", "videos", "canal", "channel", "official", "oficial", "shorts", "youtube",
    "para", "com", "uma", "the", "and", "from", "this", "that", "sobre", "novo",
}


def _clean_tag(value: str) -> str | None:
    cleaned = re.sub(r"[_#]+", " ", str(value)).strip(" -_/.,")
    cleaned = re.sub(r"\s+", " ", cleaned)
    normalized = normalize_text(cleaned)
    if not 3 <= len(cleaned) <= 48 or normalized in _TAG_STOPWORDS or cleaned.isdigit():
        return None
    if re.match(r"^(?:https?://|www\.)", cleaned, re.I) or "@" in cleaned:
        return None
    return " ".join(part.capitalize() if part.islower() else part for part in cleaned.split())


def analyze_tags(
    snapshot: ChannelSnapshot,
    categories: FieldResult,
    language: FieldResult,
    policy: AnalysisPolicy,
    *,
    full_text: str | None = None,
    comments: CommentEvidence | None = None,
) -> FieldResult:
    full_text = full_text if full_text is not None else corpus(snapshot)
    scores: Counter[str] = Counter()
    support_sources: defaultdict[str, set[str]] = defaultdict(set)
    marker_by_tag: dict[str, tuple[str, ...]] = dict(_KNOWN_TAGS)
    channel_text = corpus(snapshot, include_content=False)
    category_value = categories.value if isinstance(categories.value, dict) else None
    if category_value and category_value.get("level_1") != "Uncategorized":
        category_support = 1.0 if categories.evidence_strength in {"explicit", "strong"} else 0.25
        for index, tag in enumerate(category_value["level_2"]):
            # Category output is contextual evidence, not independent topic
            # observation. Keep it as a weak ranking prior so direct recent text
            # can correct a stale or conflicting category prediction.
            scores[tag] += max(0.5, 2.5 - index * 0.25) * category_support
            support_sources[tag].add(f"category:{categories.evidence_strength}")
            taxonomy_markers = next(
                (
                    markers for (parent, subcategory), markers in _CATEGORY_RULES.items()
                    if parent == category_value["level_1"] and subcategory == tag
                ),
                (normalize_text(tag),),
            )
            marker_by_tag[tag] = taxonomy_markers
        scores[category_value["level_1"]] += 1.0 * category_support
        support_sources[category_value["level_1"]].add(f"category:{categories.evidence_strength}")
        branch_markers = tuple(
            marker
            for (parent, _), markers in _CATEGORY_RULES.items()
            if parent == category_value["level_1"]
            for marker in markers
        )
        if branch_markers:
            marker_by_tag[category_value["level_1"]] = branch_markers

    for marker, count in _matched_marker_counts(channel_text, _KNOWN_TAG_PATTERN).items():
        for tag in _KNOWN_MARKER_TAGS[marker]:
            scores[tag] += min(9.0, count * 3.0 * _category_marker_weight(marker))
            support_sources[tag].add("channel_profile")

    comment_support_counts: Counter[str] = Counter()
    comment_reliability = 0.0
    if comments and comments.topic_text:
        unique_authors = comments.numeric["comment_unique_author_count"]
        comment_reliability = (
            unique_authors / (unique_authors + 25.0)
            * max(0.25, comments.numeric["comment_meaningful_ratio"])
        )
        for marker, count in _matched_marker_counts(
            comments.topic_text,
            _KNOWN_TAG_PATTERN,
        ).items():
            for tag in _KNOWN_MARKER_TAGS[marker]:
                comment_support_counts[tag] += count
                scores[tag] += (
                    min(5, count)
                    * 0.8
                    * _category_marker_weight(marker)
                    * comment_reliability
                )
                support_sources[tag].add("comment_topic")

    presentation_patterns = {
        tag: _compile_marker_pattern(markers) for tag, markers in _PRESENTATION_TAGS.items()
    }
    for tag, markers in _PRESENTATION_TAGS.items():
        marker_by_tag[tag] = markers
        if comments and comments.topic_text:
            count = len(presentation_patterns[tag].findall(comments.topic_text))
            if count:
                comment_support_counts[tag] += count
                scores[tag] += min(5, count) * 0.55 * comment_reliability
                support_sources[tag].add("comment_topic")

    for content in snapshot.contents:
        text = normalize_text(" ".join((content.title, *content.keywords, *content.hashtags)))
        published_at = content.published_at or content.first_seen_at
        age_days = (
            max(0.0, (snapshot.as_of - published_at).total_seconds() / 86400.0)
            if published_at else 365.0
        )
        recency = max(0.25, math.pow(0.5, age_days / 120.0))
        performance = 1.0 + min(0.5, math.log1p(content.view_count or 0) / 35.0)
        for marker, count in _matched_marker_counts(text, _KNOWN_TAG_PATTERN).items():
            for tag in _KNOWN_MARKER_TAGS[marker]:
                scores[tag] += recency * performance * _category_marker_weight(marker) * min(2, count)
                support_sources[tag].add(f"content:{content.source_content_id}")
        for tag, pattern in presentation_patterns.items():
            if pattern.search(text):
                scores[tag] += recency * performance
                support_sources[tag].add(f"content:{content.source_content_id}")

    content_types = Counter(content.content_type for content in snapshot.contents)
    total_content = sum(content_types.values())
    if total_content:
        short_share = content_types["short"] / total_content
        live_share = content_types["live"] / total_content
        long_share = content_types["video"] / total_content
        if short_share >= 0.2:
            scores["Short-form Video"] += 1.0 + 1.5 * short_share
            support_sources["Short-form Video"].add("observed_content_format")
            marker_by_tag["Short-form Video"] = ("short", "shorts")
        if live_share >= 0.1:
            scores["Live Streaming"] += 1.0 + 1.5 * live_share
            support_sources["Live Streaming"].add("observed_content_format")
            marker_by_tag["Live Streaming"] = ("live", "ao vivo")
        if long_share >= 0.2:
            scores["Long-form Video"] += 1.0 + 1.5 * long_share
            support_sources["Long-form Video"].add("observed_content_format")
            marker_by_tag["Long-form Video"] = ("video",)

    if not scores and policy is AnalysisPolicy.EVIDENCE_FIRST:
        return unavailable("channel_tags_have_no_content_signal")
    # A subject mentioned by one isolated upload is not automatically a stable
    # channel topic. This matters for channels whose stories happen to mention
    # hair, tattoos, cars, or another taxonomy marker once. Small channels keep
    # their only observation; channels with a useful history require repetition
    # unless profile, format, or strong category evidence independently supports
    # the tag.
    minimum_stable_content_mentions = 1 if len(snapshot.contents) <= 5 else 2
    content_support_counts = {
        tag: sum(source.startswith("content:") for source in sources)
        for tag, sources in support_sources.items()
    }

    def has_independent_support(tag: str, sources: set[str]) -> bool:
        return bool(
            sources & {
                "channel_profile",
                "observed_content_format",
                "category:explicit",
                "category:strong",
            }
            or comment_support_counts.get(tag, 0) >= 2
        )

    for tag, sources in tuple(support_sources.items()):
        content_mentions = content_support_counts.get(tag, 0)
        if (
            content_mentions
            and content_mentions < minimum_stable_content_mentions
            and not has_independent_support(tag, sources)
            and all(source.startswith("content:") for source in sources)
        ):
            scores.pop(tag, None)

    supported_tags = {
        tag
        for tag, sources in support_sources.items()
        if scores.get(tag, 0.0) > 0
        and (
            has_independent_support(tag, sources)
            or content_support_counts.get(tag, 0) >= minimum_stable_content_mentions
        )
    }
    fallback_category = (
        category_value["level_1"]
        if category_value and category_value.get("level_1") != "Uncategorized"
        else "Digital Culture"
    )
    observed_formats = [
        name for name, kind in (
            ("Short-form Video", "short"),
            ("Long-form Video", "video"),
            ("Live Streaming", "live"),
        )
        if content_types[kind]
    ]
    fallback = [
        fallback_category,
        *observed_formats,
        "Digital Culture",
        "Channel Series",
        "Video Commentary",
        "Educational Content",
        "How-to Content",
        "Community Discussions",
        "Product Demonstrations",
        "Storytelling",
        "Short-form Video",
        "Long-form Video",
        "Live Streaming",
    ]
    fallback_tags: list[str] = []
    for index, tag in enumerate(fallback):
        if tag in scores:
            continue
        scores[tag] += max(0.000001, 0.0001 - index * 0.000005)
        fallback_tags.append(tag)
        marker_by_tag.setdefault(tag, (normalize_text(tag),))
    merged_scores: Counter[str] = Counter()
    canonical_names: dict[str, str] = {}
    for tag, score in scores.items():
        folded = tag.casefold()
        canonical = canonical_names.setdefault(folded, tag)
        merged_scores[canonical] += score
    tags = _rank_controlled_tags(merged_scores, supported_tags, fallback_tags)
    tag_patterns = {
        tag: _compile_marker_pattern(marker_by_tag.get(tag, (normalize_text(tag),)))
        for tag in tags
    }

    attribution = {tag: 0.0 for tag in tags}
    other = 0.0
    for content in snapshot.contents:
        text = normalize_text(" ".join((content.title, content.description, *content.keywords, *content.hashtags)))
        age_days = max(0.0, (snapshot.as_of - (content.published_at or snapshot.as_of)).total_seconds() / 86400)
        recency = math.pow(0.5, age_days / 90.0)
        performance = 1.0 + min(3.0, math.log1p(content.view_count or 0) / 8.0)
        content_weight = recency * performance
        content_kind = content.content_type
        format_match = {
            "short": "Short-form Video",
            "live": "Live Streaming",
            "video": "Long-form Video",
        }[content_kind]
        matched = [tag for tag in tags if tag_patterns[tag].search(text)]
        if format_match in tags and format_match not in matched:
            matched.append(format_match)
        if matched:
            for tag in matched:
                attribution[tag] += content_weight / len(matched)
        else:
            other += content_weight
    if not snapshot.contents:
        for tag in tags:
            attribution[tag] = merged_scores[tag]
        other = max(1.0, sum(attribution.values()) * 0.1)
    tag_signal = {
        tag: attribution[tag] + max(0.000001, merged_scores[tag] * 0.15)
        for tag in tags
    }
    relevance_signal = {
        tag: tag_signal[tag] * (
            _FORMAT_TAG_RELEVANCE_FACTOR if tag in _FORMAT_TAG_SET else 1.0
        )
        for tag in tags
    }
    tags = _rank_controlled_tags(
        Counter({tag: relevance_signal[tag] for tag in tags}),
        supported_tags,
        fallback_tags,
    )
    top_five = tags[:5]
    other += sum(attribution[tag] for tag in tags[5:])
    for tag in top_five:
        attribution[tag] = relevance_signal[tag]
    weights = {tag: attribution[tag] for tag in top_five}
    weights["Other"] = max(0.25, other)
    percentages = apportion(weights, order=[*top_five, "Other"])
    # Evidence tiers decide which topics qualify for the first five slots, but
    # they can place a lower-weight evidenced topic ahead of a higher-weight
    # topic. The legacy Agent contract requires those five output rows to be
    # ordered by their final integer percentages, so reorder only within the
    # already-selected set after Hamilton apportionment.
    top_five_rank = {tag: index for index, tag in enumerate(top_five)}
    top_five = sorted(
        top_five,
        key=lambda tag: (-percentages[tag], top_five_rank[tag]),
    )
    tags = [*top_five, *tags[5:]]
    value = {
        "tags": tags,
        "top_5_distribution": [
            {"tag": tag, "percentage": percentages[tag]} for tag in [*top_five, "Other"]
        ],
    }
    supported_in_output = [tag for tag in tags if tag in supported_tags]
    strong = len(supported_in_output) >= 5 and (
        len(snapshot.contents) >= 5 or categories.evidence_strength in {"explicit", "strong"}
    )
    return FieldResult(
        value=value,
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength="strong" if strong else "weak",
        model_confidence=min(0.9, 0.3 + 0.08 * len(supported_in_output)),
        evidence_confidence=min(0.9, 0.15 + 0.1 * len(supported_in_output)),
        evidence_refs=tuple(
            (f"content_count:{len(snapshot.contents)}", "controlled_tag_rules:v5-topic-first")
            + ((f"comment_topic_authors:{int(comments.numeric['comment_unique_author_count'])}",)
               if comments and comments.topic_text else ())
        ),
        model_version="channel-tags-controlled-english-v5-topic-first",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "supported_tags": supported_in_output,
            "fallback_tags": [tag for tag in tags if tag not in supported_tags],
            "supported_tag_count": len(supported_in_output),
            "content_support_counts": {
                tag: content_support_counts.get(tag, 0)
                for tag in tags
                if content_support_counts.get(tag, 0)
            },
            "comment_support_counts": {
                tag: comment_support_counts[tag]
                for tag in tags
                if comment_support_counts[tag]
            },
            "comment_reliability": round(comment_reliability, 6),
            "comment_sample_bias": "top_comments_first_page" if comments and comments.has_comments else None,
            "minimum_stable_content_mentions": minimum_stable_content_mentions,
            "language_context": language.value,
            "unmatched_content_weight": round(other, 6),
        },
    )


_AUDIENCE_REGION_PATTERNS = {
    "Brazil": (r"(?<!\w)(?:brasil|brazil|brasileir[oa]s?)(?!\w)",),
    "Mexico": (r"(?<!\w)(?:m[eé]xico|mexican[oa]s?)(?!\w)",),
    "United States": (r"(?<!\w)(?:united states|estados unidos|usa|u\.s\.a\.)(?!\w)",),
    "Portugal": (r"(?<!\w)(?:portugal|portugu[eê]s(?:es|as)?)(?!\w)",),
    "Spain": (r"(?<!\w)(?:espa[nñ]a|spanish|espa[nñ]ol(?:es|as)?)(?!\w)",),
    "Argentina": (r"(?<!\w)(?:argentina|argentin[oa]s?)(?!\w)",),
    "Colombia": (r"(?<!\w)(?:colombia|colombian[oa]s?)(?!\w)",),
    "India": (r"(?<!\w)(?:india|indian)(?!\w)",),
    "France": (r"(?<!\w)(?:france|fran[cç]a|fran[cç]ais)(?!\w)",),
    "Germany": (r"(?<!\w)(?:germany|alemanha|deutschland)(?!\w)",),
    "Japan": (r"(?<!\w)(?:japan|jap[aã]o|日本)(?!\w)",),
    "South Korea": (r"(?<!\w)(?:south korea|coreia do sul|대한민국)(?!\w)",),
    "China": (r"(?<!\w)(?:china|中国|中國)(?!\w)",),
    "Taiwan": (r"(?<!\w)(?:taiwan|台灣|台湾|臺灣)(?!\w)",),
    "Hong Kong": (r"(?<!\w)(?:hong kong|香港)(?!\w)",),
}


def _content_format_shares(snapshot: ChannelSnapshot | None) -> dict[str, float]:
    if snapshot is None or not snapshot.contents:
        return {"short": 0.0, "longform": 0.0, "live": 0.0}
    counts: Counter[str] = Counter()
    for content in snapshot.contents:
        content_format = "longform" if content.content_type == "video" else content.content_type
        counts[content_format] += 1
    total = sum(counts.values()) or 1
    return {name: counts[name] / total for name in ("short", "longform", "live")}


def _shrunken_comment_weight(maximum: float, observations: float, shrinkage: float) -> float:
    observations = max(0.0, float(observations))
    return max(0.0, min(1.0, float(maximum))) * observations / (
        observations + max(1e-9, float(shrinkage))
    )


def _blend_probabilities(
    baseline: dict[str, float],
    observed: dict[str, float],
    observed_weight: float,
) -> dict[str, float]:
    base = normalize_weights(baseline)
    signal = normalize_weights(observed)
    weight = max(0.0, min(1.0, observed_weight)) if signal else 0.0
    return normalize_weights({
        key: (1.0 - weight) * base.get(key, 0.0) + weight * signal.get(key, 0.0)
        for key in set(base) | set(signal)
    })


def analyze_audience_markets(
    language_probabilities: dict[str, float],
    country: FieldResult,
    catalog: PriorCatalog,
    policy: AnalysisPolicy,
    *,
    snapshot: ChannelSnapshot | None = None,
    category: FieldResult | None = None,
    tags: FieldResult | None = None,
    comments: CommentEvidence | None = None,
) -> tuple[FieldResult, FieldResult]:
    if policy is AnalysisPolicy.EVIDENCE_FIRST and not catalog.production_eligible:
        reason = "audience_market_prior_is_not_production_eligible"
        return unavailable(reason), unavailable(reason)
    if not language_probabilities:
        return unavailable("audience_market_has_no_creator_language"), unavailable("audience_market_has_no_creator_language")

    config = catalog.audience_market()
    format_shares = _content_format_shares(snapshot)
    category_name = (
        str(category.value.get("level_1"))
        if category and isinstance(category.value, dict)
        else "default"
    )
    category_reach = catalog.category_cross_language_reach(category_name)
    format_reach = sum(
        format_shares[name] * float(config["format_cross_language_reach"].get(name, 0.0))
        for name in format_shares
    )
    cross_language_reach = max(0.0, min(1.0, category_reach + format_reach))

    joint: defaultdict[tuple[str, str], float] = defaultdict(float)
    creator_country = country.value if country.value and country.evidence_strength != "prior_only" else None
    for language, language_probability in language_probabilities.items():
        markets = normalize_weights(catalog.language_market(language))
        for region, market_weight in markets.items():
            joint[(region, language)] += language_probability * market_weight

    creator_blend = 0.0
    if creator_country:
        creator_blend = float(
            config["creator_country_blend"].get(country.evidence_strength, 0.0)
        ) * (1.0 - 0.35 * cross_language_reach)
        for key in list(joint):
            joint[key] *= 1.0 - creator_blend
        for language, language_probability in language_probabilities.items():
            joint[(str(creator_country), language)] += creator_blend * language_probability

    topic_hits: Counter[str] = Counter()
    if snapshot is not None:
        topic_text = normalize_text(" ".join(
            " ".join((content.title, *content.hashtags, *content.keywords))
            for content in snapshot.contents
        ))
        for region, patterns in _AUDIENCE_REGION_PATTERNS.items():
            topic_hits[region] = min(3, sum(len(re.findall(pattern, topic_text, re.I)) for pattern in patterns))
        topic_hits += Counter({key: 0 for key in _AUDIENCE_REGION_PATTERNS})
    positive_topic_hits = {key: value for key, value in topic_hits.items() if value > 0}
    topic_blend = min(
        float(config["regional_topic_max_blend"]),
        0.015 * sum(positive_topic_hits.values()),
    )
    if topic_blend:
        topic_distribution = normalize_weights(positive_topic_hits)
        for key in list(joint):
            joint[key] *= 1.0 - topic_blend
        for region, region_probability in topic_distribution.items():
            for language, language_probability in language_probabilities.items():
                joint[(region, language)] += topic_blend * region_probability * language_probability

    normalized_joint = normalize_weights({f"{region}\0{language}": value for (region, language), value in joint.items()})
    region_weights: Counter[str] = Counter()
    language_weights: Counter[str] = Counter()
    for key, weight in normalized_joint.items():
        region, content_language = key.split("\0", 1)
        region_weights[region] += weight
        propagation = normalize_weights(catalog.language_propagation(content_language))
        # "Other" geography means the country is unresolved; it is not evidence
        # that the audience speaks an unknown language. Preserve content-language
        # propagation for that bucket instead of manufacturing a large Other
        # language share.
        local_languages = (
            propagation
            if region == "Other"
            else normalize_weights(catalog.region_languages(region))
        )
        bridge_weight = min(
            0.18,
            float(config["bridge_language_weight"]) + 0.08 * cross_language_reach,
        )
        region_weight = float(config["region_language_weight"]) * (1.0 - 0.35 * cross_language_reach)
        content_weight = max(0.0, 1.0 - bridge_weight - region_weight)
        for language, probability in propagation.items():
            language_weights[language] += weight * content_weight * probability
        for language, probability in local_languages.items():
            language_weights[language] += weight * region_weight * probability
        language_weights[str(config["bridge_language"])] += weight * bridge_weight

    comment_config = catalog.comment_evidence()["audience_market"]
    comment_language_blend = 0.0
    comment_region_blend = 0.0
    if comments and comments.language.probabilities:
        comment_language_blend = _shrunken_comment_weight(
            float(comment_config["language_max_blend"]),
            comments.language.source_count,
            float(comment_config["language_shrinkage_authors"]),
        )
        language_weights = Counter(_blend_probabilities(
            dict(language_weights),
            comments.language.probabilities,
            comment_language_blend,
        ))
    if comments and comments.region_probabilities:
        explicit_count = comments.numeric["comment_explicit_region_author_count"]
        dialect_count = comments.numeric["comment_dialect_region_author_count"]
        effective_region_authors = explicit_count + 0.25 * dialect_count
        comment_region_blend = _shrunken_comment_weight(
            float(comment_config["region_max_blend"]),
            effective_region_authors,
            float(comment_config["region_shrinkage_authors"]),
        )
        region_weights = Counter(_blend_probabilities(
            dict(region_weights),
            comments.region_probabilities,
            comment_region_blend,
        ))

    ranked_regions = [(key, value) for key, value in sorted(region_weights.items(), key=lambda item: (-item[1], item[0])) if key != "Other"]
    selected_regions = ranked_regions[:5]
    selected_names = [name for name, _ in selected_regions]
    region_output_weights = {name: weight for name, weight in selected_regions}
    region_output_weights["Other"] = max(
        0.0,
        1.0 - sum(region_output_weights.values()),
    )
    if len(selected_names) < 5:
        fallback_names = [
            name for language in language_probabilities for name in catalog.language_market(language)
            if name != "Other" and name not in region_output_weights
        ]
        for name in fallback_names:
            if len(selected_names) >= 5:
                break
            selected_names.append(name)
            region_output_weights[name] = 0.000001
    region_order = [*selected_names[:5], "Other"]
    region_percent = apportion({name: region_output_weights.get(name, 0.0) for name in region_order}, order=region_order)
    region_value = [{"region": name, "percentage": region_percent[name]} for name in region_order]

    ranked_languages = sorted(language_weights.items(), key=lambda item: (-item[1], item[0]))
    selected_languages = [
        name for name, weight in ranked_languages
        if name != "Other" and weight >= 0.02
    ][:3]
    if not selected_languages:
        selected_languages = [ranked_languages[0][0]]
    other_language_weight = max(
        0.0,
        1.0 - sum(language_weights[name] for name in selected_languages),
    )
    language_output_weights = {
        name: language_weights[name] for name in selected_languages
    }
    if selected_languages and other_language_weight > language_output_weights[selected_languages[-1]]:
        capped_other = language_output_weights[selected_languages[-1]]
        overflow = other_language_weight - capped_other
        selected_total = sum(language_output_weights.values()) or 1.0
        for name in selected_languages:
            language_output_weights[name] += overflow * language_output_weights[name] / selected_total
        other_language_weight = capped_other
    if other_language_weight > 0:
        language_output_weights["Other"] = other_language_weight
        language_order = [*selected_languages, "Other"]
    else:
        language_order = selected_languages
    language_percent = apportion(language_output_weights, order=language_order)
    language_value = [{"language": name, "percentage": language_percent[name]} for name in language_order]

    metadata = {
        "calibration_status": "uncalibrated_public_estimate",
        "prior_production_eligible": catalog.production_eligible,
        "joint_market_model": True,
        "creator_country_blend": round(creator_blend, 6),
        "regional_topic_blend": round(topic_blend, 6),
        "regional_topic_hits": dict(positive_topic_hits),
        "category_cross_language_reach": round(category_reach, 6),
        "format_cross_language_reach": round(format_reach, 6),
        "content_format_shares": {key: round(value, 6) for key, value in format_shares.items()},
        "country_language_propagation_applied": True,
        "tag_context_count": len((tags.metadata.get("supported_tags") or [])) if tags else 0,
        "comment_language_blend": round(comment_language_blend, 6),
        "comment_region_blend": round(comment_region_blend, 6),
        "comment_language_author_count": comments.language.source_count if comments else 0,
        "comment_region_probabilities": comments.region_probabilities if comments else {},
        "comment_calibration_status": catalog.comment_evidence()["calibration_status"],
        "comment_sample_bias": "top_comments_first_page" if comments and comments.has_comments else None,
        "warning": (
            "Creator signals, public priors, and sampled Top comments are not measured "
            "audience geography or language."
        ),
    }
    common = dict(
        source_type="public_prior_estimate",
        truth_status="estimated",
        evidence_strength="weak" if creator_country or comment_language_blend or comment_region_blend else "prior_only",
        model_confidence=min(
            0.58,
            (0.35 if creator_country else 0.2)
            + 0.2 * max(comment_language_blend, comment_region_blend),
        ),
        evidence_confidence=min(
            0.55,
            (0.25 if creator_country else 0.1)
            + 0.35 * max(comment_language_blend, comment_region_blend),
        ),
        evidence_refs=(
            f"prior_catalog:{catalog.version}:language_markets",
            f"prior_catalog:{catalog.version}:audience_market_model",
            *( (f"comment_evidence:{catalog.comment_evidence()['version']}",)
               if comment_language_blend or comment_region_blend else () ),
        ),
        model_version="audience-joint-market-public-v3-comments",
        decision_policy_version=POLICY_VERSION,
        metadata=metadata,
    )
    return FieldResult(value=region_value, **common), FieldResult(value=language_value, **common)


def _rake_age_gender_joint(
    joint: dict[str, float],
    target_age: dict[str, float],
    target_gender: dict[str, float],
) -> dict[str, float]:
    values = {
        f"{age}_{gender}": max(1e-12, float(joint.get(f"{age}_{gender}", 0.0)))
        for age in AGE_RANGES
        for gender in ("male", "female")
    }
    for _ in range(8):
        for age in AGE_RANGES:
            keys = (f"{age}_male", f"{age}_female")
            current = sum(values[key] for key in keys)
            scale = target_age.get(age, 0.0) / current if current else 0.0
            for key in keys:
                values[key] *= scale
        for gender in ("male", "female"):
            keys = tuple(f"{age}_{gender}" for age in AGE_RANGES)
            current = sum(values[key] for key in keys)
            scale = target_gender.get(gender, 0.0) / current if current else 0.0
            for key in keys:
                values[key] *= scale
    return normalize_weights(values)


def analyze_audience_age_gender(
    category: FieldResult,
    catalog: PriorCatalog,
    policy: AnalysisPolicy,
    *,
    snapshot: ChannelSnapshot | None = None,
    tags: FieldResult | None = None,
    comments: CommentEvidence | None = None,
    creator_gender: FieldResult | None = None,
) -> FieldResult:
    if policy is AnalysisPolicy.EVIDENCE_FIRST and not catalog.production_eligible:
        return unavailable("audience_age_gender_prior_is_not_production_eligible")
    category_name = category.value.get("level_1") if isinstance(category.value, dict) else "default"
    creator_gender_value = (
        str(creator_gender.value)
        if creator_gender is not None and creator_gender.value in {"male", "female", "brand_team"}
        else None
    )
    raw = catalog.age_gender(category_name, creator_gender_value)
    adjusted = dict(raw)
    config = catalog.age_gender_adjustments()
    format_shares = _content_format_shares(snapshot)
    for age in AGE_RANGES:
        format_multiplier = sum(
            format_shares[name]
            * float(config["format_age_multipliers"][name].get(age, 1.0))
            for name in format_shares
        )
        if sum(format_shares.values()) == 0:
            format_multiplier = 1.0
        for gender in ("male", "female"):
            key = f"{age}_{gender}"
            adjusted[key] = adjusted.get(key, 0.0) * format_multiplier

    tag_values = (
        list(tags.metadata.get("supported_tags") or [])
        if tags else []
    )
    maximum_log = float(config.get("maximum_adjustment_log", 0.25))
    gender_log_adjustment = {"male": 0.0, "female": 0.0}
    applied_tag_adjustments: list[str] = []
    for tag in tag_values:
        multipliers = config["tag_gender_multipliers"].get(tag)
        if not multipliers:
            continue
        applied_tag_adjustments.append(tag)
        for gender in gender_log_adjustment:
            gender_log_adjustment[gender] += math.log(max(0.01, float(multipliers.get(gender, 1.0))))
    creator_gender_applied = creator_gender_value
    used_creator_specific_prior = catalog.has_creator_conditioned_age_gender(
        category_name, creator_gender_value
    )
    if (
        not used_creator_specific_prior
        and creator_gender is not None
        and creator_gender.value in {"male", "female"}
        and creator_gender.evidence_strength not in {"prior_only", "unavailable"}
    ):
        other = "female" if creator_gender_value == "male" else "male"
        gender_log_adjustment[str(creator_gender_value)] += math.log(1.12)
        gender_log_adjustment[other] += math.log(0.89)
    for gender in gender_log_adjustment:
        gender_log_adjustment[gender] = max(-maximum_log, min(maximum_log, gender_log_adjustment[gender]))
    for age in AGE_RANGES:
        for gender in ("male", "female"):
            adjusted[f"{age}_{gender}"] *= math.exp(gender_log_adjustment[gender])

    adjusted = normalize_weights(adjusted)
    comment_config = catalog.comment_evidence()["age_gender"]
    comment_age_blend = 0.0
    comment_gender_blend = 0.0
    comment_joint_blend = 0.0
    if comments and (comments.age_probabilities or comments.gender_probabilities):
        baseline_age = {
            age: adjusted.get(f"{age}_male", 0.0) + adjusted.get(f"{age}_female", 0.0)
            for age in AGE_RANGES
        }
        baseline_gender = {
            gender: sum(adjusted.get(f"{age}_{gender}", 0.0) for age in AGE_RANGES)
            for gender in ("male", "female")
        }
        comment_age_blend = _shrunken_comment_weight(
            float(comment_config["age_max_blend"]),
            comments.numeric["comment_life_stage_author_count"],
            float(comment_config["age_shrinkage_authors"]),
        ) if comments.age_probabilities else 0.0
        comment_gender_blend = _shrunken_comment_weight(
            float(comment_config["gender_max_blend"]),
            comments.numeric["comment_explicit_gender_author_count"],
            float(comment_config["gender_shrinkage_authors"]),
        ) if comments.gender_probabilities else 0.0
        target_age = _blend_probabilities(
            baseline_age,
            comments.age_probabilities,
            comment_age_blend,
        )
        target_gender = _blend_probabilities(
            baseline_gender,
            comments.gender_probabilities,
            comment_gender_blend,
        )
        adjusted = _rake_age_gender_joint(adjusted, target_age, target_gender)
    if comments and comments.age_gender_probabilities:
        comment_joint_blend = _shrunken_comment_weight(
            float(comment_config["joint_max_blend"]),
            comments.numeric["comment_age_gender_author_count"],
            float(comment_config["joint_shrinkage_authors"]),
        )
        adjusted = _blend_probabilities(
            adjusted,
            comments.age_gender_probabilities,
            comment_joint_blend,
        )

    order = [f"{age}_{gender}" for age in AGE_RANGES for gender in ("male", "female")]
    apportioned = apportion({key: adjusted.get(key, 0.0) for key in order}, order=order)
    value = [
        {
            "age_range": age,
            "male": apportioned[f"{age}_male"],
            "female": apportioned[f"{age}_female"],
        }
        for age in AGE_RANGES
    ]
    return FieldResult(
        value=value,
        source_type="public_prior_estimate",
        truth_status="estimated",
        evidence_strength=(
            "weak"
            if comment_age_blend or comment_gender_blend or comment_joint_blend
            else "prior_only"
        ),
        model_confidence=min(
            0.45,
            (0.28 if snapshot and snapshot.contents else 0.2)
            + 0.2 * max(comment_age_blend, comment_gender_blend, comment_joint_blend),
        ),
        evidence_confidence=min(
            0.4,
            (0.16 if snapshot and snapshot.contents else 0.1)
            + 0.25 * max(comment_age_blend, comment_gender_blend, comment_joint_blend),
        ),
        evidence_refs=(
            f"prior_catalog:{catalog.version}:age_gender:{category_name}",
            f"prior_catalog:{catalog.version}:age_gender_adjustments",
            *( (f"comment_evidence:{catalog.comment_evidence()['version']}",)
               if comment_age_blend or comment_gender_blend or comment_joint_blend else () ),
        ),
        model_version="audience-age-gender-public-v4-gender-priors",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "calibration_status": "uncalibrated_public_estimate",
            "prior_production_eligible": catalog.production_eligible,
            "content_format_shares": {key: round(value, 6) for key, value in format_shares.items()},
            "applied_tag_adjustments": applied_tag_adjustments,
            "applied_creator_gender": creator_gender_applied,
            "comment_age_blend": round(comment_age_blend, 6),
            "comment_gender_blend": round(comment_gender_blend, 6),
            "comment_joint_blend": round(comment_joint_blend, 6),
            "comment_age_probabilities": comments.age_probabilities if comments else {},
            "comment_gender_probabilities": comments.gender_probabilities if comments else {},
            "comment_calibration_status": catalog.comment_evidence()["calibration_status"],
            "comment_sample_bias": "top_comments_first_page" if comments and comments.has_comments else None,
            "warning": (
                "This is a category, content, and sampled-comment public estimate, "
                "not measured channel demographics."
            ),
        },
    )


def analyze_active_ratio(
    snapshot: ChannelSnapshot,
    catalog: PriorCatalog,
    policy: AnalysisPolicy,
    *,
    comments: CommentEvidence | None = None,
) -> FieldResult:
    if policy is AnalysisPolicy.EVIDENCE_FIRST and not catalog.production_eligible:
        return unavailable("active_ratio_prior_is_not_production_eligible")
    subscribers = snapshot.channel.get("subscriber_count")
    try:
        subscribers = int(subscribers)
    except (TypeError, ValueError):
        subscribers = 0
    config = catalog.active_audience()
    window_days = int(config["window_days"])
    maximum_age_days = int(config["maximum_content_age_days"])
    title_counts = Counter(normalize_text(content.title) for content in snapshot.contents)
    placeholder_prefixes = ("uploads from ", "envios de ", "subidas de ", "vídeos de ", "videos de ")
    rows: list[dict[str, Any]] = []
    excluded_placeholder_count = 0
    for content in snapshot.contents:
        if content.view_count is None or content.published_at is None:
            continue
        normalized_title = normalize_text(content.title)
        repeated_placeholder = (
            title_counts[normalized_title] >= 3
            and normalized_title.startswith(placeholder_prefixes)
        )
        if repeated_placeholder:
            excluded_placeholder_count += 1
            continue
        age_days = max(0.0, (snapshot.as_of - content.published_at).total_seconds() / 86400)
        if age_days > maximum_age_days:
            continue
        content_format = "longform" if content.content_type == "video" else content.content_type
        in_window = age_days <= window_days
        if in_window:
            exponent = float(config["growth_curve_exponent"][content_format])
            projection = math.pow(window_days / max(1.0, age_days), exponent)
            projection = min(float(config["maximum_projection_multiplier"]), max(1.0, projection))
            horizon_views = float(content.view_count) * projection
        else:
            projection = float(config["legacy_window_share"][content_format])
            horizon_views = float(content.view_count) * projection
        rows.append({
            "content": content,
            "format": content_format,
            "age_days": age_days,
            "in_window": in_window,
            "projection": projection,
            "horizon_views": horizon_views,
        })
    recent_count = sum(1 for row in rows if row["in_window"])
    if policy is AnalysisPolicy.EVIDENCE_FIRST and (subscribers <= 0 or recent_count < 3):
        return unavailable("active_ratio_has_insufficient_recent_view_observations")
    if not rows:
        latest = max(
            (content.published_at for content in snapshot.contents if content.published_at),
            default=None,
        )
        inactive_days = (
            max(0.0, (snapshot.as_of - latest).total_seconds() / 86400)
            if latest else None
        )
        if subscribers < 50000 and subscribers > 0:
            subscriber_tier = "under_50000"
        elif subscribers <= 500000 and subscribers > 0:
            subscriber_tier = "50000_to_500000"
        else:
            subscriber_tier = "over_500000" if subscribers > 500000 else "50000_to_500000"
        prior_center = float(config["cohort_prior_center"][subscriber_tier])
        # Recent publication activity with missing public metrics can use the cohort
        # center. A channel with no recent publication evidence is reported as zero,
        # with a deliberately wide interval and prior-only status.
        ratio = int(round(prior_center)) if inactive_days is not None and inactive_days <= maximum_age_days else 0
        return FieldResult(
            value=ratio,
            source_type="public_proxy",
            truth_status="estimated",
            evidence_strength="prior_only",
            model_confidence=0.15,
            evidence_confidence=0.05,
            evidence_refs=("no_usable_recent_view_observations",),
            model_version="active-subscriber-public-reach-v3",
            decision_policy_version=POLICY_VERSION,
            metadata={
                "window_days": window_days,
                "prediction_interval": [0, min(95, int(round(prior_center * 2)))],
                "calibration_status": "uncalibrated_public_proxy",
                "subscriber_count_estimated": subscribers <= 0,
                "subscriber_tier": subscriber_tier,
                "cohort_prior_center": prior_center,
                "usable_observation_count": 0,
                "excluded_placeholder_count": excluded_placeholder_count,
                "prior_production_eligible": catalog.production_eligible,
                "warning": "Public views do not identify unique subscribed viewers.",
            },
        )
    rows.sort(
        key=lambda row: (
            row["content"].published_at or datetime.min,
            row["content"].source_content_id,
        ),
        reverse=True,
    )
    selected_rows = rows[:30]
    view_values = [float(row["horizon_views"]) for row in selected_rows]
    view_median = median(view_values)
    absolute_deviations = [abs(value - view_median) for value in view_values]
    view_mad = median(absolute_deviations)
    robust_cap = None
    if len(view_values) >= 5:
        robust_cap = view_median + float(config["winsor_mad_multiplier"]) * max(
            view_mad,
            view_median * float(config["winsor_minimum_relative_spread"]),
            1.0,
        )
    capped_view_count = 0
    grouped_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    engagement_views = 0.0
    engagement_likes = 0.0
    engagement_comments = 0.0
    like_observations = 0
    comment_observations = 0
    for row in selected_rows:
        content = row["content"]
        usable_views = float(row["horizon_views"])
        if robust_cap is not None and usable_views > robust_cap:
            usable_views = robust_cap
            capped_view_count += 1
        grouped_rows[row["format"]].append({**row, "usable_views": usable_views})
        raw_views = float(content.view_count or 0)
        if raw_views > 0 and content.like_count is not None:
            engagement_views += raw_views
            engagement_likes += float(content.like_count)
            like_observations += 1
        if raw_views > 0 and content.comment_count is not None:
            engagement_comments += float(content.comment_count)
            comment_observations += 1
    observation_count = sum(len(values) for values in grouped_rows.values())

    format_unique_reach: dict[str, float] = {}
    format_title_overlap: dict[str, float] = {}
    for content_format, values in grouped_rows.items():
        subscribed_share = float(config["subscribed_view_share"][content_format])
        additional_share = float(config["additional_upload_unique_share"][content_format])
        ranked = sorted(values, key=lambda row: row["usable_views"], reverse=True)
        seen_titles: list[set[str]] = []
        overlaps: list[float] = []
        unique_reach = 0.0
        for index, row in enumerate(ranked):
            title_tokens = set(words(row["content"].title))
            overlap = max(
                (
                    len(title_tokens & previous) / len(title_tokens | previous)
                    for previous in seen_titles
                    if title_tokens | previous
                ),
                default=0.0,
            )
            subscribed_views = row["usable_views"] * subscribed_share
            if index == 0:
                unique_reach += subscribed_views
            else:
                unique_reach += subscribed_views * additional_share * (1.0 - overlap)
                overlaps.append(overlap)
            if title_tokens:
                seen_titles.append(title_tokens)
        format_unique_reach[content_format] = unique_reach
        format_title_overlap[content_format] = sum(overlaps) / len(overlaps) if overlaps else 0.0

    ranked_format_reach = sorted(format_unique_reach.items(), key=lambda item: (-item[1], item[0]))
    cross_format_overlap = float(config["cross_format_overlap"])
    estimated_unique_reach = sum(
        value if index == 0 else value * (1.0 - cross_format_overlap)
        for index, (_, value) in enumerate(ranked_format_reach)
    )

    engagement_components: list[float] = []
    engagement_reference = config["engagement_reference"]
    if like_observations and engagement_views > 0:
        engagement_components.append(
            (engagement_likes / engagement_views) / max(1e-9, float(engagement_reference["like_view"]))
        )
    if comment_observations and engagement_views > 0:
        engagement_components.append(
            (engagement_comments / engagement_views)
            / max(1e-9, float(engagement_reference["comment_view"]))
        )
    if engagement_components:
        engagement_relative = sum(engagement_components) / len(engagement_components)
        engagement_factor = 1.0 + float(config["engagement_signal_weight"]) * (
            math.sqrt(max(0.0, engagement_relative)) - 1.0
        )
        engagement_lower, engagement_upper = (
            float(value) for value in config["engagement_adjustment_bounds"]
        )
        engagement_factor = max(engagement_lower, min(engagement_upper, engagement_factor))
    else:
        engagement_relative = None
        engagement_factor = 1.0

    public_engagement_factor = engagement_factor
    comment_quality_factor = 1.0
    comment_evidence_weight = 0.0
    comment_quality_components: dict[str, float] = {}
    comment_config = catalog.comment_evidence()["active_audience"]
    if comments:
        unique_comment_authors = comments.numeric["comment_unique_author_count"]
        minimum_authors = float(comment_config["minimum_unique_authors"])
        if unique_comment_authors >= minimum_authors:
            comment_evidence_weight = _shrunken_comment_weight(
                float(comment_config["maximum_evidence_weight"]),
                unique_comment_authors,
                float(comment_config["shrinkage_authors"]),
            )
            comment_evidence_weight *= math.sqrt(
                max(0.0, min(1.0, comments.numeric["comment_page_coverage"]))
            )
            log_cap = float(comment_config["component_log_cap"])

            def bounded_log_ratio(value: float, reference: float) -> float:
                score = math.log(max(0.005, value) / max(0.005, reference))
                return max(-log_cap, min(log_cap, score))

            effective_meaningful = (
                comments.numeric["comment_meaningful_ratio"]
                * max(
                    0.0,
                    1.0
                    - float(comment_config["spam_ratio_penalty"])
                    * comments.numeric["comment_spam_ratio"],
                )
            )
            comment_quality_components["meaningful"] = bounded_log_ratio(
                effective_meaningful,
                float(comment_config["meaningful_ratio_reference"]),
            )
            if comments.numeric["comment_page_count"] >= 2:
                comment_quality_components["returning_authors"] = bounded_log_ratio(
                    comments.numeric["comment_returning_author_ratio"],
                    float(comment_config["returning_author_ratio_reference"]),
                )
            if comments.numeric["comment_unique_author_ratio"] > 0:
                comment_quality_components["unique_author_ratio"] = bounded_log_ratio(
                    comments.numeric["comment_unique_author_ratio"],
                    float(comment_config["unique_author_ratio_reference"]),
                )
            mean_log_signal = sum(comment_quality_components.values()) / max(
                1, len(comment_quality_components)
            )
            maximum_adjustment = float(comment_config["maximum_adjustment_log"])
            comment_adjustment = max(
                -maximum_adjustment,
                min(maximum_adjustment, mean_log_signal * comment_evidence_weight),
            )
            comment_quality_factor = math.exp(comment_adjustment)
            engagement_factor *= comment_quality_factor

    subscribers_estimated = subscribers <= 0
    if not subscribers_estimated and subscribers < 50000:
        subscriber_tier = "under_50000"
    elif not subscribers_estimated and subscribers <= 500000:
        subscriber_tier = "50000_to_500000"
    elif not subscribers_estimated:
        subscriber_tier = "over_500000"
    else:
        tier_bounds = {
            "under_50000": (100.0, 49_999.0),
            "50000_to_500000": (50_000.0, 500_000.0),
            "over_500000": (500_001.0, float("inf")),
        }
        candidates: list[tuple[float, str, float]] = []
        for tier, (lower, upper) in tier_bounds.items():
            center = float(config["cohort_prior_center"][tier])
            size = float(config["size_adjustment"][tier])
            implied = max(100.0, estimated_unique_reach * size * engagement_factor * 100.0 / max(1.0, center))
            if lower <= implied <= upper:
                distance = 0.0
            elif implied < lower:
                distance = abs(math.log(max(1.0, implied) / lower))
            else:
                distance = abs(math.log(implied / upper))
            candidates.append((distance, tier, implied))
        _, subscriber_tier, implied_subscribers = min(candidates, key=lambda item: (item[0], item[1]))
        subscribers = int(round(implied_subscribers))

    size_factor = float(config["size_adjustment"][subscriber_tier])
    cohort_center = float(config["cohort_prior_center"][subscriber_tier])
    raw_ratio = estimated_unique_reach / max(1, subscribers) * 100.0
    adjusted_ratio = raw_ratio * size_factor * engagement_factor
    evidence_weight = observation_count / (
        observation_count + float(config["shrinkage_observations"])
    )
    if subscribers_estimated:
        evidence_weight = min(evidence_weight, float(config["estimated_subscriber_max_evidence_weight"]))
    posterior_ratio = adjusted_ratio * evidence_weight + cohort_center * (1.0 - evidence_weight)

    latest = max(row["content"].published_at for row in selected_rows if row["content"].published_at)
    inactive_days = max(0.0, (snapshot.as_of - latest).total_seconds() / 86400)
    if recent_count == 0:
        inactivity_factor = math.pow(
            0.5,
            max(0.0, inactive_days - window_days) / float(config["inactivity_half_life_days"]),
        )
        posterior_ratio *= inactivity_factor
    else:
        inactivity_factor = 1.0
    lower_bound = float(config.get("minimum_ratio", 0))
    upper_bound = float(config.get("maximum_ratio", 95))
    ratio = int(round(max(lower_bound, min(upper_bound, posterior_ratio))))
    temporality_exact = snapshot.replay_quality == "current_exact"
    uncertainty = min(
        45,
        int(round(8 + (1.0 - evidence_weight) * 20 + (0 if temporality_exact else 10) + (8 if subscribers_estimated else 0))),
    )
    evidence_strength = "weak" if recent_count >= 3 and temporality_exact else "prior_only"
    return FieldResult(
        value=ratio,
        source_type="public_proxy",
        truth_status="estimated",
        evidence_strength=evidence_strength,
        model_confidence=round(
            min(0.7, 0.2 + 0.5 * evidence_weight + 0.08 * comment_evidence_weight),
            6,
        ),
        evidence_confidence=(
            round(min(0.72, recent_count / 15 + 0.08 * comment_evidence_weight), 6)
            if temporality_exact else 0.15
        ),
        evidence_refs=(
            f"recent_content_with_views:{recent_count}",
            f"prior_catalog:{catalog.version}:active_audience",
            *( (f"comment_evidence:{catalog.comment_evidence()['version']}",)
               if comment_evidence_weight else () ),
        ),
        model_version="active-subscriber-public-reach-v4-comments",
        decision_policy_version=POLICY_VERSION,
        metadata={
            "window_days": window_days,
            "prediction_interval": [max(0, ratio - uncertainty), min(100, ratio + uncertainty)],
            "calibration_status": "uncalibrated_public_proxy",
            "historical_metric_temporality": snapshot.provenance.get("content_metric_temporality"),
            "contribution_count": observation_count,
            "excluded_placeholder_count": excluded_placeholder_count,
            "winsorized_view_count": capped_view_count,
            "view_median": round(view_median, 3),
            "view_mad": round(view_mad, 3),
            "format_unique_reach": {key: round(value, 3) for key, value in format_unique_reach.items()},
            "estimated_unique_subscriber_reach": round(estimated_unique_reach, 3),
            "base_public_proxy_ratio": round(raw_ratio, 6),
            "subscriber_size_factor": size_factor,
            "subscriber_tier": subscriber_tier,
            "cohort_prior_center": cohort_center,
            "subscriber_count_used": subscribers,
            "subscriber_count_estimated": subscribers_estimated,
            "engagement_relative_to_reference": (
                round(engagement_relative, 6) if engagement_relative is not None else None
            ),
            "engagement_quality_factor": round(engagement_factor, 6),
            "public_engagement_quality_factor": round(public_engagement_factor, 6),
            "comment_quality_factor": round(comment_quality_factor, 6),
            "comment_evidence_weight": round(comment_evidence_weight, 6),
            "comment_quality_components": {
                key: round(value, 6) for key, value in comment_quality_components.items()
            },
            "comment_calibration_status": catalog.comment_evidence()["calibration_status"],
            "comment_sample_bias": "top_comments_first_page" if comments and comments.has_comments else None,
            "format_title_overlap": {key: round(value, 6) for key, value in format_title_overlap.items()},
            "content_format_shares": {
                key: round(len(grouped_rows.get(key, [])) / max(1, observation_count), 6)
                for key in ("short", "longform", "live")
            },
            "raw_adjusted_ratio": round(adjusted_ratio, 6),
            "empirical_bayes_evidence_weight": round(evidence_weight, 6),
            "inactivity_factor": round(inactivity_factor, 6),
            "prior_production_eligible": catalog.production_eligible,
            "warning": "Public views do not identify unique subscribed viewers.",
        },
    )
