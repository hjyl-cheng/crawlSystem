from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Any, Protocol, Sequence

from .contracts import AGE_RANGES, ChannelSnapshot, CommentRecord, ContentRecord
from .text_features import (
    LanguageEvidence,
    TextUnit,
    detect_languages,
    normalize_text,
    normalize_weights,
    words,
)


COMMENT_FEATURE_SCHEMA_VERSION = "channel-comment-features-v1"
COMMENT_NUMERIC_FEATURE_NAMES = (
    "comment_page_count",
    "comment_nonempty_page_count",
    "comment_disabled_content_count",
    "comment_unresolved_content_count",
    "comment_sample_count",
    "comment_total_count",
    "comment_unique_author_count",
    "comment_unique_author_ratio",
    "comment_evidence_author_count",
    "comment_returning_author_count",
    "comment_returning_author_ratio",
    "comment_page_coverage",
    "comment_nonempty_page_ratio",
    "comment_meaningful_ratio",
    "comment_generic_ratio",
    "comment_spam_ratio",
    "comment_author_id_coverage",
    "comment_sample_coverage",
    "comment_owner_ratio",
    "comment_pinned_ratio",
    "comment_hearted_ratio",
    "comment_verified_author_ratio",
    "comment_reply_ratio",
    "comment_like_median_log",
    "comment_like_p90_log",
    "comment_reply_median_log",
    "comment_full_per_view",
    "comment_unique_authors_per_1000_views",
    "comment_explicit_region_author_count",
    "comment_dialect_region_author_count",
    "comment_life_stage_author_count",
    "comment_explicit_gender_author_count",
    "comment_age_gender_author_count",
    "comment_creator_gender_author_count",
    "comment_text_characters_log",
    "comment_features_missing",
)


class LanguageIdentifier(Protocol):
    version: str

    def detect(self, units: Sequence[TextUnit]) -> LanguageEvidence:
        ...


@dataclass(frozen=True)
class CommentEvidence:
    topic_text: str
    creator_address_text: str
    language: LanguageEvidence
    region_probabilities: dict[str, float]
    age_probabilities: dict[str, float]
    gender_probabilities: dict[str, float]
    age_gender_probabilities: dict[str, float]
    creator_gender_probabilities: dict[str, float]
    creator_gender_support: dict[str, dict[str, int]]
    numeric: dict[str, float]
    diagnostics: tuple[str, ...]
    feature_schema_version: str = COMMENT_FEATURE_SCHEMA_VERSION

    @property
    def has_comments(self) -> bool:
        return self.numeric["comment_sample_count"] > 0

    def to_record(self) -> dict[str, Any]:
        return {
            "comment_feature_schema_version": self.feature_schema_version,
            "comment_topic_text": self.topic_text,
            "comment_creator_address_text": self.creator_address_text,
            "comment_language_probabilities": self.language.probabilities,
            "comment_region_probabilities": self.region_probabilities,
            "comment_age_probabilities": self.age_probabilities,
            "comment_gender_probabilities": self.gender_probabilities,
            "comment_age_gender_probabilities": self.age_gender_probabilities,
            "comment_creator_gender_probabilities": self.creator_gender_probabilities,
            "comment_creator_gender_support": self.creator_gender_support,
            "comment_diagnostics": list(self.diagnostics),
            "comment_numeric": self.numeric,
        }


_GENERIC_PATTERN = re.compile(
    r"^(?:first|primero|primeiro|nice(?: video)?|great(?: video)?|good(?: video)?|"
    r"amazing|love (?:you|this)|te amo|eu te amo|saludos|parab[eé]ns|felicidades|"
    r"kkkk+|jajaja+|hahaha+|lol+|top|show|muito bom|muy bueno|buen video|bom v[ií]deo)"
    r"[\s!?.❤♥🔥👏😂🤣😍🥰🙏]*$",
    re.IGNORECASE,
)
_SPAM_PATTERN = re.compile(
    r"\b(?:sub\s*4\s*sub|sub(?:scribe)?\s+back|check\s+(?:out\s+)?my\s+channel|"
    r"ganhe dinheiro|earn money fast|whatsapp|telegram)\b",
    re.IGNORECASE,
)
_URL_PATTERN = re.compile(r"(?:https?://|www\.|\b\w+\.(?:com|net|org|io)\b)", re.IGNORECASE)

_REGION_ALIASES = {
    "Brazil": ("brasil", "brazil"),
    "Mexico": ("méxico", "mexico"),
    "United States": ("united states", "usa", "estados unidos", "eua"),
    "United Kingdom": ("united kingdom", "uk", "reino unido"),
    "Canada": ("canada", "canadá"),
    "Australia": ("australia", "austrália"),
    "Portugal": ("portugal",),
    "Spain": ("spain", "españa", "espana"),
    "Argentina": ("argentina",),
    "Colombia": ("colombia", "colômbia"),
    "Chile": ("chile",),
    "India": ("india", "índia"),
    "France": ("france", "frança", "francia"),
    "Germany": ("germany", "deutschland", "alemanha", "alemania"),
    "Italy": ("italy", "italia", "itália"),
    "Japan": ("japan", "japão", "japón", "日本"),
    "South Korea": ("south korea", "coreia do sul", "corea del sur", "대한민국"),
    "China": ("china", "中国", "中國"),
    "Taiwan": ("taiwan", "台灣", "台湾", "臺灣"),
    "Hong Kong": ("hong kong", "香港"),
    "Indonesia": ("indonesia", "indonésia"),
    "Turkey": ("turkey", "türkiye", "turquia"),
    "Vietnam": ("vietnam", "việt nam"),
    "Thailand": ("thailand", "tailândia", "tailandia"),
    "Russia": ("russia", "rússia", "rusia", "россия"),
}

_SELF_LOCATION_PREFIX = (
    r"(?:i(?:'m| am)? from|we(?:'re| are)? from|watching from|greetings from|hello from|"
    r"here in|i live in|soy de|somos de|vivo en|saludos desde|desde|"
    r"sou d[oa]|somos d[oa]|moro n[oa]|aqui d[oa]|assistindo d[oa]|"
    r"je suis de|je viens de|depuis|ich komme aus|gr[uü][sß]e aus)"
)
_SELF_LOCATION_TRIGGER = re.compile(_SELF_LOCATION_PREFIX, re.IGNORECASE)
_CHINESE_SELF_LOCATION_TRIGGER = re.compile(
    r"(?:我(?:來自|来自|住在)|我們(?:來自|来自|住在))"
)

_DIALECT_MARKERS = {
    "Brazil": (r"(?<!\w)k{4,}(?!\w)", r"\b(?:mano|v[ée]i|bora|tamo junto|salve)\b"),
    "Portugal": (r"\b(?:fixe|gajo|rapariga|autocarro|estou a ver)\b",),
    "Mexico": (r"\b(?:g[üu]ey|wey|no manches|chido|[oó]rale|neta)\b",),
    "Argentina": (r"\b(?:che|boludo|re piola|vos sos|pibe)\b",),
    "Colombia": (r"\b(?:parce|parcero|bacano|qu[eé] m[aá]s pues)\b",),
    "Spain": (r"\b(?:chaval|hostia|vosotros|t[ií]o|flipar)\b",),
}

_CREATOR_MALE_ADDRESS = re.compile(
    r"\b(?:bro|brother|dude|sir|hermano|amigo|guapo|rey|mano|irm[aã]o|moço|rapaz)\b",
    re.IGNORECASE,
)
_CREATOR_FEMALE_ADDRESS = re.compile(
    r"\b(?:sis|sister|ma'am|lady|hermana|amiga|guapa|reina|irm[aã]|moça)\b",
    re.IGNORECASE,
)
_SECOND_PERSON_CONTEXT = re.compile(
    r"\b(?:you|your|you're|u|tu|tú|te|ti|tus?|usted|ustedes|vos|sos|eres|"
    r"voc[eê]|c[eê]|seu|sua|teu|tua|você|vocês)\b",
    re.IGNORECASE,
)
_CREATOR_NAME_STOPWORDS = {
    "canal", "channel", "official", "oficial", "television", "tv", "video", "videos",
    "the", "and", "de", "do", "da", "del", "el", "la", "los", "las",
    "anime", "gaming", "gameplay", "games", "news", "music", "show", "podcast",
    "vlog", "vlogs", "short", "shorts",
}
_AUDIENCE_MALE_SELF = re.compile(
    r"\b(?:i am|i'm|soy|sou) (?:a |um |un )?(?:man|male|father|dad|hombre|padre|homem|pai)\b",
    re.IGNORECASE,
)
_AUDIENCE_FEMALE_SELF = re.compile(
    r"\b(?:i am|i'm|soy|sou) (?:a |uma |una )?(?:woman|female|mother|mom|mujer|madre|mulher|m[aã]e)\b",
    re.IGNORECASE,
)
_EXPLICIT_AGE_PATTERNS = (
    re.compile(r"\b(?:i am|i'm)\s+(\d{1,2})\s*(?:years? old)?\b", re.IGNORECASE),
    re.compile(r"\b(?:tengo|tenho)\s+(\d{1,2})\s+(?:a[nñ]os|anos)\b", re.IGNORECASE),
    re.compile(
        r"\b(?:(?:eu )?sou (?:uma mulher|um homem)|soy (?:una mujer|un hombre))\s+de\s+"
        r"(\d{1,2})\s+(?:anos|a[nñ]os)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bj['’]ai\s+(\d{1,2})\s+ans\b", re.IGNORECASE),
)
_LIFE_STAGE_PATTERNS = {
    "18-24": re.compile(
        r"\b(?:college student|university student|sou universit[aá]ri[oa]|estudiante universitari[oa])\b",
        re.IGNORECASE,
    ),
    "25-34": re.compile(
        r"\b(?:new mom|new dad|m[aã]e de um beb[eê]|pai de um beb[eê]|madre primeriza|padre primerizo)\b",
        re.IGNORECASE,
    ),
    "35-44": re.compile(
        r"\b(?:my teenage (?:son|daughter)|meu filho adolescente|minha filha adolescente)\b",
        re.IGNORECASE,
    ),
    "55-64": re.compile(
        r"\b(?:near retirement|prestes a aposentar|casi jubilad[oa])\b",
        re.IGNORECASE,
    ),
    "65+": re.compile(
        r"\b(?:i am retired|i'm retired|sou aposentad[oa]|soy jubilad[oa])\b",
        re.IGNORECASE,
    ),
}


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, probability)) * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _safe_ratio(numerator: float, denominator: float) -> float:
    return max(0.0, float(numerator)) / max(1.0, float(denominator))


def _age_bucket(age: int) -> str | None:
    if 18 <= age <= 24:
        return "18-24"
    if age <= 34 and age >= 25:
        return "25-34"
    if age <= 44 and age >= 35:
        return "35-44"
    if age <= 54 and age >= 45:
        return "45-54"
    if age <= 64 and age >= 55:
        return "55-64"
    if 65 <= age <= 90:
        return "65+"
    return None


def _explicit_regions(text: str) -> set[str]:
    latin_location = bool(_SELF_LOCATION_TRIGGER.search(text))
    chinese_location = bool(_CHINESE_SELF_LOCATION_TRIGGER.search(text))
    if not latin_location and not chinese_location:
        return set()
    matches: set[str] = set()
    if latin_location:
        for region, aliases in _REGION_ALIASES.items():
            alias_pattern = "|".join(re.escape(alias) for alias in aliases)
            if re.search(rf"{_SELF_LOCATION_PREFIX}\s+(?:the\s+)?(?:{alias_pattern})(?!\w)", text, re.I):
                matches.add(region)
    if chinese_location and re.search(r"(?:我(?:來自|来自|住在)|我們(?:來自|来自|住在))\s*(?:中国|中國)", text):
        matches.add("China")
    if chinese_location and re.search(r"(?:我(?:來自|来自|住在)|我們(?:來自|来自|住在))\s*(?:台灣|台湾|臺灣)", text):
        matches.add("Taiwan")
    if chinese_location and re.search(r"(?:我(?:來自|来自|住在)|我們(?:來自|来自|住在))\s*香港", text):
        matches.add("Hong Kong")
    return matches


def _dialect_regions(text: str) -> set[str]:
    return {
        region
        for region, patterns in _DIALECT_MARKERS.items()
        if any(re.search(pattern, text, re.I) for pattern in patterns)
    }


def _comment_age_bucket(text: str) -> str | None:
    ages: list[int] = []
    if any(character.isdigit() for character in text):
        for pattern in _EXPLICIT_AGE_PATTERNS:
            ages.extend(int(value) for value in pattern.findall(text))
    buckets = [_age_bucket(age) for age in ages]
    usable = [bucket for bucket in buckets if bucket]
    if usable:
        return Counter(usable).most_common(1)[0][0]
    matches = [bucket for bucket, pattern in _LIFE_STAGE_PATTERNS.items() if pattern.search(text)]
    return matches[0] if len(matches) == 1 else None


def _comment_gender(text: str) -> str | None:
    if not re.search(r"\b(?:i am|i'm|soy|sou)\b", text, re.IGNORECASE):
        return None
    male = bool(_AUDIENCE_MALE_SELF.search(text))
    female = bool(_AUDIENCE_FEMALE_SELF.search(text))
    if male == female:
        return None
    return "male" if male else "female"


def _is_creator_address(
    text: str,
    match: re.Match[str],
    creator_name_tokens: set[str],
) -> bool:
    before = text[max(0, match.start() - 40):match.start()]
    after = text[match.end():match.end() + 60]
    context = f"{before} {after}"
    if _SECOND_PERSON_CONTEXT.search(context):
        return True
    if creator_name_tokens and creator_name_tokens.intersection(words(context)):
        return True
    left_vocative = match.start() == 0 or bool(re.search(r"[,!?:;]\s*$", before))
    right_vocative = bool(re.match(r"\s*[,!?:;]", after)) or not after.strip()
    return left_vocative and right_vocative


def _creator_address_gender(text: str, creator_name_tokens: set[str]) -> str | None:
    male = any(
        _is_creator_address(text, match, creator_name_tokens)
        for match in _CREATOR_MALE_ADDRESS.finditer(text)
    )
    female = any(
        _is_creator_address(text, match, creator_name_tokens)
        for match in _CREATOR_FEMALE_ADDRESS.finditer(text)
    )
    if male == female:
        return None
    return "male" if male else "female"


def _creator_name_grammar_gender(
    text: str,
    creator_name_tokens: set[str],
) -> str | None:
    # Articles before one token are useful for nickname-style handles such as
    # "o coruga". With multi-token titles, matching any common word (for
    # example "a história") attributes grammatical gender to the title, not
    # necessarily to a person.
    if len(creator_name_tokens) != 1:
        return None
    token_pattern = "|".join(
        re.escape(token)
        for token in sorted(creator_name_tokens, key=lambda value: (-len(value), value))
    )
    male = bool(re.search(rf"\b(?:o|do|ao|dele)\s+(?:{token_pattern})\b", text, re.IGNORECASE))
    female = bool(re.search(rf"\b(?:a|da|dela)\s+(?:{token_pattern})\b", text, re.IGNORECASE))
    if male == female:
        return None
    return "male" if male else "female"


def _empty_language() -> LanguageEvidence:
    return LanguageEvidence({}, 0, 0, 0.0)


def _empty_evidence(content_count: int = 0) -> CommentEvidence:
    numeric = {name: 0.0 for name in COMMENT_NUMERIC_FEATURE_NAMES}
    numeric["comment_features_missing"] = 1.0
    numeric["comment_page_coverage"] = 0.0 if content_count else 0.0
    return CommentEvidence(
        topic_text="",
        creator_address_text="",
        language=_empty_language(),
        region_probabilities={},
        age_probabilities={},
        gender_probabilities={},
        age_gender_probabilities={},
        creator_gender_probabilities={},
        creator_gender_support={},
        numeric=numeric,
        diagnostics=("no_comment_pages",),
    )


def _ordered_contents(snapshot: ChannelSnapshot) -> list[ContentRecord]:
    return sorted(
        snapshot.contents,
        key=lambda content: (
            content.published_at or content.first_seen_at or snapshot.as_of,
            content.source_content_id,
        ),
        reverse=True,
    )


def build_comment_evidence(
    snapshot: ChannelSnapshot,
    language_identifier: LanguageIdentifier | None = None,
    *,
    maximum_topic_characters: int = 12000,
    maximum_language_authors: int = 120,
) -> CommentEvidence:
    """Aggregate immutable top-level comment pages into one channel evidence record."""

    ordered_contents = _ordered_contents(snapshot)
    page_contents = [content for content in ordered_contents if content.comments_first_page is not None]
    if not page_contents:
        return _empty_evidence(len(snapshot.contents))

    entries: list[dict[str, Any]] = []
    seen_comment_ids: set[str] = set()
    total_count = 0
    known_total_pages = 0
    for content_rank, content in enumerate(page_contents):
        page = content.comments_first_page
        if page is None:
            continue
        if page.total_count is not None:
            total_count += page.total_count
            known_total_pages += 1
        for comment in page.comments:
            if comment.comment_id in seen_comment_ids:
                continue
            seen_comment_ids.add(comment.comment_id)
            normalized = normalize_text(comment.text)
            author_key = comment.author_channel_id or f"comment:{comment.comment_id}"
            entries.append({
                "content": content,
                "content_rank": content_rank,
                "comment": comment,
                "text": normalized,
                "author_key": author_key,
                "has_author_id": bool(comment.author_channel_id),
            })

    if not entries:
        empty = _empty_evidence(len(snapshot.contents))
        numeric = dict(empty.numeric)
        numeric["comment_page_count"] = float(len(page_contents))
        numeric["comment_page_coverage"] = _safe_ratio(len(page_contents), len(snapshot.contents))
        return CommentEvidence(
            **{**empty.__dict__, "numeric": numeric, "diagnostics": ("comment_pages_are_empty",)}
        )

    duplicate_text_counts = Counter(entry["text"] for entry in entries if entry["text"])
    for entry in entries:
        text = entry["text"]
        alpha_count = sum(character.isalpha() for character in text)
        token_count = len(words(text))
        generic = bool(_GENERIC_PATTERN.fullmatch(text))
        spam = bool(
            _SPAM_PATTERN.search(text)
            or len(_URL_PATTERN.findall(text)) >= 2
            or (len(text) >= 20 and duplicate_text_counts[text] >= 5)
        )
        meaningful = bool(
            text
            and not generic
            and not spam
            and len(text) >= 12
            and (token_count >= 3 or alpha_count >= 8)
        )
        entry.update({"generic": generic, "spam": spam, "meaningful": meaningful})

    audience_entries = [entry for entry in entries if not entry["comment"].is_channel_owner]
    author_videos: defaultdict[str, set[str]] = defaultdict(set)
    author_texts: defaultdict[str, list[str]] = defaultdict(list)
    author_best_position: dict[str, int] = {}
    author_has_id: dict[str, bool] = {}
    explicit_regions_by_author: dict[str, set[str]] = {}
    dialect_regions_by_author: dict[str, set[str]] = {}
    ages_by_author: dict[str, str] = {}
    genders_by_author: dict[str, str] = {}

    for entry in audience_entries:
        comment: CommentRecord = entry["comment"]
        author = entry["author_key"]
        author_videos[author].add(entry["content"].source_content_id)
        author_has_id[author] = bool(entry["has_author_id"])
        author_best_position[author] = min(author_best_position.get(author, comment.position), comment.position)
        if entry["text"] and not entry["spam"]:
            author_texts[author].append(entry["text"])

    ranked_authors = sorted(
        author_texts,
        key=lambda author: (
            -len(author_videos[author]),
            author_best_position.get(author, 10**9),
            author,
        ),
    )[:maximum_language_authors]
    handle_text = normalize_text(str(snapshot.channel.get("handle") or "").lstrip("@"))
    title_text = normalize_text(str(snapshot.channel.get("title") or ""))
    name_source = f"{title_text} {handle_text}".strip()
    creator_name_tokens = {
        token
        for token in words(name_source)
        if len(token) >= 3 and token not in _CREATOR_NAME_STOPWORDS
    }
    title_name_tokens = {
        token
        for token in words(title_text)
        if len(token) >= 3 and token not in _CREATOR_NAME_STOPWORDS
    }
    creator_grammar_tokens = (
        title_name_tokens if len(title_name_tokens) == 1 else set()
    )
    combined_author_text = {
        author: " ".join(author_texts[author])[:3000]
        for author in ranked_authors
    }
    creator_gender_by_author: dict[str, str] = {}
    creator_gender_videos_by_author: dict[str, set[str]] = {}
    creator_gender_comments_by_author: dict[str, int] = {}
    for author, text in combined_author_text.items():
        explicit_regions_by_author[author] = _explicit_regions(text)
        dialect_regions_by_author[author] = _dialect_regions(text)
        age = _comment_age_bucket(text)
        gender = _comment_gender(text)
        creator_gender = (
            _creator_address_gender(text, creator_name_tokens)
            or _creator_name_grammar_gender(text, creator_grammar_tokens)
        )
        if age:
            ages_by_author[author] = age
        if gender:
            genders_by_author[author] = gender
        if creator_gender:
            creator_gender_by_author[author] = creator_gender
            supporting_entries = [
                entry
                for entry in audience_entries
                if entry["author_key"] == author
                and (
                    _creator_address_gender(entry["text"], creator_name_tokens)
                    or _creator_name_grammar_gender(entry["text"], creator_grammar_tokens)
                ) == creator_gender
            ]
            creator_gender_videos_by_author[author] = {
                entry["content"].source_content_id for entry in supporting_entries
            }
            creator_gender_comments_by_author[author] = len(supporting_entries)

    language_units = [
        TextUnit("comment_author", combined_author_text[author][:1000], 1.0)
        for author in ranked_authors
        if len(combined_author_text[author]) >= 15
    ]
    language = (
        language_identifier.detect(language_units)
        if language_identifier is not None
        else detect_languages(language_units)
    ) if language_units else _empty_language()

    explicit_region_counts: Counter[str] = Counter()
    dialect_region_counts: Counter[str] = Counter()
    for regions in explicit_regions_by_author.values():
        if len(regions) == 1:
            explicit_region_counts[next(iter(regions))] += 1
    for author, regions in dialect_regions_by_author.items():
        explicit = explicit_regions_by_author.get(author, set())
        usable = regions - explicit
        if len(usable) == 1:
            dialect_region_counts[next(iter(usable))] += 1
    region_probabilities = normalize_weights({
        region: explicit_region_counts[region] * 4.0 + dialect_region_counts[region]
        for region in set(explicit_region_counts) | set(dialect_region_counts)
    })

    age_counts = Counter(ages_by_author.values())
    gender_counts = Counter(genders_by_author.values())
    age_gender_counts: Counter[str] = Counter()
    for author, age in ages_by_author.items():
        gender = genders_by_author.get(author)
        if gender:
            age_gender_counts[f"{age}_{gender}"] += 1.0
    age_probabilities = normalize_weights({age: age_counts[age] for age in AGE_RANGES})
    gender_probabilities = normalize_weights(dict(gender_counts))
    age_gender_probabilities = normalize_weights(dict(age_gender_counts))
    creator_gender_counts = Counter(creator_gender_by_author.values())
    creator_gender_support: dict[str, dict[str, int]] = {}
    for label in sorted(creator_gender_counts):
        label_authors = {
            author for author, candidate in creator_gender_by_author.items()
            if candidate == label
        }
        creator_gender_support[label] = {
            "author_count": len(label_authors),
            "video_count": len({
                content_id
                for author in label_authors
                for content_id in creator_gender_videos_by_author.get(author, set())
            }),
            "comment_count": sum(
                creator_gender_comments_by_author.get(author, 0)
                for author in label_authors
            ),
        }
    creator_gender_probabilities: dict[str, float] = {}
    if creator_gender_counts:
        top_gender, top_count = creator_gender_counts.most_common(1)[0]
        total_creator_gender_authors = sum(creator_gender_counts.values())
        if top_count >= 3 and top_count / total_creator_gender_authors >= 0.75:
            creator_gender_probabilities = normalize_weights(dict(creator_gender_counts))
        else:
            top_gender = ""
    else:
        top_gender = ""

    topic_candidates = sorted(
        (entry for entry in audience_entries if entry["meaningful"]),
        key=lambda entry: (
            entry["content_rank"],
            -int(entry["comment"].is_pinned),
            -int(entry["comment"].is_hearted),
            -math.log1p(entry["comment"].like_count),
            entry["comment"].position,
            entry["comment"].comment_id,
        ),
    )
    topic_parts: list[str] = []
    topic_characters = 0
    topic_seen: set[str] = set()
    topic_by_author: Counter[str] = Counter()
    topic_by_content: Counter[str] = Counter()
    for entry in topic_candidates:
        text = entry["text"]
        author = entry["author_key"]
        content_id = entry["content"].source_content_id
        if text in topic_seen or topic_by_author[author] >= 2 or topic_by_content[content_id] >= 10:
            continue
        if topic_characters + len(text) + 1 > maximum_topic_characters:
            continue
        topic_seen.add(text)
        topic_by_author[author] += 1
        topic_by_content[content_id] += 1
        topic_parts.append(text)
        topic_characters += len(text) + 1

    address_parts: list[str] = []
    address_seen_authors: set[str] = set()
    address_characters = 0
    for entry in audience_entries:
        author = entry["author_key"]
        if (
            author in address_seen_authors
            or author not in creator_gender_by_author
            or creator_gender_by_author[author] != top_gender
        ):
            continue
        text = entry["text"]
        if address_characters + len(text) + 1 > 4000:
            break
        address_seen_authors.add(author)
        address_parts.append(text)
        address_characters += len(text) + 1

    sample_count = len(entries)
    audience_count = len(audience_entries)
    unique_authors = len(author_videos)
    returning_authors = sum(len(videos) >= 2 for videos in author_videos.values())
    likes = [math.log1p(entry["comment"].like_count) for entry in audience_entries]
    replies = [math.log1p(entry["comment"].reply_count) for entry in audience_entries]
    view_total = sum(
        float(content.view_count or 0)
        for content in page_contents
        if content.view_count is not None
    )
    full_comment_total = sum(
        float(content.comment_count or 0)
        for content in page_contents
        if content.comment_count is not None
    )
    known_total = total_count if known_total_pages else 0
    numeric = {
        "comment_page_count": float(len(page_contents)),
        "comment_nonempty_page_count": float(sum(bool(content.comments_first_page and content.comments_first_page.comments) for content in page_contents)),
        "comment_disabled_content_count": float(sum(content.comments_disabled is True for content in snapshot.contents)),
        "comment_unresolved_content_count": float(sum(
            content.comments_first_page is None
            and content.comments_disabled is not True
            and content.comment_count_status in {"unavailable", "unresolved"}
            for content in snapshot.contents
        )),
        "comment_sample_count": float(sample_count),
        "comment_total_count": float(known_total),
        "comment_unique_author_count": float(unique_authors),
        "comment_unique_author_ratio": _safe_ratio(unique_authors, audience_count),
        "comment_evidence_author_count": float(len(ranked_authors)),
        "comment_returning_author_count": float(returning_authors),
        "comment_returning_author_ratio": _safe_ratio(returning_authors, unique_authors),
        "comment_page_coverage": _safe_ratio(len(page_contents), len(snapshot.contents)),
        "comment_nonempty_page_ratio": _safe_ratio(sum(bool(content.comments_first_page and content.comments_first_page.comments) for content in page_contents), len(page_contents)),
        "comment_meaningful_ratio": _safe_ratio(sum(entry["meaningful"] for entry in audience_entries), audience_count),
        "comment_generic_ratio": _safe_ratio(sum(entry["generic"] for entry in audience_entries), audience_count),
        "comment_spam_ratio": _safe_ratio(sum(entry["spam"] for entry in audience_entries), audience_count),
        "comment_author_id_coverage": _safe_ratio(sum(entry["has_author_id"] for entry in audience_entries), audience_count),
        "comment_sample_coverage": _safe_ratio(sample_count, known_total) if known_total else 0.0,
        "comment_owner_ratio": _safe_ratio(sum(entry["comment"].is_channel_owner for entry in entries), sample_count),
        "comment_pinned_ratio": _safe_ratio(sum(entry["comment"].is_pinned for entry in audience_entries), audience_count),
        "comment_hearted_ratio": _safe_ratio(sum(entry["comment"].is_hearted for entry in audience_entries), audience_count),
        "comment_verified_author_ratio": _safe_ratio(sum(entry["comment"].is_verified for entry in audience_entries), audience_count),
        "comment_reply_ratio": _safe_ratio(sum(entry["comment"].reply_count > 0 for entry in audience_entries), audience_count),
        "comment_like_median_log": median(likes) if likes else 0.0,
        "comment_like_p90_log": _quantile(likes, 0.9),
        "comment_reply_median_log": median(replies) if replies else 0.0,
        "comment_full_per_view": _safe_ratio(full_comment_total, view_total),
        "comment_unique_authors_per_1000_views": _safe_ratio(unique_authors * 1000.0, view_total),
        "comment_explicit_region_author_count": float(sum(explicit_region_counts.values())),
        "comment_dialect_region_author_count": float(sum(dialect_region_counts.values())),
        "comment_life_stage_author_count": float(len(ages_by_author)),
        "comment_explicit_gender_author_count": float(len(genders_by_author)),
        "comment_age_gender_author_count": float(sum(age_gender_counts.values())),
        "comment_creator_gender_author_count": float(sum(creator_gender_counts.values())),
        "comment_text_characters_log": math.log1p(sum(len(entry["text"]) for entry in audience_entries)),
        "comment_features_missing": 0.0,
    }
    if tuple(numeric) != COMMENT_NUMERIC_FEATURE_NAMES:
        raise AssertionError("comment numeric feature order drifted")
    diagnostics = ["sample_bias:top_comments"]
    if known_total_pages < len(page_contents):
        diagnostics.append("some_comment_page_totals_are_missing")
    if any(not value for value in author_has_id.values()):
        diagnostics.append("some_comment_authors_lack_channel_id")
    if len(author_texts) > maximum_language_authors:
        diagnostics.append("comment_author_evidence_capped")
    if creator_gender_counts and not creator_gender_probabilities:
        diagnostics.append("creator_gender_comment_signal_below_consensus_gate")
    return CommentEvidence(
        topic_text=" ".join(topic_parts),
        creator_address_text=" ".join(address_parts),
        language=language,
        region_probabilities=region_probabilities,
        age_probabilities=age_probabilities,
        gender_probabilities=gender_probabilities,
        age_gender_probabilities=age_gender_probabilities,
        creator_gender_probabilities=creator_gender_probabilities,
        creator_gender_support=creator_gender_support,
        numeric=numeric,
        diagnostics=tuple(diagnostics),
    )
