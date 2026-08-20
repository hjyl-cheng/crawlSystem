from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

from .contracts import ChannelSnapshot


WORD_RE = re.compile(r"[^\W\d_]+(?:['’-][^\W\d_]+)*", re.UNICODE)
SPACE_RE = re.compile(r"\s+")


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return SPACE_RE.sub(" ", text).strip()


def words(value: object) -> list[str]:
    return WORD_RE.findall(normalize_text(value))


@dataclass(frozen=True)
class TextUnit:
    source: str
    text: str
    weight: float


def snapshot_text_units(snapshot: ChannelSnapshot) -> list[TextUnit]:
    channel = snapshot.channel
    units = [
        TextUnit("channel_title", str(channel.get("title") or "")[:300], 2.0),
        TextUnit("channel_handle", str(channel.get("handle") or "")[:100], 0.8),
        TextUnit("channel_summary", str(channel.get("summary") or "")[:1500], 2.5),
        TextUnit("channel_about", str(channel.get("about_description") or "")[:5000], 4.0),
        TextUnit("channel_keywords", " ".join(channel.get("keywords") or [])[:2500], 3.0),
    ]
    for content in snapshot.contents:
        published_at = content.published_at or content.first_seen_at
        age_days = (
            max(0.0, (snapshot.as_of - published_at).total_seconds() / 86400.0)
            if published_at else 365.0
        )
        # Old content still describes a channel, but recent uploads are more
        # representative of its current language and topic mix.
        recency = max(0.35, math.pow(0.5, age_days / 180.0))
        units.extend(
            (
                TextUnit("content_title", content.title[:400], 1.5 * recency),
                TextUnit("content_description", content.description[:1500], 0.65 * recency),
                TextUnit(
                    "content_keywords",
                    " ".join((*content.keywords, *content.hashtags))[:1500],
                    1.2 * recency,
                ),
            )
        )
    deduplicated: list[TextUnit] = []
    seen: set[str] = set()
    for unit in units:
        normalized = normalize_text(unit.text)
        if len(normalized) < 3 or normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(unit)
    return deduplicated


_LATIN_MARKERS = {
    "Portuguese": {
        "o", "a", "os", "as", "de", "do", "da", "que", "e", "em", "para",
        "com", "uma", "um", "meu", "minha", "voce", "voces", "nao", "canal",
        "vídeo", "vídeos", "hoje", "sejam", "seja", "bem-vindo", "inscreva",
        "vc", "vcs", "pra", "muito", "obrigado", "tudo", "quiser", "fazer",
        "gente", "aí",
    },
    "Spanish": {
        "el", "la", "los", "las", "de", "del", "que", "y", "en", "para",
        "con", "una", "un", "mi", "mis", "ustedes", "hoy", "canal", "video",
        "vídeo", "bienvenidos", "suscríbete", "como", "pero",
    },
    "English": {
        "the", "and", "of", "to", "in", "for", "with", "this", "that", "you",
        "your", "our", "channel", "video", "videos", "welcome", "subscribe", "from",
        "about", "how", "new", "today", "we", "i",
    },
    "French": {
        "le", "la", "les", "des", "de", "du", "et", "en", "pour", "avec", "une",
        "un", "vous", "notre", "chaîne", "vidéo", "bienvenue", "abonnez", "sur",
    },
    "German": {
        "der", "die", "das", "und", "von", "mit", "für", "ein", "eine", "ist",
        "auf", "wir", "ihr", "kanal", "video", "willkommen", "abonnieren",
    },
    "Italian": {
        "il", "lo", "la", "gli", "le", "di", "che", "e", "in", "per", "con",
        "una", "un", "canale", "video", "benvenuti", "iscriviti", "oggi",
    },
    "Indonesian": {
        "dan", "yang", "di", "untuk", "dengan", "ini", "itu", "dari", "kami",
        "kamu", "channel", "video", "selamat", "subscribe", "adalah", "tidak",
    },
    "Turkish": {
        "ve", "bir", "bu", "için", "ile", "kanal", "video", "hoş", "geldiniz",
        "abone", "olan", "ben", "biz", "de", "da", "çok",
    },
    "Vietnamese": {
        "và", "của", "cho", "với", "này", "các", "một", "kênh", "video", "chào",
        "bạn", "đăng", "ký", "không", "trong",
    },
}


def _script_counts(text: str) -> dict[str, int]:
    if text.isascii():
        return {}
    counts = Counter()
    han_count = 0
    kana_count = 0
    for char in text:
        code = ord(char)
        if 0x0600 <= code <= 0x06FF:
            counts["Arabic"] += 1
        elif 0x0900 <= code <= 0x097F:
            counts["Hindi"] += 1
        elif 0x3040 <= code <= 0x30FF:
            kana_count += 1
        elif 0x4E00 <= code <= 0x9FFF:
            han_count += 1
        elif 0xAC00 <= code <= 0xD7AF:
            counts["Korean"] += 1
        elif 0x0400 <= code <= 0x04FF:
            counts["Russian"] += 1
        elif 0x0E00 <= code <= 0x0E7F:
            counts["Thai"] += 1
    if kana_count:
        # Japanese normally mixes kana and kanji. Han characters without kana
        # are Chinese evidence; treating every Han character as Japanese is a
        # script-to-country shortcut and causes systematic errors.
        counts["Japanese"] += kana_count + han_count
    elif han_count:
        counts["Chinese"] += han_count
    return dict(counts)


def normalize_weights(values: dict[str, float]) -> dict[str, float]:
    clean = {key: max(0.0, float(value)) for key, value in values.items() if value > 0}
    total = sum(clean.values())
    return {key: value / total for key, value in clean.items()} if total else {}


@dataclass(frozen=True)
class LanguageEvidence:
    probabilities: dict[str, float]
    effective_characters: int
    source_count: int
    top_margin: float


def detect_languages(units: Iterable[TextUnit]) -> LanguageEvidence:
    scores: Counter[str] = Counter()
    effective_characters = 0
    source_count = 0
    for unit in units:
        text = normalize_text(unit.text)
        if len(text) < 8:
            continue
        source_count += 1
        effective_characters += min(len(text), 1000)
        length_weight = unit.weight * min(3.0, max(0.5, math.log1p(len(text)) / 3.0))
        token_counts = Counter(words(text))
        for language, markers in _LATIN_MARKERS.items():
            hits = sum(token_counts[token] for token in markers)
            if hits:
                scores[language] += length_weight * hits
        for language, count in _script_counts(text).items():
            scores[language] += length_weight * count * 0.55
        if re.search(r"[ãõçáéíóúâêôà]", text):
            scores["Portuguese"] += length_weight * 1.8
        if re.search(r"(?<!\w)k{4,}(?!\w)", text):
            scores["Portuguese"] += length_weight * 1.2
        if re.search(r"[ñ¿¡]", text):
            scores["Spanish"] += length_weight * 2.2
        if re.search(r"[ğışçöü]", text):
            scores["Turkish"] += length_weight * 2.0
        if not scores and re.search(r"[a-z]", text):
            scores["English"] += length_weight * 0.25
    probabilities = normalize_weights(dict(scores))
    # Marker counts are classifier evidence, not a measured language mixture.
    # Sharpen them so shared stop words do not become fake secondary languages,
    # while genuinely balanced bilingual evidence remains balanced.
    probabilities = normalize_weights({key: value**3 for key, value in probabilities.items()})
    if not probabilities and effective_characters:
        probabilities = {"Other": 1.0}
    ranked = sorted(probabilities.values(), reverse=True)
    margin = ranked[0] - ranked[1] if len(ranked) > 1 else (ranked[0] if ranked else 0.0)
    return LanguageEvidence(probabilities, effective_characters, source_count, margin)


def corpus(
    snapshot: ChannelSnapshot,
    *,
    include_content: bool = True,
    units: Iterable[TextUnit] | None = None,
) -> str:
    selected_units = list(units) if units is not None else snapshot_text_units(snapshot)
    selected = selected_units if include_content else [
        unit for unit in selected_units if unit.source.startswith("channel_")
    ]
    return " ".join(normalize_text(unit.text) for unit in selected)
