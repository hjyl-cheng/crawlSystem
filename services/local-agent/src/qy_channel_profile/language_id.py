from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from pathlib import Path
from typing import Sequence

from .errors import ModelBundleError
from .text_features import (
    LanguageEvidence,
    TextUnit,
    detect_languages,
    normalize_text,
    normalize_weights,
)


FASTTEXT_LANGUAGE_VERSION = "fasttext-lid.176-ftz"

_CONTACT_TOKEN_PATTERN = re.compile(
    r"(?:https?://\S+|www\.\S+|[\w.+-]+@[\w.-]+\.[a-z]{2,})",
    re.IGNORECASE,
)
_BUSINESS_LINE_PATTERN = re.compile(
    r"\b(?:for\s+(?:chinese\s+)?business|business\s+(?:enquir|inquir|contact)|"
    r"commercial\s+(?:enquir|inquir|contact))",
    re.IGNORECASE,
)


def _language_text(unit: TextUnit) -> str:
    """Remove contact boilerplate that describes routing, not content language."""

    raw = str(unit.text or "")
    if unit.source in {"channel_summary", "channel_about"}:
        retained = [
            line
            for line in raw.splitlines()
            if not _BUSINESS_LINE_PATTERN.search(line)
        ]
        raw = "\n".join(retained)
    return normalize_text(_CONTACT_TOKEN_PATTERN.sub(" ", raw)).replace("\n", " ")


def _log_pool_language_evidence(
    model_probabilities: dict[str, float],
    lexical: LanguageEvidence,
) -> dict[str, float]:
    """Correct an uncertain generic model with independent lexical evidence."""

    if not model_probabilities or not lexical.probabilities:
        return model_probabilities
    model_ranked = sorted(model_probabilities.items(), key=lambda item: (-item[1], item[0]))
    lexical_ranked = sorted(lexical.probabilities.items(), key=lambda item: (-item[1], item[0]))
    _, model_probability = model_ranked[0]
    _, lexical_probability = lexical_ranked[0]
    strong_lexical = (
        lexical_probability >= 0.8
        and lexical.top_margin >= 0.65
        and lexical.effective_characters >= 80
        and lexical.source_count >= 2
    )
    # FastText is normally authoritative. The lexical classifier only acts
    # when the generic model itself is uncertain; agreement is not a reason to
    # sharpen an already-confident probability distribution.
    if not strong_lexical or model_probability >= 0.55:
        return model_probabilities

    lexical_weight = 0.45
    floor = 1e-6
    languages = set(model_probabilities) | set(lexical.probabilities)
    pooled = {
        language: math.exp(
            (1.0 - lexical_weight)
            * math.log(max(floor, model_probabilities.get(language, floor)))
            + lexical_weight
            * math.log(max(floor, lexical.probabilities.get(language, floor)))
        )
        for language in languages
    }
    return normalize_weights(pooled)

_LANGUAGE_NAMES = {
    "af": "Afrikaans", "ar": "Arabic", "az": "Azerbaijani", "be": "Belarusian",
    "bg": "Bulgarian", "bn": "Bengali", "bs": "Bosnian", "ca": "Catalan",
    "ceb": "Cebuano", "co": "Corsican", "cs": "Czech", "cy": "Welsh",
    "da": "Danish", "de": "German", "el": "Greek", "en": "English",
    "eo": "Esperanto", "es": "Spanish", "et": "Estonian", "eu": "Basque",
    "fa": "Persian", "fi": "Finnish", "fr": "French", "fy": "Western Frisian",
    "ga": "Irish", "gd": "Scottish Gaelic", "gl": "Galician", "gu": "Gujarati",
    "ha": "Hausa", "haw": "Hawaiian", "he": "Hebrew", "hi": "Hindi",
    "hmn": "Hmong", "hr": "Croatian", "ht": "Haitian Creole", "hu": "Hungarian",
    "hy": "Armenian", "id": "Indonesian", "ig": "Igbo", "is": "Icelandic",
    "it": "Italian", "ja": "Japanese", "jv": "Javanese", "ka": "Georgian",
    "kk": "Kazakh", "km": "Khmer", "kn": "Kannada", "ko": "Korean",
    "ku": "Kurdish", "ky": "Kyrgyz", "la": "Latin", "lb": "Luxembourgish",
    "lo": "Lao", "lt": "Lithuanian", "lv": "Latvian", "mg": "Malagasy",
    "mi": "Maori", "mk": "Macedonian", "ml": "Malayalam", "mn": "Mongolian",
    "mr": "Marathi", "ms": "Malay", "mt": "Maltese", "my": "Burmese",
    "ne": "Nepali", "nl": "Dutch", "no": "Norwegian", "ny": "Chichewa",
    "pa": "Punjabi", "pl": "Polish", "ps": "Pashto", "pt": "Portuguese",
    "ro": "Romanian", "ru": "Russian", "sd": "Sindhi", "si": "Sinhala",
    "sk": "Slovak", "sl": "Slovenian", "sm": "Samoan", "sn": "Shona",
    "so": "Somali", "sq": "Albanian", "sr": "Serbian", "st": "Southern Sotho",
    "su": "Sundanese", "sv": "Swedish", "sw": "Swahili", "ta": "Tamil",
    "te": "Telugu", "tg": "Tajik", "th": "Thai", "tl": "Tagalog",
    "tr": "Turkish", "uk": "Ukrainian", "ur": "Urdu", "uz": "Uzbek",
    "vi": "Vietnamese", "xh": "Xhosa", "yi": "Yiddish", "yo": "Yoruba",
    "zh": "Chinese", "zu": "Zulu",
}


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


class FastTextLanguageIdentifier:
    version = FASTTEXT_LANGUAGE_VERSION

    def __init__(self, model_path: str | Path, *, expected_sha256: str | None = None) -> None:
        path = Path(model_path)
        if not path.is_file():
            raise ModelBundleError(f"FastText language model not found: {path}")
        actual_hash = file_sha256(path)
        if expected_sha256 and actual_hash != expected_sha256:
            raise ModelBundleError("FastText language model hash mismatch")
        try:
            import fasttext
        except ImportError as error:
            raise ModelBundleError("fasttext-wheel is required for the language artifact") from error
        self.path = path
        self.sha256 = actual_hash
        self._model = fasttext.load_model(str(path))

    def detect(self, units: Sequence[TextUnit]) -> LanguageEvidence:
        source_scores: dict[str, Counter[str]] = {}
        source_characters: Counter[str] = Counter()
        effective_characters = 0
        source_count = 0
        evaluated_units: list[TextUnit] = []
        prepared_units: list[tuple[TextUnit, str, float]] = []
        for unit in units:
            text = _language_text(unit)
            if len(text) < 15 or not any(character.isalpha() for character in text):
                continue
            evaluated_units.append(TextUnit(unit.source, text, unit.weight))
            source_count += 1
            capped_length = min(len(text), 1000)
            effective_characters += capped_length
            source_characters[unit.source] += capped_length
            length_weight = unit.weight * min(4.0, math.log1p(capped_length))
            prepared_units.append((unit, text, length_weight))

        if prepared_units:
            batch_labels, batch_probabilities = self._model.predict(
                [text[:10000] for _, text, _ in prepared_units],
                k=3,
                threshold=0.01,
            )
        else:
            batch_labels, batch_probabilities = (), ()
        for (unit, _, length_weight), labels, probabilities in zip(
            prepared_units,
            batch_labels,
            batch_probabilities,
        ):
            unit_scores = source_scores.setdefault(unit.source, Counter())
            for label, probability in zip(labels, probabilities):
                code = str(label).removeprefix("__label__")
                language = _LANGUAGE_NAMES.get(code, "Other")
                unit_scores[language] += length_weight * float(probability)

        # Thirty near-identical descriptions must not overwhelm an explicit
        # channel About. Each source family gets a reliability budget, while
        # still allowing titles/keywords across several uploads to outweigh a
        # very short profile.
        source_budgets = {
            "channel_title": 1.5,
            "channel_handle": 0.35,
            "channel_summary": 3.0,
            "channel_about": 7.0,
            "channel_keywords": 1.5,
            "content_title": 8.0,
            "content_description": 3.0,
            # Keywords are topic-rich but often dominated by English proper
            # nouns, product names, and translated metadata.
            "content_keywords": 2.5,
        }
        scores: Counter[str] = Counter()
        for source, values in source_scores.items():
            normalized_source = normalize_weights(dict(values))
            information = min(1.0, math.log1p(source_characters[source]) / math.log(501))
            budget = source_budgets.get(source, 1.0) * information
            for language, probability in normalized_source.items():
                scores[language] += budget * probability
        normalized = normalize_weights(dict(scores))
        normalized = _log_pool_language_evidence(
            normalized,
            detect_languages(evaluated_units),
        )
        if not normalized and effective_characters:
            normalized = {"Other": 1.0}
        ranked = sorted(normalized.values(), reverse=True)
        margin = ranked[0] - ranked[1] if len(ranked) > 1 else (ranked[0] if ranked else 0.0)
        return LanguageEvidence(
            probabilities=normalized,
            effective_characters=effective_characters,
            source_count=source_count,
            top_margin=margin,
        )
