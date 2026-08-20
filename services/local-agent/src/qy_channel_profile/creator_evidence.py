from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .contracts import ChannelSnapshot
from .text_features import normalize_text


_SELF_NAME_PATTERNS = (
    re.compile(
        r"\b(?:ol[aá][,!]?\s*)?(?:muito prazer\s+)?(?:eu\s+)?me\s+chamo\s+"
        r"([\wÀ-ÿ'’-]+(?:\s+[\wÀ-ÿ'’-]+){0,3}?)"
        r"(?=\s+(?:sou|e sou|mas|tenho|e\b)|[,!;.\n]|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:meu nome [ée]|mi nombre es|me llamo|my name is)\s+"
        r"([\wÀ-ÿ'’-]+(?:\s+[\wÀ-ÿ'’-]+){0,3}?)"
        r"(?=\s+(?:sou|soy|i am|i'm|mas|e\b)|[,!;.\n]|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+[ao]\s+"
        r"(?!maior|melhor|unica|única|unico|único|nova|novo|primeira|primeiro|"
        r"favor|prova|mesma|pessoa|canal|tipo|voz|vez)"
        r"([\wÀ-ÿ'’-]+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+"
        r"(?!uma?\b|un\b|el\b|la\b|[ao]\b)"
        r"([A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÀ-ÿ'’-]*)"
    ),
    re.compile(
        r"\bi(?:'m| am)\s+([\wÀ-ÿ'’-]+)(?=\s*[,!.]| but\b| and\b|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?i:o canal)\s+"
        r"([A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÀ-ÿ'’-]+(?:\s+[A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÀ-ÿ'’-]*){0,3})"
        r"(?i:\s+agora\s+[ée])\b"
    ),
    re.compile(
        r"(?i:(?:o canal\s+[ée]\s+)?apresentad[oa]\s+por|presented by|hosted by)\s+"
        r"([A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÀ-ÿ'’-]+(?:\s+[A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÀ-ÿ'’-]*){0,2})"
    ),
)

_MALE_SELF_PATTERNS = (
    re.compile(
        r"\b(?:eu\s+)?sou\s+(?:um\s+)?"
        r"(?:homem|pai|papai|marido|rapaz|ator|cantor|blogueiro|influenciador|empreendedor|criador)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+o\s+(?!maior|melhor|unico|único|novo|primeiro|canal|tipo)"
        r"[\wÀ-ÿ'’-]+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+(?:muito\s+)?"
        r"(?:apaixonado|grato|cansado|formado|casado|divorciado|separado|"
        r"brasileiro|americano|m[eé]dico)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:criador de conte[uú]do|fundador|propriet[aá]rio)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:um pai|sou pai|pai de fam[ií]lia)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bsoy\s+(?:un\s+|el\s+)?"
        r"(?:hombre|padre|pap[aá]|esposo|chico|actor|bloguero|influenciador|emprendedor|creador)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bi(?:'m| am)\s+(?:a\s+)?"
        r"(?:man|father|dad|husband|male creator|actor|businessman)\b",
        re.IGNORECASE,
    ),
)

_FEMALE_SELF_PATTERNS = (
    re.compile(
        r"\b(?:eu\s+)?sou\s+(?:uma\s+)?"
        r"(?:mulher|m[aã]e|mam[aã]e|esposa|garota|atriz|cantora|blogueira|influenciadora|empreendedora|criadora)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+a\s+(?!maior|melhor|unica|única|nova|primeira|favor|prova|mesma|pessoa|canal|tipo)"
        r"[\wÀ-ÿ'’-]+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:eu\s+)?sou\s+(?:muito\s+)?"
        r"(?:apaixonada|grata|cansada|formada|casada|divorciada|separada|gr[aá]vida|"
        r"russa|brasileira|americana|m[eé]dica|gaiteira)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:maquiadora|criadora de conte[uú]do|criadora\s+d[aeo]s?|apresentadora|"
        r"professora|doutora|advogada|enfermeira|fundadora|propriet[aá]ria|gaiteira)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:uma m[aã]e|sou m[aã]e|m[aã]e de fam[ií]lia)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bsoy\s+(?:una\s+|la\s+)?"
        r"(?:mujer|madre|mam[aá]|esposa|chica|actriz|bloguera|influenciadora|emprendedora|creadora)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bi(?:'m| am)\s+(?:a\s+)?"
        r"(?:woman|mother|mom|wife|female creator|actress|businesswoman|makeup artist)\b",
        re.IGNORECASE,
    ),
)

_MALE_ATTRIBUTION_PATTERNS = (
    re.compile(
        r"\bcriad[oa]\s+(?:pelo|por um)\s+"
        r"(?:prof(?:essor)?\.?|senhor|sr\.?|doutor|dr\.?|homem|rapaz|garoto|"
        r"criador|apresentador|ator|cantor|chef|jogador|gamer)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bcread[oa]\s+por un\s+"
        r"(?:profesor|se[nñ]or|doctor|hombre|chico|creador|presentador|actor|cantante|chef|jugador|gamer)\b",
        re.IGNORECASE,
    ),
)

_FEMALE_ATTRIBUTION_PATTERNS = (
    re.compile(
        r"\bcriad[oa]\s+(?:pela|por uma)\s+"
        r"(?:professora|senhora|sra\.?|doutora|dra\.?|mulher|garota|criadora|apresentadora|"
        r"atriz|cantora|chef|jogadora|gamer)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bcread[oa]\s+por una\s+"
        r"(?:profesora|se[nñ]ora|doctora|mujer|chica|creadora|presentadora|actriz|cantante|chef|jugadora|gamer)\b",
        re.IGNORECASE,
    ),
)

_MALE_PRIMARY_ROLE = re.compile(
    r"\b(?:o|um|sou\s+o|sou\s+um)\s+"
    r"(?:fundador|apresentador|propriet[aá]rio|dono|"
    r"criador(?!\s+de\s+(?:tudo|deus|universo|c[eé]us?)))"
    r"(?:\s+d[aeo]s?\s+(?!tudo|deus|universo)[\wÀ-ÿ'’-]+){0,6}\b",
    re.IGNORECASE,
)
_MALE_CHANNEL_OWNER_ROLE = re.compile(
    r"\b(?:o|um)\s+"
    r"(?:fundador|apresentador|propriet[aá]rio|dono|"
    r"criador(?!\s+de\s+(?:tudo|deus|universo|c[eé]us?)))\s+"
    r"d[aeo]s?\s+(?!tudo|deus|universo)[\wÀ-ÿ'’-]+",
    re.IGNORECASE,
)
_FEMALE_PRIMARY_ROLE = re.compile(
    r"\b(?:a|uma|sou\s+a|sou\s+uma)\s+"
    r"(?:fundadora|criadora|apresentadora|propriet[aá]ria|dona)"
    r"(?:\s+d[aeo]s?\s+[\wÀ-ÿ'’-]+){0,6}\b",
    re.IGNORECASE,
)
_GUEST_ROLE_CONTEXT = re.compile(
    r"\b(?:guest|convidad[oa]|entrevistad[oa]|featuring|participa[cç][aã]o)\b",
    re.IGNORECASE,
)
_FEMALE_CHANNEL_OWNER_ROLE = re.compile(
    r"\b(?:a|uma)\s+(?:fundadora|criadora|propriet[aá]ria|dona)\s+"
    r"d[aeo]s?\s+[\wÀ-ÿ'’-]+",
    re.IGNORECASE,
)

_TEAM_PATTERNS = (
    re.compile(r"\b(?:we are|we're|n[oó]s somos|nosotros somos)\b", re.IGNORECASE),
    re.compile(
        r"\bsomos\s+(?:uma?|un[ao]?)\s+"
        r"(?:equipe|empresa|ag[eê]ncia|marca|produtora|time|grupo|canal)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:our|nosso|nossa|nuestro|nuestra)\s+"
        r"(?:team|equipe|equipo|family|fam[ií]lia|canal|channel)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:dupla|trio|casal|irm[aã]os|irm[aã]s|brothers|sisters|collective|coletivo|colectivo)\b",
        re.IGNORECASE,
    ),
)
_BRAND_PATTERN = re.compile(
    r"\b(?:ceo|empresa|company|marca|brand|loja|store|ag[eê]ncia|agency|"
    r"canal oficial|official channel|(?<!social\s)media|network|records)\b",
    re.IGNORECASE,
)
_NEGATION_PREFIX = re.compile(r"\b(?:n[aã]o|not|nunca|jamais)\s+$", re.IGNORECASE)

_AGE_PATTERNS = (
    re.compile(
        r"\b(?:i am|i'm|aged)\s+(?:a\s+)?(\d{1,2})\s*(?:years?\s*old)?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(?:tenho|tengo)\s+(\d{1,2})\s+(?:anos|años)\b", re.IGNORECASE),
    re.compile(
        r"\b(?:sou (?:um homem|uma mulher)|soy (?:un hombre|una mujer))\s+de\s+"
        r"(\d{1,2})\s+(?:anos|años)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bj['’]ai\s+(\d{1,2})\s+ans\b", re.IGNORECASE),
    re.compile(
        r"\b(?:my age is|minha idade [ée]|mi edad es|idade|age|edad)\s*[:=-]?\s*"
        r"(\d{1,2})\s*(?:anos|años|years?)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:i am|i'm|sou|soy)\s+(?:a\s+|uma?\s+|un\s+)?"
        r"(\d{1,2})[- ]years?[- ]old\b",
        re.IGNORECASE,
    ),
)
_BIRTH_YEAR_PATTERN = re.compile(
    r"\b(?:born (?:in|on)|birth(?:day| year)?\s*[:=-]?|nascid[oa] em|nací en|nasci em|"
    r"ano de nascimento\s*[:=-]?|año de nacimiento\s*[:=-]?)\s*(19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_ABOUT_CREATOR_SECTION = re.compile(
    r"(?:^|\n|//|\||📌)\s*(?:about me|sobre mim|sobre m[ií]|quem sou eu|acerca de m[ií])\s*[:=-]?",
    re.IGNORECASE,
)
_PERSONAL_INFO_SECTION = re.compile(
    r"(?:about me|sobre mim|sobre m[ií]|quem sou eu|acerca de m[ií]|"
    r"minhas informa[cç][oõ]es|informa[cç][oõ]es pessoais|"
    r"personal info(?:rmation)?)",
    re.IGNORECASE,
)
_FIRST_PERSON_AGE = re.compile(
    r"\b(?:i am|i'm|my age is|tenho|tengo|minha idade [ée]|mi edad es|idade|"
    r"sou (?:um homem|uma mulher)|soy (?:un hombre|una mujer)|j['’]ai)\b",
    re.IGNORECASE,
)
_FIRST_PERSON_BIRTH = re.compile(
    r"\b(?:i\s+(?:was\s+)?born\s+(?:in|on)|(?:eu\s+)?nasci\s+em|"
    r"(?:yo\s+)?nac[ií]\s+en)\b",
    re.IGNORECASE,
)
_REPORTED_SPEECH_PREFIX = re.compile(
    r"\b(?:guest|convidad[oa]|entrevistad[oa])"
    r"(?:\s+[\wÀ-ÿ'’-]+){0,4}\s+"
    r"(?:says?|said|diz|disse|falou|afirma|conta|explains?)\s*[:,-]?\s*[\"'“‘]?\s*$|"
    r"\b(?:he|she|ele|ela)\s+"
    r"(?:says?|said|diz|disse|falou|afirma|conta|explains?)\s*[:,-]?\s*[\"'“‘]?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CreatorTextSource:
    ref: str
    source_type: str
    source_family: str
    text: str
    observed_at: datetime


@dataclass(frozen=True)
class GenderEvidenceEvent:
    candidate: str
    subject: str
    claim_type: str
    source_ref: str
    source_family: str
    span: str
    template_hash: str
    target_relation: str = "primary_creator"
    duplicate_count: int = 1


@dataclass(frozen=True)
class AgeEvidenceEvent:
    claim_type: str
    value: int
    subject: str
    source_ref: str
    source_family: str
    span: str
    observed_at: datetime
    template_hash: str
    target_relation: str = "primary_creator"
    duplicate_count: int = 1


@dataclass(frozen=True)
class CreatorEvidence:
    account_entity_type: str
    primary_creator_status: str
    primary_creator_subject: str
    gender_events: tuple[GenderEvidenceEvent, ...]
    age_events: tuple[AgeEvidenceEvent, ...]

    @property
    def gender_candidate(self) -> str | None:
        candidates = {event.candidate for event in self.gender_events}
        return next(iter(candidates)) if len(candidates) == 1 else None

    @property
    def gender_conflict(self) -> bool:
        return len({event.candidate for event in self.gender_events}) > 1


def _template_hash(value: str) -> str:
    normalized = normalize_text(value)
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _text_sources(snapshot: ChannelSnapshot) -> tuple[CreatorTextSource, ...]:
    sources: list[CreatorTextSource] = []
    seen_channel_text: set[str] = set()
    for key in ("about_description", "summary"):
        text = str(snapshot.channel.get(key) or "").strip()
        normalized = normalize_text(text)
        if not normalized or normalized in seen_channel_text:
            continue
        seen_channel_text.add(normalized)
        sources.append(CreatorTextSource(
            ref=f"channel:{key}",
            source_type="channel_profile",
            source_family="owner_authored_channel_profile",
            text=text,
            observed_at=snapshot.as_of,
        ))
    for content in snapshot.contents:
        description = content.description.strip()
        if description:
            sources.append(CreatorTextSource(
                ref=f"content:{content.source_content_id}:description",
                source_type="content_description",
                source_family="owner_authored_content_description",
                text=description,
                observed_at=content.published_at or content.first_seen_at or snapshot.as_of,
            ))
        page = content.comments_first_page
        if page is None:
            continue
        for comment in page.comments:
            if not comment.is_channel_owner or not comment.text.strip():
                continue
            sources.append(CreatorTextSource(
                ref=f"content:{content.source_content_id}:owner_comment:{comment.comment_id}",
                source_type="owner_comment",
                source_family="channel_owner_comment",
                text=comment.text,
                observed_at=comment.published_at or page.collected_at,
            ))
    return tuple(sources)


def _clean_subject(value: str) -> str:
    return " ".join(value.strip(" \t\r\n,;:!.-").split())


def _subject_from_text(text: str) -> str:
    for pattern in _SELF_NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            subject = _clean_subject(match.group(1))
            if 2 <= len(subject) <= 80:
                return subject
    called = re.search(r"\bchamad[oa]\s+([\wÀ-ÿ'’-]+(?:\s+[\wÀ-ÿ'’-]+){0,3})", text, re.IGNORECASE)
    return _clean_subject(called.group(1)) if called else ""


def _same_subject(left: str, right: str) -> bool:
    return bool(left and right and normalize_text(left) == normalize_text(right))


def _trusted_source_subject(source: CreatorTextSource) -> str:
    subject = _subject_from_text(source.text)
    if not subject:
        return ""
    if source.source_type in {"channel_profile", "owner_comment"}:
        return subject
    if source.source_type == "content_description" and _ABOUT_CREATOR_SECTION.search(source.text):
        return subject
    return ""


def _content_source_is_creator_bound(source: CreatorTextSource, primary_subject: str) -> bool:
    if source.source_type != "content_description":
        return True
    if _ABOUT_CREATOR_SECTION.search(source.text):
        return True
    return _same_subject(_subject_from_text(source.text), primary_subject)


def _role_source_allowed(source: CreatorTextSource, primary_subject: str) -> bool:
    if source.source_type in {"channel_profile", "owner_comment"}:
        return True
    if _content_source_is_creator_bound(source, primary_subject):
        return True
    return bool(primary_subject)


def _is_reported_speech(text: str, match: re.Match[str]) -> bool:
    prefix = text[max(0, match.start() - 100):match.start()]
    return bool(_REPORTED_SPEECH_PREFIX.search(prefix))


def _is_negated_claim(text: str, match: re.Match[str]) -> bool:
    prefix = text[max(0, match.start() - 12):match.start()]
    return bool(_NEGATION_PREFIX.search(prefix))


def _event(
    candidate: str,
    subject: str,
    claim_type: str,
    source: CreatorTextSource,
    match: re.Match[str],
) -> GenderEvidenceEvent:
    span = match.group(0).strip()
    return GenderEvidenceEvent(
        candidate=candidate,
        subject=subject,
        claim_type=claim_type,
        source_ref=source.ref,
        source_family=source.source_family,
        span=span,
        template_hash=_template_hash(f"{candidate}:{claim_type}:{span}"),
    )


def _gender_events(
    sources: Iterable[CreatorTextSource],
    primary_subject: str,
    fallback_label: str,
) -> tuple[GenderEvidenceEvent, ...]:
    raw: list[GenderEvidenceEvent] = []
    for source in sources:
        source_subject = _subject_from_text(source.text) or primary_subject or fallback_label
        if _content_source_is_creator_bound(source, primary_subject):
            for candidate, patterns in (
                ("male", _MALE_SELF_PATTERNS),
                ("female", _FEMALE_SELF_PATTERNS),
            ):
                for pattern in patterns:
                    for match in pattern.finditer(source.text):
                        if _is_reported_speech(source.text, match) or _is_negated_claim(source.text, match):
                            continue
                        raw.append(_event(candidate, source_subject, "explicit_self_identity", source, match))
        if _content_source_is_creator_bound(source, primary_subject):
            for candidate, patterns in (
                ("male", _MALE_ATTRIBUTION_PATTERNS),
                ("female", _FEMALE_ATTRIBUTION_PATTERNS),
            ):
                for pattern in patterns:
                    for match in pattern.finditer(source.text):
                        raw.append(_event(candidate, source_subject, "explicit_creator_attribution", source, match))
        # Role words may sit in a different video than the self-introduction.
        # Unbound descriptions may only use "fundadora/criadora da X" style
        # owner roles, not a bare guest mention. Channel title is not a
        # real primary subject and must not unlock story-character roles.
        if _role_source_allowed(source, primary_subject):
            bound = _content_source_is_creator_bound(source, primary_subject) or (
                source.source_type in {"channel_profile", "owner_comment"}
            )
            role_patterns = (
                (("male", _MALE_PRIMARY_ROLE), ("female", _FEMALE_PRIMARY_ROLE))
                if bound
                else (("male", _MALE_CHANNEL_OWNER_ROLE), ("female", _FEMALE_CHANNEL_OWNER_ROLE))
            )
            for candidate, pattern in role_patterns:
                for match in pattern.finditer(source.text):
                    window = source.text[max(0, match.start() - 40):match.end() + 40]
                    if _is_reported_speech(source.text, match) or _GUEST_ROLE_CONTEXT.search(window):
                        continue
                    raw.append(_event(candidate, source_subject, "explicit_role_attribution", source, match))

    grouped: dict[tuple[str, str, str, str], list[GenderEvidenceEvent]] = {}
    for event in raw:
        key = (event.candidate, normalize_text(event.subject), event.claim_type, event.template_hash)
        grouped.setdefault(key, []).append(event)
    deduplicated: list[GenderEvidenceEvent] = []
    for events in grouped.values():
        first = min(events, key=lambda item: item.source_ref)
        deduplicated.append(GenderEvidenceEvent(
            **{**first.__dict__, "duplicate_count": len(events)}
        ))
    return tuple(sorted(deduplicated, key=lambda item: (item.candidate, item.claim_type, item.source_ref)))


def _age_match_is_creator_bound(
    source: CreatorTextSource,
    match: re.Match[str],
    primary_subject: str,
    *,
    birth_year: bool,
) -> bool:
    if _is_reported_speech(source.text, match):
        return False
    window = source.text[max(0, match.start() - 40):match.end() + 40]
    if _GUEST_ROLE_CONTEXT.search(window):
        return False
    local_subject = _subject_from_text(source.text)
    personal_info = bool(
        _ABOUT_CREATOR_SECTION.search(source.text)
        or _PERSONAL_INFO_SECTION.search(source.text)
    )
    has_subject_binding = personal_info or _same_subject(local_subject, primary_subject)
    context = source.text[max(0, match.start() - 24):match.end() + 8]
    first_person = bool(
        (_FIRST_PERSON_BIRTH if birth_year else _FIRST_PERSON_AGE).search(context)
    )
    if source.source_type == "content_description":
        if personal_info:
            return True
        if first_person and primary_subject:
            return True
        return has_subject_binding and first_person
    return first_person or has_subject_binding


def _age_events(
    sources: Iterable[CreatorTextSource],
    primary_subject: str,
    fallback_subject: str,
) -> tuple[AgeEvidenceEvent, ...]:
    raw: list[AgeEvidenceEvent] = []
    for source in sources:
        subject = _subject_from_text(source.text) or primary_subject or fallback_subject
        for pattern in _AGE_PATTERNS:
            for match in pattern.finditer(source.text):
                age = int(match.group(1))
                if 13 <= age <= 90 and _age_match_is_creator_bound(
                    source, match, primary_subject, birth_year=False
                ):
                    span = match.group(0).strip()
                    raw.append(AgeEvidenceEvent(
                        claim_type="explicit_age",
                        value=age,
                        subject=subject,
                        source_ref=source.ref,
                        source_family=source.source_family,
                        span=span,
                        observed_at=source.observed_at,
                        template_hash=_template_hash(f"explicit_age:{span}"),
                    ))
        for match in _BIRTH_YEAR_PATTERN.finditer(source.text):
            if not _age_match_is_creator_bound(
                source, match, primary_subject, birth_year=True
            ):
                continue
            year = int(match.group(1))
            span = match.group(0).strip()
            raw.append(AgeEvidenceEvent(
                claim_type="birth_year",
                value=year,
                subject=subject,
                source_ref=source.ref,
                source_family=source.source_family,
                span=span,
                observed_at=source.observed_at,
                template_hash=_template_hash(f"birth_year:{span}"),
            ))

    grouped: dict[tuple[str, int, str, str], list[AgeEvidenceEvent]] = {}
    for event in raw:
        grouped.setdefault(
            (event.claim_type, event.value, normalize_text(event.subject), event.template_hash),
            [],
        ).append(event)
    deduplicated: list[AgeEvidenceEvent] = []
    for events in grouped.values():
        latest = max(events, key=lambda item: (item.observed_at, item.source_ref))
        deduplicated.append(AgeEvidenceEvent(
            **{**latest.__dict__, "duplicate_count": len(events)}
        ))
    return tuple(sorted(deduplicated, key=lambda item: (item.claim_type, item.observed_at, item.source_ref)))


def extract_creator_evidence(snapshot: ChannelSnapshot) -> CreatorEvidence:
    sources = _text_sources(snapshot)
    subjects = [subject for source in sources if (subject := _trusted_source_subject(source))]
    primary_subject = subjects[0] if subjects else ""
    channel_identity_text = " ".join(
        str(snapshot.channel.get(key) or "")
        for key in ("title", "summary", "about_description")
    )
    if _BRAND_PATTERN.search(channel_identity_text):
        account_entity_type = "brand_or_team"
    elif any(pattern.search(channel_identity_text) for pattern in _TEAM_PATTERNS):
        account_entity_type = "brand_or_team"
    elif primary_subject:
        account_entity_type = "individual"
    else:
        account_entity_type = "unknown"
    gender_events = _gender_events(
        sources,
        primary_subject,
        str(snapshot.channel.get("title") or ""),
    )
    if not primary_subject and gender_events:
        primary_subject = gender_events[0].subject
    age_events = _age_events(
        sources,
        primary_subject,
        str(snapshot.channel.get("title") or ""),
    )
    primary_status = "single_stable" if primary_subject or gender_events else "unknown"
    return CreatorEvidence(
        account_entity_type=account_entity_type,
        primary_creator_status=primary_status,
        primary_creator_subject=primary_subject,
        gender_events=gender_events,
        age_events=age_events,
    )
