from __future__ import annotations

from typing import Iterable

CORE_GENERAL_COMMENT_TEMPLATES: tuple[str, ...] = (
    "Sharp piece. The way you framed the tradeoff made the idea stick.",
    "Thoughtful read. The main point landed without feeling overstated.",
    "Strong post. You kept it practical without flattening the nuance.",
    "I liked the pacing here. The idea unfolded in a very clear way.",
    "Clean argument. The examples gave the takeaway real weight.",
    "This opened up a useful angle on a familiar topic.",
    "Concise, grounded, and memorable. Nicely done.",
    "Good read. The framing makes the takeaway easy to carry forward.",
)

EXTENDED_GENERAL_COMMENT_TEMPLATES: tuple[str, ...] = (
    "The structure here makes the insight feel earned rather than stated.",
    "There is a calm confidence to this piece that works well.",
    "I appreciated the balance between reflection and practicality here.",
    "This is one of those posts that keeps unfolding after the last paragraph.",
    "The voice in this piece feels clear and deliberate. That made it easy to stay with.",
    "Thoughtful post. The takeaway feels usable beyond the immediate topic.",
    "I liked how this connected a specific observation to a bigger pattern.",
    "Useful perspective. You gave the idea room to breathe without losing focus.",
)

GUIDE_COMMENT_TEMPLATES: tuple[str, ...] = (
    "Clear walkthrough. The step-by-step structure made this easy to follow.",
    "Useful breakdown. The sequence of ideas keeps the whole post very accessible.",
    "This is practical in the best way. The structure makes it easy to apply.",
    "Strong guide. You made the process feel clear without oversimplifying it.",
)

REFLECTION_COMMENT_TEMPLATES: tuple[str, ...] = (
    "Thoughtful reflection. The personal angle adds weight to the takeaway.",
    "This feels honest and well-observed. The reflection never drifts into abstraction.",
    "Strong reflective piece. The insight feels grounded because the writing stays specific.",
    "I liked the emotional restraint here. It lets the point land more cleanly.",
)

BUILDER_COMMENT_TEMPLATES: tuple[str, ...] = (
    "Useful builder perspective here. The practical framing works.",
    "Solid execution-focused post. The examples make the idea land.",
    "Good read. The operational angle keeps this especially useful.",
    "This has a nice builder mindset to it. Clear, concrete, and applicable.",
)

DEFAULT_PRE_FOLLOW_COMMENT_TEMPLATES: tuple[str, ...] = CORE_GENERAL_COMMENT_TEMPLATES
DEFAULT_PRE_FOLLOW_COMMENT_TEMPLATES_RAW = "||".join(DEFAULT_PRE_FOLLOW_COMMENT_TEMPLATES)


def _dedupe_preserving_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _clip_excerpt(text: str | None, *, limit: int = 72) -> str | None:
    if not text:
        return None
    normalized = " ".join(text.split()).strip(" .,:;!?\"'()[]{}")
    if not normalized:
        return None
    if len(normalized) <= limit:
        return normalized
    clipped = normalized[:limit].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0]
    return clipped.strip(" .,:;!?\"'()[]{}") or None


def build_comment_template_pool(
    *,
    candidate_title: str | None,
    candidate_bio: str | None,
    post_lead_text: str | None = None,
    post_closing_text: str | None = None,
    base_templates: list[str] | None = None,
) -> list[str]:
    lead_excerpt = _clip_excerpt(post_lead_text)
    closing_excerpt = _clip_excerpt(post_closing_text)
    text = f"{candidate_title or ''} {candidate_bio or ''} {lead_excerpt or ''} {closing_excerpt or ''}".lower()
    templates: list[str] = list(base_templates or DEFAULT_PRE_FOLLOW_COMMENT_TEMPLATES)
    templates.extend(EXTENDED_GENERAL_COMMENT_TEMPLATES)

    if lead_excerpt:
        templates.extend(
            (
                f'The opening point around "{lead_excerpt}" made the rest very easy to follow.',
                f'I liked how you opened with "{lead_excerpt}" and then built from it.',
            )
        )
    if closing_excerpt:
        templates.extend(
            (
                f'The closing line on "{closing_excerpt}" landed cleanly.',
                f'Your ending around "{closing_excerpt}" tied the post together well.',
            )
        )

    if any(token in text for token in ("guide", "how to", "checklist", "framework", "tutorial", "step")):
        templates.extend(GUIDE_COMMENT_TEMPLATES)
    elif any(token in text for token in ("lesson", "story", "reflection", "learned", "mistake", "journey")):
        templates.extend(REFLECTION_COMMENT_TEMPLATES)
    elif any(token in text for token in ("build", "engineer", "product", "startup", "code", "python", "ai")):
        templates.extend(BUILDER_COMMENT_TEMPLATES)

    return _dedupe_preserving_order(templates)
