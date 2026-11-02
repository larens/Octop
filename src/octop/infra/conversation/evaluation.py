"""Offline evaluation helpers for the shadow Router classifier.

The low-confidence second stage is explicit evaluation code. It is never wired
into dashboard turns, and it has both call-count and token budgets so an
evaluation run cannot silently become an unbounded model workload.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from octop.infra.conversation.shadow import classify_shadow_route


@dataclass(frozen=True)
class ClassificationSample:
    text: str
    expected: str
    has_attachments: bool = False
    target_agent_ids: tuple[str, ...] = ()


def evaluate_classifier(
    samples: Sequence[ClassificationSample],
    *,
    confidence_threshold: float = 0.65,
    decisions: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return overall accuracy and the safety-critical side-effect miss rate."""
    total = 0
    correct = 0
    side_effect_total = 0
    side_effect_missed = 0
    low_confidence = 0
    rows: list[dict[str, Any]] = []

    for sample in samples:
        decision = (
            decisions.get(sample.text)
            if decisions is not None
            else classify_shadow_route(
                sample.text,
                has_attachments=sample.has_attachments,
                target_agent_ids=sample.target_agent_ids,
            )
        )
        if decision is None:
            continue
        predicted = str(decision.get("intent") or "")
        confidence = float(decision.get("confidence") or 0.0)
        total += 1
        is_correct = predicted == sample.expected
        correct += int(is_correct)
        if float(confidence) < confidence_threshold:
            low_confidence += 1
        if sample.expected == "side_effect_task":
            side_effect_total += 1
            if predicted != sample.expected:
                side_effect_missed += 1
        rows.append(
            {
                "text": sample.text,
                "expected": sample.expected,
                "predicted": predicted,
                "confidence": confidence,
                "correct": is_correct,
            }
        )

    return {
        "total": total,
        "correct": correct,
        "accuracy": (correct / total) if total else 0.0,
        "side_effect_total": side_effect_total,
        "side_effect_missed": side_effect_missed,
        "side_effect_miss_rate": (
            side_effect_missed / side_effect_total if side_effect_total else 0.0
        ),
        "low_confidence_count": low_confidence,
        "confidence_threshold": confidence_threshold,
        "rows": rows,
        "dispatch_enabled": False,
    }


async def evaluate_low_confidence_with_llm(
    samples: Sequence[ClassificationSample],
    classify_async: Callable[[str], Awaitable[Mapping[str, Any]]],
    *,
    confidence_threshold: float = 0.65,
    max_calls: int = 20,
    token_budget: int = 8_000,
    estimated_tokens_per_call: int = 400,
) -> dict[str, Any]:
    """Run a bounded second-stage classifier over low-confidence samples only.

    ``classify_async`` is supplied by the caller and may return ``tokens`` to
    report actual usage. Budget is checked before each call, so the hard limits
    hold even when the callback overruns its estimate on the final call.
    """
    call_limit = max(0, int(max_calls))
    budget_limit = max(0, int(token_budget))
    estimate = max(1, int(estimated_tokens_per_call))
    decisions: dict[str, Mapping[str, Any]] = {}
    calls = 0
    estimated_tokens = 0
    actual_tokens = 0
    skipped_for_budget = 0

    for sample in samples:
        first = classify_shadow_route(
            sample.text,
            has_attachments=sample.has_attachments,
            target_agent_ids=sample.target_agent_ids,
        )
        decisions[sample.text] = first
        if float(first.get("confidence") or 0.0) >= confidence_threshold:
            continue
        if calls >= call_limit or estimated_tokens + estimate > budget_limit:
            skipped_for_budget += 1
            continue
        second = await classify_async(sample.text)
        calls += 1
        estimated_tokens += estimate
        try:
            actual_tokens += max(0, int(second.get("tokens", estimate)))
        except (TypeError, ValueError):
            actual_tokens += estimate
        decisions[sample.text] = second

    metrics = evaluate_classifier(
        samples,
        confidence_threshold=confidence_threshold,
        decisions=decisions,
    )
    metrics.update(
        {
            "second_stage_calls": calls,
            "second_stage_estimated_tokens": estimated_tokens,
            "second_stage_actual_tokens": actual_tokens,
            "second_stage_token_budget": budget_limit,
            "second_stage_max_calls": call_limit,
            "second_stage_skipped_for_budget": skipped_for_budget,
            "dispatch_enabled": False,
        }
    )
    return metrics


__all__ = [
    "ClassificationSample",
    "evaluate_classifier",
    "evaluate_low_confidence_with_llm",
]
