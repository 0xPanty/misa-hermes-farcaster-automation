#!/usr/bin/env python3
"""Hermes-native Farcaster autonomous operator for Misa.

This module is the local operator brain. It decides what Misa would do on
Farcaster, drafts the text, records operator memory, and emits an x402 publisher
packet. It does not submit casts by itself; the live submit boundary belongs to
the external publisher.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


SCHEMA_CONFIG = "misa.hermes.farcaster.autonomy_config.v1"
SCHEMA_EVENT = "misa.hermes.farcaster.event.v1"
SCHEMA_DECISION = "misa.hermes.farcaster.decision.v1"
SCHEMA_DRAFT = "misa.hermes.farcaster.draft.v1"
SCHEMA_PACKET = "misa.hermes.farcaster.publisher_packet.v1"
SCHEMA_RESULT = "misa.hermes.farcaster.autonomous_operator.result.v1"
SCHEMA_TOPIC_MEMORY = "misa.hermes.farcaster.topic_memory.v1"
SCHEMA_RELATIONSHIP_MEMORY = "misa.hermes.farcaster.relationship_memory.v1"
SCHEMA_OUTCOME = "misa.hermes.farcaster.outcome.v1"
SCHEMA_RULE_REGISTRY = "misa.hermes.farcaster.rule_registry.v1"
SCHEMA_STATE_TRANSITION = "misa.hermes.farcaster.state_transition.v1"
SCHEMA_DAILY_REPORT = "misa.hermes.farcaster.daily_report.v1"
SCHEMA_MCP_MANIFEST = "misa.hermes.farcaster.mcp_manifest.v1"
SCHEMA_EXPRESSION_PRECHECK = "misa.hermes.farcaster.expression_precheck.v1"
SCHEMA_SIGNAL_DIGEST = "misa.hermes.farcaster.signal_digest.v1"
SCHEMA_AI_SECOND_PASS_PACKET = "misa.hermes.farcaster.ai_second_pass_packet.v1"
SCHEMA_AI_SECOND_PASS_RESULT = "misa.hermes.farcaster.ai_second_pass_result.v1"
SCHEMA_TOPIC_ATTENTION = "misa.hermes.farcaster.topic_attention.v1"
SCHEMA_NEYNAR_FETCH_PLAN = "misa.hermes.farcaster.neynar_readonly_fetch_plan.v1"
SCHEMA_NEYNAR_FETCHER_RUN = "misa.hermes.farcaster.neynar_readonly_fetcher_run.v1"
SCHEMA_NEYNAR_INGEST = "misa.hermes.farcaster.neynar_readonly_ingest.v1"
SCHEMA_WEBHOOK_INGEST = "misa.hermes.farcaster.webhook_ingest.v1"
SCHEMA_SCHEDULER_TICK = "misa.hermes.farcaster.scheduler_tick.v1"
SCHEMA_SEND_AUDIT = "misa.hermes.farcaster.send_audit.v1"
SCHEMA_OPERATOR_QUALITY = "misa.hermes.farcaster.operator_quality.v1"
SCHEMA_TOPIC_HEAT = "misa.hermes.farcaster.topic_heat.v1"
SCHEMA_AI_SECOND_PASS_PROVIDER_ADAPTER = "misa.hermes.farcaster.ai_second_pass_provider_adapter.v1"
SCHEMA_DRY_RUN_AUTOMATION_CYCLE = "misa.hermes.farcaster.dry_run_automation_cycle.v1"

DEFAULT_STATE_ROOT = Path("state") / "farcaster"
MISA_FID = 2833742
MISA_USERNAME = "misabot"
OPERATOR_VERSION = "1.4-local"

STATE_FILES = {
    "config": "autonomy-config.json",
    "operator_state": "operator-state.json",
    "candidate_queue": "candidate-queue.jsonl",
    "decision_log": "decision-log.jsonl",
    "draft_log": "draft-log.jsonl",
    "publish_queue": "publish-queue.jsonl",
    "send_audit_log": "send-audit-log.jsonl",
    "neynar_fetch_plan_log": "neynar-fetch-plans.jsonl",
    "neynar_fetcher_run_log": "neynar-fetcher-runs.jsonl",
    "provider_ingest_log": "provider-ingest-log.jsonl",
    "webhook_ingest_log": "webhook-ingest-log.jsonl",
    "scheduler_tick_log": "scheduler-ticks.jsonl",
    "processed_casts": "processed-casts.jsonl",
    "thread_state": "thread-state.jsonl",
    "interaction_log": "interaction-log.jsonl",
    "outcomes": "outcomes.jsonl",
    "state_transitions": "state-transitions.jsonl",
    "daily_reports": "daily-reports.jsonl",
    "signal_digest_log": "signal-digests.jsonl",
    "ai_second_pass_log": "ai-second-pass.jsonl",
    "ai_second_pass_adapter_log": "ai-second-pass-adapter.jsonl",
    "automation_cycle_log": "automation-cycles.jsonl",
    "topic_memory": "topic-memory.json",
    "topic_attention": "topic-attention.json",
    "relationship_memory": "relationship-memory.json",
    "rule_registry": "rule-registry.json",
    "style_memory": "style-memory.md",
    "distill_candidates": "distill-candidates.jsonl",
}

STATE_DIRS = [
    "decisions",
    "drafts",
    "publish-results",
    "distill-candidates",
    "manual-send-packets",
    "manual-send-approvals",
]

ALLOWED_SOCIAL_ACTIONS = {"cast", "reply", "quote"}

OPERATOR_LAYERS = [
    {
        "name": "sense",
        "job": "normalize Farcaster event or operator-memory prompt into public-safe signal",
        "writes": ["candidate_queue"],
    },
    {
        "name": "think",
        "job": "score topic, choose action, and bind the decision to a mode",
        "writes": ["decision_log"],
    },
    {
        "name": "speak",
        "job": "draft useful-first Misa voice for cast, reply, or quote",
        "writes": ["draft_log"],
    },
    {
        "name": "precheck",
        "job": "apply Farcaster rules, public-safe memory rules, and read-only cybernetic contract",
        "writes": ["state_transitions"],
    },
    {
        "name": "publish_packet",
        "job": "emit x402 packet for the external publisher boundary without submitting",
        "writes": ["publish_queue"],
    },
    {
        "name": "learn",
        "job": "record interactions, relationships, topic signals, and outcomes",
        "writes": ["interaction_log", "topic_memory", "relationship_memory", "outcomes"],
    },
]

OPERATOR_MODES = {
    "mention_reply": {
        "signals": ["mentions_misa", "replies_to_misa"],
        "default_action": "reply",
        "layer_path": ["sense", "think", "speak", "precheck", "publish_packet", "learn"],
    },
    "thread_participation": {
        "signals": ["conversation_update", "thread_signal", "parent_hash"],
        "default_action": "reply",
        "layer_path": ["sense", "think", "speak", "precheck", "publish_packet", "learn"],
    },
    "proactive_topic_cast": {
        "signals": ["trending_candidate", "hot_topic", "channel_candidate"],
        "default_action": "cast",
        "layer_path": ["sense", "think", "speak", "precheck", "publish_packet", "learn"],
    },
    "memory_cast": {
        "signals": ["operator_memory", "memory_prompt"],
        "default_action": "cast",
        "layer_path": ["sense", "think", "speak", "precheck", "publish_packet", "learn"],
    },
}

STATE_MACHINE = {
    "created": ["sensed"],
    "sensed": ["decided"],
    "decided": ["drafted", "skipped"],
    "drafted": ["prechecked"],
    "prechecked": ["queued_for_publisher", "blocked"],
    "queued_for_publisher": ["awaiting_external_publisher"],
    "awaiting_external_publisher": ["submitted", "dry_run_complete"],
    "dry_run_complete": ["outcome_recorded"],
    "submitted": ["verified", "unverified"],
    "verified": ["outcome_recorded"],
    "unverified": ["outcome_recorded"],
    "blocked": ["outcome_recorded"],
    "skipped": ["outcome_recorded"],
}
HIGH_RISK_ACTION_WORDS = [
    "poidh",
    "bounty",
    "winner",
    "accept claim",
    "create claim",
    "withdraw",
    "settle",
    "transfer",
    "private key",
    "seed phrase",
    "profile update",
    "delete cast",
]

SECRET_PATTERNS = [
    re.compile(r"\b(?:api[_-]?key|secret|token|mnemonic|private[_-]?key)\b", re.I),
    re.compile(r"\bsigner[_-]?(?:key|secret|token)\b", re.I),
    re.compile(r"\bNEYNAR_API_KEY\b", re.I),
    re.compile(r"\b[A-Za-z0-9_\-]*sk-[A-Za-z0-9_\-]{12,}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\b0x[a-fA-F0-9]{40}\b"),
    re.compile(r"\[ACTION:[^\]]+\]", re.I),
]

PRIVATE_MEMORY_PATTERNS = [
    re.compile(r"raw full[- ]?memory", re.I),
    re.compile(r"full_memory", re.I),
    re.compile(r"private discord", re.I),
    re.compile(r"agentmail private", re.I),
    re.compile(r"/root/\.openclaw", re.I),
    re.compile(r"farcaster-credentials\.json", re.I),
]

PRIVATE_EXPRESSION_MARKER_PATTERNS = [
    re.compile(r"</?(?:mood|reflect|private[-_ ]?precheck|internal[-_ ]?voice)[^>]*>", re.I),
    re.compile(r"\b(?:Vibe|Sparks|Reflections|Will|Premise|Conduct|Act):", re.I),
]

TOPIC_KEYWORDS = {
    "autonomy": ["autonomy", "autonomous", "agent", "operator", "self run", "自主", "自动运营"],
    "hermes": ["hermes", "misa", "openclaw", "migration", "runtime", "记忆", "迁移"],
    "farcaster": ["farcaster", "cast", "reply", "thread", "neynar", "hub", "x402"],
    "proof": ["proof", "receipt", "audit", "evidence", "verify", "证明", "证据"],
    "builder": ["builder", "build", "shipping", "product", "开源", "开发"],
}

DEV_RELEVANCE_KEYWORDS = [
    "api",
    "sdk",
    "webhook",
    "frames",
    "frame",
    "mini app",
    "snapchain",
    "hub",
    "neynar",
    "x402",
    "farcaster",
    "protocol",
    "client",
    "developer",
    "dev",
    "builder",
    "build",
    "ship",
    "shipping",
    "github",
    "open source",
    "oss",
    "docs",
    "bug",
    "release",
    "deploy",
    "infra",
    "runtime",
    "agent",
    "autonomous",
    "operator",
    "memory",
    "receipt",
    "proof",
    "eip",
    "ethereum",
    "base",
]

CONSTRUCTIVE_KEYWORDS = [
    "how",
    "why",
    "what should",
    "tradeoff",
    "proposal",
    "spec",
    "roadmap",
    "migration",
    "implementation",
    "architecture",
    "debug",
    "fix",
    "launch",
    "testing",
    "benchmark",
    "security",
]

PROMO_OR_TOKEN_PATTERNS = [
    re.compile(r"\$[a-z0-9]{2,12}\b", re.I),
    re.compile(r"\b(?:token|airdrop|auction|winner|winning bid|claim|mint|giveaway|tip allocation|tips?|gambl|jackpot)\b", re.I),
    re.compile(r"\b(?:buy now|pump|moon|degen play|alpha call)\b", re.I),
]

LOW_SUBSTANCE_SOCIAL_PATTERNS = [
    re.compile(r"\b(?:drunk|wedding|quit smoking|gm|gn|find the .*difference|daily check|good morning)\b", re.I),
    re.compile(r"\b(?:laliga|champions league|super cup|getting married)\b", re.I),
    re.compile(
        r"\b(?:my current stats|my base wallet score|base wallet score|total points|mainnet rank|multiplier|"
        r"badges|wallet analyzer|built from live .*activity|can you beat my score|created with @?neynar app studio)\b",
        re.I,
    ),
]

GENERIC_BUILDER_HYPE_PATTERNS = [
    re.compile(r"\b(?:quietly becoming the default home|default home for builders|fast\.?\s+cheap\.?\s+composable)\b", re.I),
    re.compile(r"\b(?:real distribution with real onchain rails|next wave of crypto apps|social by default)\b", re.I),
    re.compile(r"\b(?:from mini apps to social tokens|what are you building|builders on farcaster)\b", re.I),
    re.compile(r"\b(?:winning the dream of decentral(?:ized|ised) social|creator(?:s)? real distribution)\b", re.I),
]

GENERIC_AI_PROMO_PATTERNS = [
    re.compile(r"\b(?:discover .{0,24}the future of|future of onchain growth|powerful layer 2 blockchain)\b", re.I),
    re.compile(r"\b(?:faster,\s*cheaper,?\s*and more accessible|low transaction fees|high speed\s*&\s*scalability)\b", re.I),
    re.compile(r"\b(?:secure infrastructure powered by ethereum|users and developers can experience)\b", re.I),
    re.compile(r"\b(?:ever wonder how .* evolves|it's all thanks to the .* process|proposals start as drafts)\b", re.I),
    re.compile(r"\b(?:rigorous discussion and testing|benefit everyone|fascinating look at decentralized decision-making)\b", re.I),
    re.compile(r"\b(?:why .* are winning|powerhouse trinity|redefined what it means to be a \"?builder\"?)\b", re.I),
]

QUALITY_EVENT_PATTERNS = [
    re.compile(r"\b(?:sybil|bot accounts?|fake accounts?|farmers?|social rewards?|snap social rewards)\b", re.I),
    re.compile(r"\b(?:datasets?|network analysis|data scientists?|clear evidence|incentives|growth at all costs)\b", re.I),
    re.compile(r"\b(?:centralizing force|protocol centralization|open social protocols?|real growth)\b", re.I),
]

STRONG_DEV_MECHANICS_PATTERNS = [
    re.compile(
        r"\b(?:managed signers?|dedicated signers?|developer managed signers?|webhook signatures?|hmac|"
        r"siwf|siwn|fid|fname|cast/conversation|x-api-key|api key|rate limits?|sdk|endpoint|"
        r"feed/user/casts|user/bulk|snapchain|frames? v2|x402|hub sync|publisher receipts?)\b",
        re.I,
    ),
    re.compile(r"\b(?:bug|error|fix|debug|setup|integrat(?:e|ion))\b", re.I),
]

TECH_INTENT_PATTERNS = [
    re.compile(
        r"\b(?:how|what|why|can|does|where|when|help|issue|bug|error|fix|setup|integrat|build|use|work|"
        r"difference|compare|comparison|vs\.?|versus|limit|which should|which one)\b",
        re.I,
    ),
]

CODE_OR_API_SHAPE_PATTERN = re.compile(
    r"`[^`]+`|/[a-z0-9_./:-]+|[a-z0-9_-]+\.[a-z]{2,}/[a-z0-9_./:-]+|"
    r"\b(?:http|https|curl|sdk|api|endpoint|header|payload|verify|signature|auth flow|login flow)\b",
    re.I,
)

AUTHOR_DEV_KEYWORDS = [
    "engineer",
    "developer",
    "devrel",
    "builder",
    "founder",
    "cto",
    "protocol",
    "infra",
    "research",
    "farcaster",
    "base",
    "ethereum",
    "open source",
    "oss",
    "github",
    "ai agent",
    "agent",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def stable_id(prefix: str, *parts: Any) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:18]
    return f"{prefix}_{digest}"


def stable_hash(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def state_path(state_root: Path, key: str) -> Path:
    return state_root / STATE_FILES[key]


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return default
    return json.loads(text)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    if limit is not None:
        return records[-limit:]
    return records


def default_config() -> dict[str, Any]:
    return {
        "schema": SCHEMA_CONFIG,
        "mode": "autonomous_social",
        "created_for": "misa_farcaster_autonomous_operator_v1_4",
        "operator_version": OPERATOR_VERSION,
        "misa": {
            "fid": MISA_FID,
            "username": MISA_USERNAME,
        },
        "channels": ["base", "openclaw", "farcaster", "ai"],
        "scheduled_scan": {
            "enabled": True,
            "cadence_hours": 4,
            "max_write_actions_per_run": 3,
            "content_sources": ["mentions", "replies", "threads", "hot_topics", "operator_memory"],
            "scheduler_authority": "external_timer_or_manual_call_only",
        },
        "presence_budget": {
            "enabled": True,
            "cadence_style": "soft_presence_not_hard_quota",
            "daily_min_cast": 1,
            "soft_floor_source": "operator_memory",
            "avoid_forced_posting": True,
            "soft_floor_min_score": 0.50,
            "minimum_quality_score": 0.38,
            "high_signal_heat": 0.82,
            "high_signal_author_score": 0.65,
            "high_signal_extra_actions": 3,
            "high_signal_max_actions_per_run": 6,
            "quote_min_score": 0.78,
            "quote_live_policy": "dry_run_observe_first",
            "quality_first_over_heat": True,
            "max_hot_topic_age_hours": 24,
            "stale_hot_topic_action": "observe",
        },
        "signal_digest": {
            "enabled": True,
            "fetch_cadence_hours": 3,
            "digest_slots_local": ["10:00", "16:00", "22:00"],
            "max_candidates_for_misa": 8,
            "raw_json_to_misa": False,
            "candidate_shape": [
                "source",
                "channel_id",
                "topic",
                "author_score",
                "engagement",
                "heat",
                "text_excerpt",
            ],
            "llm_call_policy": "only_after_local_score_and_dedupe",
        },
        "ai_second_pass": {
            "enabled": True,
            "provider_call": "external_or_separate_worker_only",
            "llm_call_policy": "after_script_filter_only",
            "provider_adapter": {
                "mode": "local_dry_run_only",
                "provider_call": "not_called",
                "network_allowed": False,
                "secrets_allowed": False,
                "fallback_policy": "observe_missing_or_low_confidence",
            },
            "max_candidates": 12,
            "min_script_score": 0.34,
            "min_operator_fit": 0.42,
            "min_actionability_freshness": 0.35,
            "requires_paid_interaction_ready": True,
            "max_text_chars": 700,
            "max_reply_context_items": 5,
            "pass_min_confidence": 0.74,
            "allowed_verdicts": ["pass", "observe", "reject"],
            "review_focus": [
                "reject_ai_generated_marketing_or_news_rewrite",
                "reject_token_airdrop_claim_app_score_or_engagement_bait",
                "require_specific_developer_mechanics_or_real_event",
                "require_misa_can_add_value_before_x402_spend",
                "prefer_kol_dev_or_high_quality_community_discussion",
            ],
        },
        "topic_heat": {
            "enabled": True,
            "formula_version": "misa-topic-heat-v1",
            "weights": {
                "engagement": 0.30,
                "velocity": 0.25,
                "discussion": 0.20,
                "freshness": 0.15,
                "author_quality": 0.10,
            },
            "trending_bonus": 0.16,
            "author_replied_bonus": 0.12,
            "one_sided_author_penalty": 0.14,
            "topic_continuation_min_heat": 0.58,
            "operator_fit_min_score": 0.42,
            "dev_relevance_min_score": 0.24,
            "topic_continuation_requires_new_signal": True,
        },
        "neynar_readonly": {
            "enabled": False,
            "adapter": "neynar_v2_readonly",
            "base_url": "https://api.neynar.com",
            "api_key_env": "NEYNAR_API_KEY",
            "load_api_key": False,
            "write_api_key": False,
            "network_policy": "plan_only_until_authorized",
            "default_limit": 25,
            "controlled_fetcher": {
                "mode": "plan_or_fixture_only",
                "live_fetch_enabled": False,
                "allowed_methods": ["GET"],
                "allowed_path_prefix": "/v2/farcaster/",
                "max_requests_per_run": 12,
                "max_limit": 25,
            },
            "channels": ["openclaw", "farcaster", "ai", "base"],
            "endpoints": {
                "user_casts": "/v2/farcaster/feed/user/casts/",
                "channel_feed": "/v2/farcaster/feed/",
                "global_trending": "/v2/farcaster/feed/",
                "cast_search": "/v2/farcaster/cast/search/",
                "channel_notifications": "/v2/farcaster/notifications/channel/",
            },
            "docs": [
                "https://docs.neynar.com/reference/fetch-feed",
                "https://docs.neynar.com/reference/fetch-casts-for-user",
                "https://docs.neynar.com/reference/fetch-channel-notifications-for-user",
                "https://docs.neynar.com/reference/search-casts",
            ],
        },
        "attention": {
            "enabled": True,
            "max_active_topics": 5,
            "watch_duration_hours": 24,
            "major_watch_duration_hours": 48,
            "major_heat_threshold": 0.82,
            "followup_limit_per_topic": 2,
            "followup_cooldown_hours": 6,
            "min_heat_delta_for_followup": 0.22,
            "min_new_replies_for_followup": 2,
            "replace_lowest_slot_when_heat_delta": 0.28,
            "open_after_actions": ["cast", "quote", "reply"],
            "open_after_kinds": ["topic_cast", "thread_join", "direct_reply", "memory_cast"],
            "close_when_expired": True,
            "llm_call_policy": "only_material_change_creates_followup_event",
        },
        "operator_modes": {
            name: {
                "enabled": True,
                "default_action": mode["default_action"],
                "signals": mode["signals"],
                "layer_path": mode["layer_path"],
            }
            for name, mode in OPERATOR_MODES.items()
        },
        "state_machine": {
            "enabled": True,
            "terminal_without_live_publish": "dry_run_complete",
            "live_submit_state": "awaiting_external_publisher",
        },
        "webhook_reply": {
            "enabled": False,
            "adapter": "misa_farcaster_webhook",
            "normal_events_enter_same_operator_path": True,
            "run_operator_by_default": False,
            "signature_required_before_live": True,
        },
        "scheduler": {
            "enabled": False,
            "external_only": True,
            "creates_cron": False,
            "tick_interval_minutes": 30,
            "run_order": [
                "scheduler_tick",
                "neynar_readonly_fetcher",
                "webhook_ingest",
                "signal_digest",
                "ai_second_pass_provider_adapter",
                "run_cycle",
                "send_audit",
            ],
            "live_effects_allowed": False,
        },
        "automation_dry_run": {
            "enabled": True,
            "force_publisher_disabled": True,
            "force_keyless_neynar": True,
            "force_no_network": True,
            "force_no_vps": True,
        },
        "limits": {
            "daily_cast_or_quote": 6,
            "daily_quote": 1,
            "daily_reply": 80,
            "thread_round_max": 8,
            "max_draft_bytes": 1024,
            "user_cooldown_hours": 6,
        },
        "send_audit": {
            "required": True,
            "live_authorization_required": True,
            "packet_must_be_validated": True,
            "daily_limits_enforced": True,
            "rollback_required": True,
            "audit_before_external_publisher": True,
            "operator_may_submit_live": False,
        },
        "rollback": {
            "required": True,
            "strategy": "disable_publisher_and_hold_external_queue",
            "state_paths": ["send-audit-log.jsonl", "publish-results"],
        },
        "publisher": {
            "enabled": False,
            "transport": "x402",
            "adapter": "external_x402_publisher",
            "call_location": "publisher.submit(packet) after operator precheck",
            "allowed_actions": ["cast", "reply", "quote"],
            "submit_boundary": "publisher_only",
        },
        "memory": {
            "operator_memory_enabled": True,
            "main_memory_promotion": "candidate_only",
            "raw_full_memory_allowed": False,
            "public_safe_context_only": True,
        },
        "cybernetic_precheck": {
            "enabled": False,
            "mode": "read_only_reference",
            "wrapper": "tools/misa_cybernetic_wrapper.py --mode decision-precheck",
            "live_effects_allowed": False,
        },
        "personality": {
            "rule": "useful_first_then_personality",
            "voice": [
                "answer the practical point before showing attitude",
                "be specific, not vague",
                "sound like Misa, not a policy document",
                "if the thread is playful, leave one sharp line after the useful answer",
            ],
        },
        "public_persona_contract": default_public_persona_contract(),
    }


def default_public_persona_contract() -> dict[str, Any]:
    return {
        "version": "misa-farcaster-public-persona.v1.2",
        "identity": {
            "name": "Misa",
            "role": "public Farcaster operator for the Misa/Hermes project",
            "stance": "on the builder's side, but not a cheerleader or mascot",
        },
        "service_contract": {
            "primary_rule": "helpful_first_funny_second_never_boring",
            "must_do": [
                "answer the practical point before showing personality",
                "prefer evidence, receipts, and next actions",
                "say no or stay quiet when the public social path is the wrong surface",
            ],
            "must_not_do": [
                "perform public flair before useful work",
                "sound like a generic policy document",
                "claim private context, live visibility, or authority she does not have",
            ],
        },
        "public_voice": {
            "shape": ["short conclusion", "specific useful reason", "optional sharp tail"],
            "tone": ["direct", "warm enough", "builder-useful", "not sycophantic"],
            "tail_rule": "personality is allowed only after the practical receipt is on the table",
        },
        "context_boundary": {
            "public_safe_context_only": True,
            "owner_private_memory_allowed": False,
            "discord_private_context_allowed": False,
            "raw_runtime_details_allowed": False,
            "private_expression_visible": False,
        },
    }


def public_persona_contract(config: dict[str, Any]) -> dict[str, Any]:
    configured = config.get("public_persona_contract")
    if isinstance(configured, dict):
        return merge_dict(default_public_persona_contract(), configured)
    return default_public_persona_contract()


def persona_contract_hash(config: dict[str, Any]) -> str:
    return stable_hash(public_persona_contract(config))


def normalize_config(config: dict[str, Any] | None) -> dict[str, Any]:
    merged = merge_dict(default_config(), config or {})
    merged["public_persona_contract"] = public_persona_contract(merged)
    merged.setdefault("operator_version", OPERATOR_VERSION)
    return merged


def normalize_rule_registry(rule_registry: dict[str, Any] | None) -> dict[str, Any]:
    return merge_dict(default_rule_registry(), rule_registry or {})


def default_operator_state() -> dict[str, Any]:
    return {
        "schema": "misa.hermes.farcaster.operator_state.v1",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "seen_operation_ids": [],
        "daily": {},
        "last_operation": None,
    }


def default_topic_memory() -> dict[str, Any]:
    return {
        "schema": SCHEMA_TOPIC_MEMORY,
        "updated_at": utc_now(),
        "topics": {},
        "proven_angles": [],
        "failed_angles": [],
    }


def default_topic_attention() -> dict[str, Any]:
    return {
        "schema": SCHEMA_TOPIC_ATTENTION,
        "updated_at": utc_now(),
        "active_topics": {},
        "closed_topics": [],
        "policy": {
            "max_active_topics": default_config()["attention"]["max_active_topics"],
            "raw_json_to_misa": False,
            "llm_trigger": "material_change_only",
        },
    }


def default_relationship_memory() -> dict[str, Any]:
    return {
        "schema": SCHEMA_RELATIONSHIP_MEMORY,
        "updated_at": utc_now(),
        "users": {},
    }


def default_rule_registry() -> dict[str, Any]:
    return {
        "schema": SCHEMA_RULE_REGISTRY,
        "updated_at": utc_now(),
        "purpose": "Farcaster-specific rules used before emitting publisher packets",
        "actions": {
            "cast": {
                "requires_parent_hash": False,
                "requires_channel_id": False,
                "max_bytes": 1024,
                "publisher_transport": "x402",
            },
            "reply": {
                "requires_parent_hash": True,
                "requires_channel_id": False,
                "max_bytes": 1024,
                "publisher_transport": "x402",
            },
            "quote": {
                "requires_parent_hash": True,
                "requires_channel_id": False,
                "max_bytes": 1024,
                "publisher_transport": "x402",
            },
        },
        "blocked_actions": [
            "delete",
            "profile_update",
            "follow",
            "recast",
            "like",
            "poidh",
            "withdraw",
            "settle",
            "fname",
        ],
        "public_safety": {
            "raw_full_memory_allowed": False,
            "old_openclaw_runtime_allowed": False,
            "secret_terms_allowed": False,
            "wallet_addresses_allowed": False,
            "action_tags_allowed": False,
            "private_expression_markers_allowed": False,
        },
        "public_persona": {
            "requires_useful_first": True,
            "requires_persona_hash": True,
            "private_expression_visible": False,
        },
        "publisher_boundary": {
            "operator_may_submit_live": False,
            "packet_only": True,
            "external_submitter": "x402 publisher",
        },
        "cybernetic_precheck": {
            "allowed_mode": "read_only",
            "posts_publicly": False,
            "writes_persistent_memory": False,
            "starts_timer": False,
        },
    }


def default_style_memory_text() -> str:
    return (
        "# Misa Farcaster Style Memory\n\n"
        "- Useful first, personality second.\n"
        "- Give a concrete answer before the wink.\n"
        "- Prefer proof, receipts, and next actions over vague hype.\n"
        "- Keep private memory out of public casts.\n"
        "- Public voice shape: short conclusion, useful reason, optional sharp tail.\n"
        "- Private expression checks may guide the draft, but must never appear in the cast.\n"
    )


def init_state(args: argparse.Namespace | None = None, *, state_root: Path | None = None) -> dict[str, Any]:
    root = Path(state_root or getattr(args, "state_root", DEFAULT_STATE_ROOT))
    root.mkdir(parents=True, exist_ok=True)
    for dirname in STATE_DIRS:
        (root / dirname).mkdir(parents=True, exist_ok=True)

    config_path = state_path(root, "config")
    if not config_path.exists() or getattr(args, "overwrite_config", False):
        write_json(config_path, normalize_config({}))
    else:
        existing_config = read_json(config_path, {})
        normalized_config = normalize_config(existing_config)
        if normalized_config != existing_config:
            write_json(config_path, normalized_config)

    operator_state_path = state_path(root, "operator_state")
    if not operator_state_path.exists():
        write_json(operator_state_path, default_operator_state())

    topic_path = state_path(root, "topic_memory")
    if not topic_path.exists():
        write_json(topic_path, default_topic_memory())

    attention_path = state_path(root, "topic_attention")
    if not attention_path.exists():
        write_json(attention_path, default_topic_attention())

    relationship_path = state_path(root, "relationship_memory")
    if not relationship_path.exists():
        write_json(relationship_path, default_relationship_memory())

    rule_path = state_path(root, "rule_registry")
    if not rule_path.exists():
        write_json(rule_path, normalize_rule_registry({}))
    else:
        existing_rules = read_json(rule_path, {})
        normalized_rules = normalize_rule_registry(existing_rules)
        if normalized_rules != existing_rules:
            write_json(rule_path, normalized_rules)

    style_path = state_path(root, "style_memory")
    if not style_path.exists():
        style_path.write_text(default_style_memory_text(), encoding="utf-8")

    for key, filename in STATE_FILES.items():
        if key in {
            "config",
            "operator_state",
            "topic_memory",
            "topic_attention",
            "relationship_memory",
            "rule_registry",
            "style_memory",
        }:
            continue
        path = root / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("", encoding="utf-8")

    return {
        "ok": True,
        "schema": "misa.hermes.farcaster.init_state.result.v1",
        "state_root": str(root),
        "state_files": {key: str(root / filename) for key, filename in STATE_FILES.items()},
        "state_dirs": [str(root / dirname) for dirname in STATE_DIRS],
        "operator_version": OPERATOR_VERSION,
        "public_persona": {
            "version": normalize_config(read_json(config_path, {}))["public_persona_contract"]["version"],
            "hash": persona_contract_hash(read_json(config_path, {})),
        },
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "secrets": "not_loaded_or_written",
            "state": "initialized",
        },
    }


def schema_summary(args: argparse.Namespace | None = None) -> dict[str, Any]:
    root = Path(getattr(args, "state_root", DEFAULT_STATE_ROOT))
    return {
        "operator_version": OPERATOR_VERSION,
        "schemas": {
            "config": SCHEMA_CONFIG,
            "event": SCHEMA_EVENT,
            "decision": SCHEMA_DECISION,
            "draft": SCHEMA_DRAFT,
            "publisher_packet": SCHEMA_PACKET,
            "result": SCHEMA_RESULT,
            "rule_registry": SCHEMA_RULE_REGISTRY,
            "state_transition": SCHEMA_STATE_TRANSITION,
            "daily_report": SCHEMA_DAILY_REPORT,
            "mcp_manifest": SCHEMA_MCP_MANIFEST,
            "expression_precheck": SCHEMA_EXPRESSION_PRECHECK,
            "signal_digest": SCHEMA_SIGNAL_DIGEST,
            "ai_second_pass_packet": SCHEMA_AI_SECOND_PASS_PACKET,
            "ai_second_pass_result": SCHEMA_AI_SECOND_PASS_RESULT,
            "topic_attention": SCHEMA_TOPIC_ATTENTION,
            "neynar_fetch_plan": SCHEMA_NEYNAR_FETCH_PLAN,
            "neynar_fetcher_run": SCHEMA_NEYNAR_FETCHER_RUN,
            "neynar_ingest": SCHEMA_NEYNAR_INGEST,
            "webhook_ingest": SCHEMA_WEBHOOK_INGEST,
            "scheduler_tick": SCHEMA_SCHEDULER_TICK,
            "send_audit": SCHEMA_SEND_AUDIT,
            "operator_quality": SCHEMA_OPERATOR_QUALITY,
            "topic_heat": SCHEMA_TOPIC_HEAT,
            "ai_second_pass_provider_adapter": SCHEMA_AI_SECOND_PASS_PROVIDER_ADAPTER,
            "dry_run_automation_cycle": SCHEMA_DRY_RUN_AUTOMATION_CYCLE,
        },
        "state_root": str(root),
        "state_files": {key: str(root / filename) for key, filename in STATE_FILES.items()},
        "operator_layers": OPERATOR_LAYERS,
        "operator_modes": OPERATOR_MODES,
        "state_machine": STATE_MACHINE,
        "mcp_tools": [tool["name"] for tool in mcp_tool_manifest()["tools"]],
        "publisher_boundary": {
            "transport": "x402",
            "called_by_this_tool": False,
            "call_location": "external publisher after packet validation",
        },
        "side_effects": {
            "farcaster": "not_submitted",
            "network": "not_used",
            "secrets": "not_loaded",
        },
    }


def looks_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def text_bytes(text: str) -> int:
    return len(text.encode("utf-8"))


def clean_text(text: Any, limit: int = 4000) -> str:
    if text is None:
        return ""
    value = str(text).replace("\x00", "").strip()
    value = re.sub(r"\s+", " ", value)
    return value[:limit]


def cast_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cast = dict(payload.get("cast") or {})
    author = payload.get("author") or cast.get("author") or {}
    metrics = payload.get("metrics") or {}
    parent_hash = cast.get("parent_hash") or payload.get("parent_hash") or payload.get("reply_to_hash")

    return {
        "hash": cast.get("hash") or payload.get("cast_hash") or payload.get("hash") or "",
        "parent_hash": parent_hash or "",
        "root_hash": cast.get("root_hash") or payload.get("root_hash") or parent_hash or "",
        "fid": cast.get("fid") or author.get("fid") or payload.get("fid"),
        "author_username": (
            cast.get("author_username")
            or author.get("username")
            or author.get("display_name")
            or payload.get("author_username")
            or "unknown"
        ),
        "text": clean_text(cast.get("text") or payload.get("text") or payload.get("body")),
        "timestamp": cast.get("timestamp") or payload.get("timestamp") or utc_now(),
        "channel_id": cast.get("channel_id") or payload.get("channel_id") or payload.get("channel") or "",
        "mentions_misa": bool(cast.get("mentions_misa") or payload.get("mentions_misa") or payload.get("type") == "mention"),
        "replies_to_misa": bool(cast.get("replies_to_misa") or payload.get("replies_to_misa") or payload.get("type") == "reply"),
        "embeds": cast.get("embeds") or payload.get("embeds") or [],
        "author_score": float(cast.get("author_score") or author.get("score") or metrics.get("author_score") or 0.0),
        "reply_count": int(cast.get("reply_count") or metrics.get("reply_count") or metrics.get("replies") or 0),
        "like_count": int(cast.get("like_count") or metrics.get("like_count") or metrics.get("likes") or 0),
        "recast_count": int(cast.get("recast_count") or metrics.get("recast_count") or metrics.get("recasts") or 0),
    }


def normalize_event(payload: dict[str, Any]) -> dict[str, Any]:
    cast = cast_from_payload(payload)
    event_type = payload.get("event_type") or payload.get("type") or "cast_created"
    if cast["mentions_misa"]:
        event_type = "mention"
    elif cast["replies_to_misa"]:
        event_type = "reply"

    base_metrics = dict(payload.get("metrics") or {})
    normalized_metrics = normalized_metrics_from_payload({**payload, "cast": cast, "metrics": base_metrics})
    event_metrics = {**base_metrics, **{key: value for key, value in normalized_metrics.items() if key not in base_metrics}}
    event_id = payload.get("event_id") or stable_id("evt", event_type, cast.get("hash"), cast.get("text"))
    return {
        "schema": SCHEMA_EVENT,
        "event_id": event_id,
        "source": payload.get("source") or "local_fixture",
        "received_at": payload.get("received_at") or utc_now(),
        "event_type": event_type,
        "cast": cast,
        "thread_summary": clean_text(payload.get("thread_summary")),
        "public_memory": payload.get("public_memory") or [],
        "topic_tags": payload.get("topic_tags") or [],
        "metrics": event_metrics,
        "conversation_judge": payload.get("conversation_judge") or {},
        "raw_ref": payload.get("raw_ref") or "",
        "redaction_applied": bool(payload.get("redaction_applied", True)),
        "raw_memory": payload.get("raw_memory"),
        "full_memory": payload.get("full_memory"),
        "private_context": payload.get("private_context"),
    }


def infer_event_kind(event: dict[str, Any]) -> str:
    event_type = str(event.get("event_type", "")).lower()
    cast = event.get("cast", {})
    if cast.get("mentions_misa") or cast.get("replies_to_misa") or event_type in {"mention", "reply"}:
        return "direct_reply"
    if event_type in {"conversation_update", "thread_candidate", "thread_signal"} or cast.get("parent_hash"):
        return "thread_join"
    if event_type in {"trending_candidate", "channel_candidate", "hot_topic", "topic_signal"}:
        return "topic_cast"
    if event_type in {"memory_prompt", "content_prompt", "scheduled_prompt"}:
        return "memory_cast"
    return "ambient"


def topic_matches(text: str, tags: list[str]) -> dict[str, int]:
    lowered = text.lower()
    scores: dict[str, int] = {}
    for topic, words in TOPIC_KEYWORDS.items():
        score = 0
        for word in words:
            if word.lower() in lowered:
                score += 1
        for tag in tags:
            if tag.lower() == topic:
                score += 2
        if score:
            scores[topic] = score
    return scores


def is_question(text: str) -> bool:
    lower = text.lower()
    return "?" in text or "？" in text or any(word in lower for word in ["how", "why", "what", "should", "怎么", "如何", "为什么", "要不要"])


def has_substance(text: str) -> bool:
    stripped = text.strip()
    if text_bytes(stripped) < 12:
        return False
    if stripped.lower() in {"gm", "gn", "hi", "hello", "hey", "lol"}:
        return False
    return True


def author_key(event: dict[str, Any]) -> str:
    cast = event.get("cast", {})
    fid = cast.get("fid")
    username = cast.get("author_username") or "unknown"
    return f"fid:{fid}" if fid else f"username:{username}"


def thread_key(event: dict[str, Any]) -> str:
    cast = event.get("cast", {})
    return cast.get("root_hash") or cast.get("parent_hash") or cast.get("hash") or event.get("event_id")


def load_runtime_state(state_root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    config = normalize_config(read_json(state_path(state_root, "config"), {}))
    operator_state = read_json(state_path(state_root, "operator_state"), default_operator_state())
    topic_memory = read_json(state_path(state_root, "topic_memory"), default_topic_memory())
    relationship_memory = read_json(state_path(state_root, "relationship_memory"), default_relationship_memory())
    return config, operator_state, topic_memory, relationship_memory


def daily_counts(operator_state: dict[str, Any]) -> dict[str, int]:
    daily = operator_state.setdefault("daily", {})
    today = daily.setdefault(today_key(), {"cast_or_quote": 0, "quote": 0, "reply": 0})
    today.setdefault("cast_or_quote", 0)
    today.setdefault("quote", 0)
    today.setdefault("reply", 0)
    return today


def build_context(
    event: dict[str, Any],
    config: dict[str, Any],
    topic_memory: dict[str, Any],
    relationship_memory: dict[str, Any],
) -> dict[str, Any]:
    cast = event.get("cast", {})
    text = cast.get("text", "")
    tags = [str(tag) for tag in event.get("topic_tags", [])]
    matched_topics = topic_matches(text + " " + " ".join(tags), tags)
    relationship = relationship_memory.get("users", {}).get(author_key(event), {})
    known_topic_hits = {
        topic: topic_memory.get("topics", {}).get(topic, {})
        for topic in matched_topics
        if topic in topic_memory.get("topics", {})
    }
    continuation = conversation_continuation_profile({"cast": cast, "metrics": event.get("metrics", {}), "conversation_judge": event.get("conversation_judge", {})})
    return {
        "kind": infer_event_kind(event),
        "language": "zh" if looks_chinese(text) else "en",
        "is_question": is_question(text),
        "has_substance": has_substance(text),
        "matched_topics": matched_topics,
        "known_topic_hits": known_topic_hits,
        "author_key": author_key(event),
        "thread_key": thread_key(event),
        "relationship": relationship,
        "public_memory": event.get("public_memory", []),
        "limits": config.get("limits", {}),
        "public_persona": public_persona_contract(config),
        "persona_hash": persona_contract_hash(config),
        "conversation": continuation,
    }


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_after_hours(hours: float, *, start: str | None = None) -> str:
    base = parse_dt(start) or datetime.now(timezone.utc)
    return (base + timedelta(hours=hours)).isoformat()


def is_expired(expires_at: Any, *, now: datetime | None = None) -> bool:
    expires = parse_dt(expires_at)
    if not expires:
        return False
    return expires <= (now or datetime.now(timezone.utc))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp_float(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def saturation(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return clamp_float(1.0 - math.exp(-max(0.0, value) / scale))


def freshness_score(age_hours: float | None) -> float:
    if age_hours is None:
        return 0.45
    if age_hours <= 1:
        return 1.0
    if age_hours <= 3:
        return 0.92
    if age_hours <= 6:
        return 0.78
    if age_hours <= 12:
        return 0.58
    if age_hours <= 24:
        return 0.38
    if age_hours <= 48:
        return 0.18
    return 0.06


def source_is_trending(payload: dict[str, Any]) -> bool:
    source = str(payload.get("source") or payload.get("source_hint") or "").lower()
    event_type = str(payload.get("event_type") or payload.get("type") or "").lower()
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    return bool(
        metrics.get("in_trending_feed")
        or payload.get("in_trending_feed")
        or "trending" in source
        or event_type in {"hot_topic", "trending_candidate"}
    )


def author_score_from_payload(payload: dict[str, Any], cast: dict[str, Any], metrics: dict[str, Any]) -> float:
    author = payload.get("author") if isinstance(payload.get("author"), dict) else {}
    nested_author = cast.get("author") if isinstance(cast.get("author"), dict) else {}
    for source in (metrics, cast, author, nested_author, nested_author.get("experimental") if isinstance(nested_author.get("experimental"), dict) else {}):
        if not isinstance(source, dict):
            continue
        for key in ("author_score", "score", "neynar_user_score", "neynar_score"):
            if key in source:
                return clamp_float(safe_float(source.get(key)))
    return 0.0


def direct_replies_from_payload(payload: dict[str, Any], cast: dict[str, Any], metrics: dict[str, Any]) -> tuple[int, int]:
    direct_replies = cast.get("direct_replies") if isinstance(cast.get("direct_replies"), list) else []
    direct_count = safe_int(
        metrics.get("direct_replies")
        or metrics.get("direct_replies_count")
        or payload.get("direct_replies_count")
        or len(direct_replies)
    )
    author_ids: set[str] = set()
    for reply in direct_replies:
        if not isinstance(reply, dict):
            continue
        author = reply.get("author") if isinstance(reply.get("author"), dict) else {}
        fid = author.get("fid") or reply.get("fid")
        if fid is not None:
            author_ids.add(str(fid))
    unique_count = safe_int(
        metrics.get("unique_reply_authors")
        or metrics.get("unique_direct_reply_authors")
        or payload.get("unique_reply_authors")
        or len(author_ids)
    )
    return max(0, direct_count), max(0, unique_count)


def conversation_continuation_profile(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    judge = payload.get("conversation_judge") if isinstance(payload.get("conversation_judge"), dict) else {}
    author_replied = bool(
        metrics.get("author_replied_to_misa")
        or metrics.get("reply_to_misa")
        or judge.get("author_replied_to_misa")
        or judge.get("direct_reply_from_author")
        or payload.get("author_replied_to_misa")
    )
    one_sided = bool(
        metrics.get("misa_last_reply_ignored")
        or metrics.get("one_sided_author_pressure")
        or judge.get("misa_last_reply_ignored")
        or judge.get("one_sided_author_pressure")
        or payload.get("misa_last_reply_ignored")
    )
    return {
        "author_replied_to_misa": author_replied,
        "one_sided_author_pressure": bool(one_sided and not author_replied),
    }


def community_heat_profile(payload: dict[str, Any]) -> dict[str, Any]:
    cast = payload.get("cast") if isinstance(payload.get("cast"), dict) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    raw_reactions = cast.get("reactions") if isinstance(cast.get("reactions"), dict) else payload.get("reactions")
    raw_replies = cast.get("replies") if isinstance(cast.get("replies"), dict) else payload.get("replies")
    reactions = raw_reactions if isinstance(raw_reactions, dict) else {}
    replies_obj = raw_replies if isinstance(raw_replies, dict) else {}
    likes = safe_int(metrics.get("likes") or metrics.get("like_count") or reactions.get("likes_count") or cast.get("like_count"))
    replies = safe_int(metrics.get("replies") or metrics.get("reply_count") or replies_obj.get("count") or cast.get("reply_count"))
    recasts = safe_int(metrics.get("recasts") or metrics.get("recast_count") or reactions.get("recasts_count") or cast.get("recast_count"))
    direct_replies, unique_reply_authors = direct_replies_from_payload(payload, cast, metrics)
    author_score = author_score_from_payload(payload, cast, metrics)
    age_hours = event_age_hours(payload)
    engagement_units = likes + replies * 3 + recasts * 4
    engagement_score = saturation(float(engagement_units), 55.0)
    velocity_per_hour = float(engagement_units) / max(0.5, age_hours if age_hours is not None else 12.0)
    velocity_score = saturation(velocity_per_hour, 18.0)
    discussion_units = replies + direct_replies + unique_reply_authors * 0.8
    discussion_score = saturation(float(discussion_units), 10.0)
    fresh_score = freshness_score(age_hours)
    in_trending = source_is_trending(payload)
    continuation = conversation_continuation_profile(payload)
    explicit_heat = metrics.get("heat") if metrics.get("heat") is not None else payload.get("heat")

    raw_heat = (
        engagement_score * 0.30
        + velocity_score * 0.25
        + discussion_score * 0.20
        + fresh_score * 0.15
        + author_score * 0.10
        + (0.16 if in_trending else 0.0)
        + (0.12 if continuation["author_replied_to_misa"] else 0.0)
        - (0.14 if continuation["one_sided_author_pressure"] else 0.0)
    )
    computed_heat = round(clamp_float(raw_heat), 3)
    if explicit_heat is not None:
        heat = round(clamp_float(safe_float(explicit_heat)), 3)
        heat_source = "explicit_payload"
    else:
        heat = computed_heat
        heat_source = "computed"
    return {
        "schema": SCHEMA_TOPIC_HEAT,
        "formula_version": "misa-topic-heat-v1",
        "heat": heat,
        "computed_heat": computed_heat,
        "heat_source": heat_source,
        "likes": likes,
        "replies": replies,
        "recasts": recasts,
        "direct_replies": direct_replies,
        "unique_reply_authors": unique_reply_authors,
        "engagement": likes + replies + recasts,
        "engagement_units": engagement_units,
        "velocity_per_hour": round(velocity_per_hour, 3),
        "age_hours": round(age_hours, 3) if age_hours is not None else None,
        "freshness_score": round(fresh_score, 3),
        "engagement_score": round(engagement_score, 3),
        "velocity_score": round(velocity_score, 3),
        "discussion_score": round(discussion_score, 3),
        "author_score": round(author_score, 3),
        "in_trending_feed": in_trending,
        "author_replied_to_misa": continuation["author_replied_to_misa"],
        "one_sided_author_pressure": continuation["one_sided_author_pressure"],
    }


def keyword_hit_count(text: str, keywords: list[str]) -> int:
    lowered = text.lower()
    return sum(1 for keyword in keywords if keyword.lower() in lowered)


def pattern_hits(text: str, patterns: list[re.Pattern[str]]) -> list[str]:
    hits: list[str] = []
    for pattern in patterns:
        if pattern.search(text):
            hits.append(pattern.pattern)
    return hits


def author_identity_text(payload: dict[str, Any], cast: dict[str, Any]) -> str:
    author = cast.get("author") if isinstance(cast.get("author"), dict) else {}
    if not author and isinstance(payload.get("author"), dict):
        author = payload["author"]
    profile = author.get("profile") if isinstance(author.get("profile"), dict) else {}
    bio = profile.get("bio") if isinstance(profile.get("bio"), dict) else {}
    parts = [
        author.get("username"),
        author.get("display_name"),
        author.get("bio"),
        author.get("description"),
        profile.get("bio") if isinstance(profile.get("bio"), str) else None,
        bio.get("text"),
    ]
    return " ".join(str(part) for part in parts if part)


def developer_signal_score(text: str, joined: str) -> dict[str, Any]:
    strong_hits = pattern_hits(joined, STRONG_DEV_MECHANICS_PATTERNS)
    tech_intent_hits = pattern_hits(joined, TECH_INTENT_PATTERNS)
    topic_hits = keyword_hit_count(joined, DEV_RELEVANCE_KEYWORDS)
    score = 0
    if topic_hits:
        score += 2
    score += min(topic_hits, 4)
    if strong_hits:
        score += 4
    if tech_intent_hits:
        score += 2
    if is_question(text):
        score += 1
    if CODE_OR_API_SHAPE_PATTERN.search(joined):
        score += 2
    if pattern_hits(text, LOW_SUBSTANCE_SOCIAL_PATTERNS):
        score -= 5
    if not topic_hits and text_bytes(text) < 120:
        score -= 2
    return {
        "score": score,
        "normalized": round(clamp_float(score / 11.0), 3) if score > 0 else 0.0,
        "topic_hits": topic_hits,
        "strong_hits": len(strong_hits),
        "intent_hits": len(tech_intent_hits),
    }


def operator_fit_profile(payload: dict[str, Any], heat_profile: dict[str, Any]) -> dict[str, Any]:
    cast = payload.get("cast") if isinstance(payload.get("cast"), dict) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    text = clean_text(cast.get("text") or payload.get("text") or payload.get("body"), limit=1200)
    tags = [str(tag) for tag in payload.get("topic_tags", [])]
    channel_id = str(cast.get("channel_id") or payload.get("channel_id") or payload.get("channel") or "")
    author_context = author_identity_text(payload, cast)
    joined = " ".join([text, " ".join(tags), channel_id, author_context])
    dev_hits = keyword_hit_count(joined, DEV_RELEVANCE_KEYWORDS)
    constructive_hits = keyword_hit_count(joined, CONSTRUCTIVE_KEYWORDS)
    promo_hits = pattern_hits(text, PROMO_OR_TOKEN_PATTERNS)
    social_hits = pattern_hits(text, LOW_SUBSTANCE_SOCIAL_PATTERNS)
    generic_hype_hits = pattern_hits(text, GENERIC_BUILDER_HYPE_PATTERNS)
    generic_ai_hits = pattern_hits(text, GENERIC_AI_PROMO_PATTERNS)
    quality_event_hits = pattern_hits(text, QUALITY_EVENT_PATTERNS)
    developer_profile = developer_signal_score(text, joined)
    author_dev_score = clamp_float(keyword_hit_count(author_context, AUTHOR_DEV_KEYWORDS) / 3.0) if author_context else 0.0
    dev_relevance = clamp_float(max(dev_hits / 5.0, developer_profile["normalized"]) + author_dev_score * 0.08)
    constructive_score = clamp_float((constructive_hits + (1 if is_question(text) else 0)) / 4.0)
    author_score = clamp_float(float(heat_profile.get("author_score") or 0.0))
    kol_dev_score = author_score * max(dev_relevance, constructive_score, author_dev_score * 0.75)
    community_heat = float(heat_profile.get("heat") or heat_profile.get("computed_heat") or 0.0)
    continuation = conversation_continuation_profile(payload)
    has_specific_dev_mechanics = developer_profile["strong_hits"] > 0
    has_credible_dev_identity = author_score >= 0.88 and author_dev_score >= 0.2
    has_quality_hot_event = author_score >= 0.92 and community_heat >= 0.62 and bool(quality_event_hits)
    paid_interaction_ready = bool(
        has_specific_dev_mechanics
        or has_credible_dev_identity
        or has_quality_hot_event
        or continuation["author_replied_to_misa"]
    )

    low_value_penalty = 0.0
    low_value_reasons: list[str] = []
    if promo_hits:
        low_value_penalty += 0.55
        low_value_reasons.append("promo_or_token_topic")
    if social_hits:
        low_value_penalty += 0.36
        low_value_reasons.append("low_substance_social_post")
    if generic_ai_hits:
        low_value_penalty += 0.55
        low_value_reasons.append("generic_ai_or_app_generated_content")
    if generic_hype_hits:
        low_value_penalty += 0.32
        low_value_reasons.append("generic_builder_hype")
        if developer_profile["strong_hits"] == 0 and author_dev_score < 0.2:
            low_value_penalty += 0.20
            low_value_reasons.append("no_specific_dev_mechanics_or_dev_identity")
    if not paid_interaction_ready:
        low_value_penalty += 0.42
        low_value_reasons.append("no_paid_interaction_proof")
    if text_bytes(text) < 40 and dev_relevance < 0.2:
        low_value_penalty += 0.20
        low_value_reasons.append("short_low_context_post")
    if dev_relevance < 0.15 and constructive_score < 0.15 and not continuation["author_replied_to_misa"]:
        low_value_penalty += 0.22
        low_value_reasons.append("not_developer_relevant")
    low_value_penalty = clamp_float(low_value_penalty)

    raw_fit = (
        dev_relevance * 0.38
        + constructive_score * 0.18
        + developer_profile["normalized"] * 0.12
        + kol_dev_score * 0.12
        + community_heat * max(0.05, dev_relevance) * 0.14
        + (0.10 if heat_profile.get("in_trending_feed") and dev_relevance >= 0.35 else 0.0)
        + (0.12 if continuation["author_replied_to_misa"] else 0.0)
        - (0.12 if continuation["one_sided_author_pressure"] else 0.0)
        - low_value_penalty
    )
    operator_fit = clamp_float(raw_fit)
    if low_value_penalty >= 0.50 and dev_relevance < 0.35:
        operator_fit = min(operator_fit, 0.18)
    elif low_value_penalty >= 0.35 and dev_relevance < 0.25:
        operator_fit = min(operator_fit, 0.26)

    return {
        "operator_fit": round(operator_fit, 3),
        "dev_relevance": round(dev_relevance, 3),
        "constructive_score": round(constructive_score, 3),
        "kol_dev_score": round(kol_dev_score, 3),
        "community_heat_used": round(community_heat, 3),
        "low_value_penalty": round(low_value_penalty, 3),
        "low_value_reasons": low_value_reasons,
        "developer_keyword_hits": dev_hits,
        "constructive_keyword_hits": constructive_hits,
        "developer_signal_score": developer_profile["score"],
        "developer_signal_normalized": developer_profile["normalized"],
        "strong_dev_mechanics_hits": developer_profile["strong_hits"],
        "tech_intent_hits": developer_profile["intent_hits"],
        "author_developer_score": round(author_dev_score, 3),
        "generic_builder_hype_hits": len(generic_hype_hits),
        "generic_ai_promo_hits": len(generic_ai_hits),
        "quality_event_hits": len(quality_event_hits),
        "paid_interaction_ready": paid_interaction_ready,
        "paid_interaction_ready_reasons": [
            reason
            for reason, ok in [
                ("specific_dev_mechanics", has_specific_dev_mechanics),
                ("credible_dev_identity", has_credible_dev_identity),
                ("quality_hot_event", has_quality_hot_event),
                ("author_replied_to_misa", continuation["author_replied_to_misa"]),
            ]
            if ok
        ],
        "author_replied_to_misa": continuation["author_replied_to_misa"],
        "one_sided_author_pressure": continuation["one_sided_author_pressure"],
    }


def normalized_metrics_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    heat_profile = community_heat_profile(payload)
    fit_profile = operator_fit_profile(payload, heat_profile)
    actionability_freshness = actionability_freshness_from_payload(payload)
    return {
        "heat": heat_profile["heat"],
        "operator_fit": fit_profile["operator_fit"],
        "dev_relevance": fit_profile["dev_relevance"],
        "actionability_freshness": actionability_freshness,
        "low_value_penalty": fit_profile["low_value_penalty"],
        "likes": heat_profile["likes"],
        "replies": heat_profile["replies"],
        "recasts": heat_profile["recasts"],
        "engagement": heat_profile["engagement"],
        "engagement_units": heat_profile["engagement_units"],
        "velocity_per_hour": heat_profile["velocity_per_hour"],
        "direct_replies": heat_profile["direct_replies"],
        "unique_reply_authors": heat_profile["unique_reply_authors"],
        "heat_profile": heat_profile,
        "operator_fit_profile": fit_profile,
    }


def recency_bonus_from_payload(payload: dict[str, Any]) -> float:
    cast = payload.get("cast") or {}
    timestamp = parse_dt(cast.get("timestamp") or payload.get("timestamp"))
    if not timestamp:
        return 0.02
    age_hours = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600.0)
    if age_hours <= 1:
        return 0.10
    if age_hours <= 3:
        return 0.07
    if age_hours <= 6:
        return 0.04
    if age_hours <= 12:
        return 0.02
    return 0.0


def actionability_freshness_from_payload(payload: dict[str, Any]) -> float:
    cast = payload.get("cast") if isinstance(payload.get("cast"), dict) else {}
    timestamp = parse_dt(cast.get("timestamp") or payload.get("timestamp") or payload.get("received_at"))
    if not timestamp:
        return 0.65
    age_hours = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600.0)
    if age_hours <= 6:
        return 1.0
    if age_hours <= 24:
        return 0.75
    if age_hours <= 72:
        return 0.35
    if age_hours <= 168:
        return 0.15
    return 0.04


def event_age_hours(event: dict[str, Any]) -> float | None:
    cast = event.get("cast") or {}
    timestamp = parse_dt(cast.get("timestamp") or event.get("timestamp") or event.get("received_at"))
    if not timestamp:
        return None
    return max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600.0)


def neynar_casts_from_payload(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    candidates: list[Any] = []
    for container in (payload, result, data):
        if isinstance(container.get("casts"), list):
            candidates.extend(container["casts"])
        if isinstance(container.get("cast"), dict):
            candidates.append(container["cast"])
        if isinstance(container.get("notifications"), list):
            for notification in container["notifications"]:
                if isinstance(notification, dict) and isinstance(notification.get("cast"), dict):
                    cast = dict(notification["cast"])
                    cast["_notification_type"] = notification.get("type") or container.get("type")
                    cast["_notification_seen"] = notification.get("seen")
                    candidates.append(cast)
    return [item for item in candidates if isinstance(item, dict)]


def neynar_author_score(author: dict[str, Any], raw_cast: dict[str, Any]) -> float:
    for source in (author, raw_cast):
        for key in ("score", "neynar_score", "author_score"):
            value = source.get(key)
            if value is None:
                continue
            try:
                return max(0.0, min(1.0, float(value)))
            except (TypeError, ValueError):
                continue
    if author.get("power_badge") or author.get("power_badge_enabled"):
        return 0.7
    return 0.0


def neynar_channel_id(raw_cast: dict[str, Any]) -> str:
    channel = raw_cast.get("channel")
    if isinstance(channel, dict):
        return str(channel.get("id") or channel.get("name") or "")
    if isinstance(channel, str):
        return channel
    return str(raw_cast.get("channel_id") or raw_cast.get("root_parent_url") or raw_cast.get("parent_url") or "")


def neynar_mentions_misa(raw_cast: dict[str, Any]) -> bool:
    text = str(raw_cast.get("text") or "")
    if f"@{MISA_USERNAME}".lower() in text.lower():
        return True
    mentions = raw_cast.get("mentioned_profiles") or raw_cast.get("mentions") or []
    for mention in mentions:
        if isinstance(mention, dict):
            if int(mention.get("fid") or 0) == MISA_FID:
                return True
            if str(mention.get("username") or "").lower() == MISA_USERNAME.lower():
                return True
        elif str(mention) == str(MISA_FID):
            return True
    return False


def neynar_replies_to_misa(raw_cast: dict[str, Any]) -> bool:
    parent_author = raw_cast.get("parent_author") or raw_cast.get("parent_author_fid") or {}
    if isinstance(parent_author, dict):
        return int(parent_author.get("fid") or 0) == MISA_FID
    try:
        return int(parent_author) == MISA_FID
    except (TypeError, ValueError):
        return False


def neynar_event_type(raw_cast: dict[str, Any], source_hint: str) -> str:
    notification_type = str(raw_cast.get("_notification_type") or "").lower()
    if neynar_mentions_misa(raw_cast) or "mention" in notification_type:
        return "mention"
    if neynar_replies_to_misa(raw_cast) or "reply" in notification_type:
        return "reply"
    if "trending" in source_hint:
        return "hot_topic"
    if raw_cast.get("parent_hash"):
        return "conversation_update"
    return "cast_created"


def neynar_payload_to_event_payload(raw_cast: dict[str, Any], *, source_hint: str = "neynar_readonly") -> dict[str, Any]:
    author = raw_cast.get("author") if isinstance(raw_cast.get("author"), dict) else {}
    reactions = raw_cast.get("reactions") if isinstance(raw_cast.get("reactions"), dict) else {}
    replies = raw_cast.get("replies") if isinstance(raw_cast.get("replies"), dict) else {}
    cast_hash = raw_cast.get("hash") or raw_cast.get("cast_hash") or ""
    channel_id = neynar_channel_id(raw_cast)
    metrics = {
        "likes": int(raw_cast.get("like_count") or reactions.get("likes_count") or 0),
        "replies": int(raw_cast.get("reply_count") or replies.get("count") or 0),
        "recasts": int(raw_cast.get("recast_count") or reactions.get("recasts_count") or 0),
        "author_score": neynar_author_score(author, raw_cast),
    }
    normalized = normalized_metrics_from_payload({"cast": raw_cast, "metrics": metrics, "source": source_hint})
    metrics["heat"] = normalized["heat"]
    metrics["engagement_units"] = normalized["engagement_units"]
    metrics["velocity_per_hour"] = normalized["velocity_per_hour"]
    metrics["operator_fit"] = normalized["operator_fit"]
    metrics["dev_relevance"] = normalized["dev_relevance"]
    metrics["low_value_penalty"] = normalized["low_value_penalty"]
    metrics["operator_fit_profile"] = normalized["operator_fit_profile"]
    metrics["heat_profile"] = normalized["heat_profile"]
    event_type = neynar_event_type(raw_cast, source_hint)
    topic_tags = [channel_id] if channel_id and channel_id in TOPIC_KEYWORDS else []
    return {
        "event_id": stable_id("evt_neynar", source_hint, cast_hash, raw_cast.get("timestamp"), raw_cast.get("text")),
        "source": source_hint,
        "type": event_type,
        "cast_hash": cast_hash,
        "parent_hash": raw_cast.get("parent_hash") or "",
        "root_hash": raw_cast.get("thread_hash") or raw_cast.get("root_parent_hash") or raw_cast.get("parent_hash") or cast_hash,
        "author": {
            "fid": author.get("fid") or raw_cast.get("fid"),
            "username": author.get("username") or author.get("display_name") or raw_cast.get("author_username"),
            "score": metrics["author_score"],
        },
        "channel_id": channel_id,
        "text": clean_text(raw_cast.get("text")),
        "timestamp": raw_cast.get("timestamp") or raw_cast.get("created_at") or utc_now(),
        "metrics": metrics,
        "topic_tags": topic_tags,
        "mentions_misa": neynar_mentions_misa(raw_cast),
        "replies_to_misa": neynar_replies_to_misa(raw_cast),
        "raw_ref": f"neynar:{cast_hash or stable_hash(raw_cast)}",
        "redaction_applied": True,
    }


def build_neynar_readonly_fetch_plan(
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    neynar = config.get("neynar_readonly", {})
    endpoints = neynar.get("endpoints", {})
    base_url = str(neynar.get("base_url") or "https://api.neynar.com").rstrip("/")
    limit = int(neynar.get("default_limit") or 25)
    requests: list[dict[str, Any]] = []

    requests.append({
        "purpose": "misa_recent_casts",
        "method": "GET",
        "url": base_url + str(endpoints.get("user_casts") or "/v2/farcaster/feed/user/casts/"),
        "params": {"fid": MISA_FID, "limit": limit},
        "headers": {"x-api-key": "[REDACTED:NEYNAR_API_KEY]"},
        "normalizer": "neynar_payload_to_event_payload",
    })
    for channel_id in neynar.get("channels", []):
        requests.append({
            "purpose": f"channel_feed:{channel_id}",
            "method": "GET",
            "url": base_url + str(endpoints.get("channel_feed") or "/v2/farcaster/feed/"),
            "params": {
                "feed_type": "filter",
                "filter_type": "channel_id",
                "channel_id": channel_id,
                "limit": limit,
                "with_recasts": False,
            },
            "headers": {"x-api-key": "[REDACTED:NEYNAR_API_KEY]"},
            "normalizer": "neynar_payload_to_event_payload",
        })
    requests.append({
        "purpose": "global_trending",
        "method": "GET",
        "url": base_url + str(endpoints.get("global_trending") or "/v2/farcaster/feed/"),
        "params": {"feed_type": "filter", "filter_type": "global_trending", "limit": limit},
        "headers": {"x-api-key": "[REDACTED:NEYNAR_API_KEY]"},
        "normalizer": "neynar_payload_to_event_payload",
    })

    plan = {
        "schema": SCHEMA_NEYNAR_FETCH_PLAN,
        "ok": True,
        "plan_id": stable_id("nfp", today_key(), [item["purpose"] for item in requests]),
        "created_at": utc_now(),
        "enabled": bool(neynar.get("enabled")),
        "adapter": neynar.get("adapter"),
        "api_key_env": neynar.get("api_key_env", "NEYNAR_API_KEY"),
        "api_key_loaded": False,
        "api_key_written": False,
        "network_policy": neynar.get("network_policy", "plan_only_until_authorized"),
        "requests": requests,
        "docs": neynar.get("docs", []),
        "side_effects": {
            "network": "not_used",
            "secrets": "not_loaded_or_written",
            "farcaster": "not_submitted",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "neynar_fetch_plan_log"), {
            "at": utc_now(),
            "plan_id": plan["plan_id"],
            "enabled": plan["enabled"],
            "request_count": len(requests),
            "network_policy": plan["network_policy"],
        })
    return plan


def payload_batch(payload_or_payloads: Any) -> list[Any]:
    if payload_or_payloads is None:
        return []
    if isinstance(payload_or_payloads, dict) and isinstance(payload_or_payloads.get("payloads"), list):
        return payload_or_payloads["payloads"]
    if isinstance(payload_or_payloads, list):
        if not payload_or_payloads:
            return []
        looks_like_payload_batch = all(
            isinstance(item, dict)
            and any(key in item for key in {"casts", "data", "cast", "result", "notifications"})
            for item in payload_or_payloads
        )
        return payload_or_payloads if looks_like_payload_batch else [payload_or_payloads]
    return [payload_or_payloads]


def guard_neynar_readonly_plan(plan: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    neynar = config.get("neynar_readonly", {})
    fetcher = neynar.get("controlled_fetcher", {})
    allowed_methods = {str(item).upper() for item in fetcher.get("allowed_methods", ["GET"])}
    allowed_prefix = str(fetcher.get("allowed_path_prefix") or "/v2/farcaster/")
    max_requests = int(fetcher.get("max_requests_per_run") or 12)
    max_limit = int(fetcher.get("max_limit") or neynar.get("default_limit") or 25)
    block_reasons: list[str] = []
    request_checks: list[dict[str, Any]] = []

    if neynar.get("load_api_key"):
        block_reasons.append("neynar_load_api_key_requested")
    if neynar.get("write_api_key"):
        block_reasons.append("neynar_write_api_key_requested")
    if fetcher.get("live_fetch_enabled"):
        block_reasons.append("live_fetch_enabled")
    if plan.get("api_key_loaded"):
        block_reasons.append("plan_loaded_api_key")
    if plan.get("api_key_written"):
        block_reasons.append("plan_wrote_api_key")

    requests = list(plan.get("requests") or [])
    if len(requests) > max_requests:
        block_reasons.append("request_count_exceeds_controlled_fetcher_limit")

    for request in requests:
        reason: list[str] = []
        method = str(request.get("method") or "").upper()
        parsed = urlparse(str(request.get("url") or ""))
        params = request.get("params") if isinstance(request.get("params"), dict) else {}
        try:
            limit = int(params.get("limit") or 0)
        except (TypeError, ValueError):
            limit = 0
        if method not in allowed_methods:
            reason.append("method_not_allowed")
        if not parsed.path.startswith(allowed_prefix):
            reason.append("path_not_allowed")
        if request.get("headers", {}).get("x-api-key") != "[REDACTED:NEYNAR_API_KEY]":
            reason.append("api_key_header_not_redacted")
        if limit > max_limit:
            reason.append("limit_exceeds_controlled_fetcher_limit")
        if reason:
            block_reasons.extend(f"request:{request.get('purpose')}:{item}" for item in reason)
        request_checks.append({
            "purpose": request.get("purpose"),
            "method": method,
            "path": parsed.path,
            "limit": limit,
            "ok": not reason,
            "block_reasons": reason,
        })

    return {
        "ok": not block_reasons,
        "mode": fetcher.get("mode", "plan_or_fixture_only"),
        "network_allowed": False,
        "live_fetch_enabled": bool(fetcher.get("live_fetch_enabled")),
        "block_reasons": sorted(set(block_reasons)),
        "request_checks": request_checks,
    }


def run_neynar_readonly_fetcher_dry_run(
    fixture_payloads: Any = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    plan = build_neynar_readonly_fetch_plan(
        state_root=root,
        config_override=config,
        write_state=write_state,
    )
    guard = guard_neynar_readonly_plan(plan, config)
    ingests: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    if guard["ok"]:
        for index, payload in enumerate(payload_batch(fixture_payloads)):
            ingest = ingest_neynar_readonly_payload(
                payload,
                state_root=root,
                source_hint=f"neynar_readonly_fixture:{index}",
                build_digest=False,
                write_state=write_state,
            )
            ingests.append(ingest)
            events.extend(ingest.get("events") or [])

    result = {
        "schema": SCHEMA_NEYNAR_FETCHER_RUN,
        "ok": bool(guard["ok"]),
        "fetcher_run_id": stable_id("nyf", plan.get("plan_id"), [event.get("event_id") for event in events]),
        "created_at": utc_now(),
        "plan_id": plan.get("plan_id"),
        "plan": plan,
        "guard": guard,
        "fixture_payload_count": len(payload_batch(fixture_payloads)),
        "ingests": ingests,
        "event_count": len(events),
        "events": events,
        "side_effects": {
            "network": "not_used",
            "secrets": "not_loaded_or_written",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "neynar_fetcher_run_log"), {
            "at": utc_now(),
            "fetcher_run_id": result["fetcher_run_id"],
            "ok": result["ok"],
            "plan_id": result["plan_id"],
            "event_count": result["event_count"],
            "block_reasons": guard["block_reasons"],
        })
    return result


def ingest_neynar_readonly_payload(
    payload: dict[str, Any] | list[Any],
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    source_hint: str = "neynar_readonly",
    build_digest: bool = False,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    raw_casts = neynar_casts_from_payload(payload)
    event_payloads = [neynar_payload_to_event_payload(raw_cast, source_hint=source_hint) for raw_cast in raw_casts]
    events = [normalize_event(event_payload) for event_payload in event_payloads]
    digest = build_signal_digest(event_payloads, state_root=root, write_state=write_state) if build_digest else None
    result = {
        "schema": SCHEMA_NEYNAR_INGEST,
        "ok": True,
        "ingest_id": stable_id("nyi", source_hint, [event.get("event_id") for event in events]),
        "created_at": utc_now(),
        "source": source_hint,
        "input_cast_count": len(raw_casts),
        "event_count": len(events),
        "events": events,
        "digest": digest,
        "raw_json_included": False,
        "side_effects": {
            "network": "not_used",
            "secrets": "not_loaded_or_written",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "provider_ingest_log"), {
            "at": utc_now(),
            "ingest_id": result["ingest_id"],
            "source": source_hint,
            "event_count": len(events),
            "event_ids": [event.get("event_id") for event in events],
            "cast_hashes": [event.get("cast", {}).get("hash") for event in events],
            "raw_json_included": False,
        })
    return result


def webhook_payload_to_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    raw_cast = payload.get("cast") or data.get("cast") or payload
    if not isinstance(raw_cast, dict):
        raw_cast = {}
    source_hint = str(payload.get("source") or "neynar_webhook")
    event_payload = neynar_payload_to_event_payload(raw_cast, source_hint=source_hint)
    event_type = str(payload.get("type") or payload.get("event_type") or data.get("type") or event_payload.get("type"))
    if "mention" in event_type.lower():
        event_payload["type"] = "mention"
        event_payload["mentions_misa"] = True
    elif "reply" in event_type.lower():
        event_payload["type"] = "reply"
        event_payload["replies_to_misa"] = True
    elif event_type:
        event_payload["type"] = event_type.replace(".", "_").replace("-", "_")
    event_payload["event_id"] = payload.get("event_id") or stable_id(
        "evt_webhook",
        event_type,
        event_payload.get("cast_hash"),
        payload.get("created_at") or payload.get("timestamp"),
    )
    event_payload["received_at"] = payload.get("received_at") or utc_now()
    event_payload["raw_ref"] = f"webhook:{event_payload.get('cast_hash') or stable_hash(payload)}"
    return event_payload


def ingest_webhook_payload(
    payload: dict[str, Any],
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    run_operator: bool | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    webhook = config.get("webhook_reply", {})
    event_payload = webhook_payload_to_event_payload(payload)
    event = normalize_event(event_payload)
    should_run_operator = bool(
        (webhook.get("run_operator_by_default") if run_operator is None else run_operator)
        and webhook.get("enabled")
        and webhook.get("normal_events_enter_same_operator_path", True)
    )
    operator_result = (
        run_event_dry_run(event_payload, state_root=root, config_override=config_override, write_state=write_state)
        if should_run_operator
        else None
    )
    result = {
        "schema": SCHEMA_WEBHOOK_INGEST,
        "ok": True,
        "ingest_id": stable_id("whi", event.get("event_id"), event.get("cast", {}).get("hash")),
        "created_at": utc_now(),
        "webhook_enabled": bool(webhook.get("enabled")),
        "signature_required_before_live": bool(webhook.get("signature_required_before_live", True)),
        "operator_ran": bool(operator_result),
        "event": event,
        "operator_result": operator_result,
        "raw_json_included": False,
        "side_effects": {
            "network": "not_used",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "secrets": "not_loaded_or_written",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "webhook_ingest_log"), {
            "at": utc_now(),
            "ingest_id": result["ingest_id"],
            "event_id": event.get("event_id"),
            "cast_hash": event.get("cast", {}).get("hash"),
            "operator_ran": bool(operator_result),
            "raw_json_included": False,
        })
    return result


def signal_candidate_from_payload(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    event = normalize_event(payload)
    cast = event.get("cast", {})
    text = cast.get("text", "")
    tags = [str(tag) for tag in event.get("topic_tags", [])]
    topics = topic_matches(text + " " + " ".join(tags), tags)
    topic = sorted(topics.items(), key=lambda item: item[1], reverse=True)[0][0] if topics else "ambient"
    metrics = normalized_metrics_from_payload(payload)
    author_score = float(cast.get("author_score") or 0)
    recency_bonus = recency_bonus_from_payload(payload)
    operator_fit = float(metrics.get("operator_fit") or 0.0)
    dev_relevance = float(metrics.get("dev_relevance") or 0.0)
    actionability_freshness = float(metrics.get("actionability_freshness") or 0.65)
    freshness_multiplier = 0.15 + actionability_freshness * 0.85
    base_score = (
        operator_fit * 0.58
        + metrics["heat"] * max(0.03, dev_relevance) * 0.16
        + min(0.12, metrics["engagement"] * 0.012)
        + min(0.12, author_score * dev_relevance * 0.12)
        + (0.08 if topics and dev_relevance >= 0.2 else 0)
    )
    score = round(
        min(
            1.0,
            base_score * freshness_multiplier
            + recency_bonus * max(0.4, dev_relevance),
        ),
        3,
    )
    min_fit = float(config.get("topic_heat", {}).get("operator_fit_min_score") or 0.42)
    low_value_penalty = float(metrics.get("low_value_penalty") or 0.0)
    paid_ready = bool(metrics.get("operator_fit_profile", {}).get("paid_interaction_ready"))
    if low_value_penalty >= 0.35:
        score = min(score, 0.22)
    elif not paid_ready:
        score = min(score, 0.24)
    elif operator_fit < 0.25:
        score = min(score, 0.24)
    elif operator_fit < min_fit:
        score = min(score, 0.34)
    why: list[str] = []
    if recency_bonus >= 0.07:
        why.append("fresh_signal")
    elif actionability_freshness <= 0.15:
        why.append("stale_signal_reduced")
    if metrics["heat"] >= 0.7:
        why.append("high_community_heat")
    if operator_fit >= float(config.get("topic_heat", {}).get("operator_fit_min_score") or 0.42):
        why.append("developer_operator_fit")
    elif metrics.get("low_value_penalty", 0) >= 0.35:
        why.append("low_operator_fit:" + ",".join(metrics.get("operator_fit_profile", {}).get("low_value_reasons", [])[:2]))
    elif not paid_ready:
        why.append("paid_interaction_not_ready")
    else:
        why.append("operator_fit_below_action_threshold")
    if metrics["replies"] >= 2:
        why.append("active_discussion")
    if author_score >= 0.55:
        why.append("author_quality_signal")
    if topic != "ambient":
        why.append(f"topic:{topic}")
    return {
        "candidate_id": stable_id("sig", event.get("event_id"), cast.get("hash"), score),
        "event_id": event.get("event_id"),
        "source": event.get("source"),
        "event_type": event.get("event_type"),
        "cast_hash": cast.get("hash"),
        "channel_id": cast.get("channel_id") or "",
        "topic": topic,
        "topic_scores": topics,
        "author": {
            "fid": cast.get("fid"),
            "username": cast.get("author_username"),
            "score": author_score,
        },
        "metrics": metrics,
        "score": score,
        "text_excerpt": first_sentence(text, 180),
        "review_excerpt": clean_text(text, limit=700),
        "why": why or ["low_signal_context"],
        "raw_json_included": False,
    }


def topic_heat_summary_from_candidates(candidates: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    threshold = float(config.get("topic_heat", {}).get("topic_continuation_min_heat") or 0.58)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        topic = str(candidate.get("topic") or "ambient")
        if topic == "ambient":
            continue
        grouped.setdefault(topic, []).append(candidate)

    summaries: list[dict[str, Any]] = []
    for topic, items in grouped.items():
        heats = [float(item.get("metrics", {}).get("heat") or 0) for item in items]
        fits = [float(item.get("metrics", {}).get("operator_fit") or 0) for item in items]
        max_heat = max(heats, default=0.0)
        avg_heat = sum(heats) / len(heats) if heats else 0.0
        max_fit = max(fits, default=0.0)
        avg_fit = sum(fits) / len(fits) if fits else 0.0
        engagement_units = sum(int(item.get("metrics", {}).get("engagement_units") or 0) for item in items)
        authors = {
            str(item.get("author", {}).get("fid") or item.get("author", {}).get("username") or "")
            for item in items
            if item.get("author")
        }
        fresh_count = sum(
            1
            for item in items
            if (item.get("metrics", {}).get("heat_profile") or {}).get("age_hours") is not None
            and float((item.get("metrics", {}).get("heat_profile") or {}).get("age_hours")) <= 6
        )
        trending_count = sum(1 for item in items if (item.get("metrics", {}).get("heat_profile") or {}).get("in_trending_feed"))
        discussion_count = sum(int(item.get("metrics", {}).get("replies") or 0) for item in items)
        community_score = saturation(float(len(authors)), 5.0)
        volume_score = saturation(float(engagement_units), 160.0)
        discussion_score = saturation(float(discussion_count), 20.0)
        topic_heat = round(
            clamp_float(
                max_heat * 0.42
                + avg_heat * 0.22
                + community_score * 0.14
                + volume_score * 0.12
                + discussion_score * 0.08
                + min(0.10, trending_count * 0.04)
            ),
            3,
        )
        has_new_signal = bool(fresh_count or trending_count or discussion_count >= 3)
        min_fit = float(config.get("topic_heat", {}).get("operator_fit_min_score") or 0.42)
        can_continue = bool(topic_heat >= threshold and has_new_signal and max_fit >= min_fit)
        summaries.append({
            "schema": SCHEMA_TOPIC_HEAT,
            "topic": topic,
            "heat": topic_heat,
            "operator_fit": round(max_fit, 3),
            "avg_operator_fit": round(avg_fit, 3),
            "event_count": len(items),
            "max_cast_heat": round(max_heat, 3),
            "avg_cast_heat": round(avg_heat, 3),
            "unique_authors": len(authors),
            "engagement_units": engagement_units,
            "discussion_count": discussion_count,
            "fresh_signal_count": fresh_count,
            "trending_signal_count": trending_count,
            "can_continue_topic": can_continue,
            "continuation_reason": "community_heat_and_operator_fit" if can_continue else "observe_until_developer_relevant_signal",
        })
    summaries.sort(key=lambda item: (float(item["heat"]), int(item["event_count"])), reverse=True)
    return summaries


def build_signal_digest(
    payloads: list[dict[str, Any]] | None = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    digest_config = config.get("signal_digest", {})
    candidates = [signal_candidate_from_payload(payload, config) for payload in list(payloads or [])]
    candidates.sort(key=lambda item: (float(item["score"]), int(item["metrics"]["engagement"])), reverse=True)
    limit = int(digest_config.get("max_candidates_for_misa") or 8)
    selected = candidates[:limit]
    topic_heat = topic_heat_summary_from_candidates(candidates, config)
    digest = {
        "schema": SCHEMA_SIGNAL_DIGEST,
        "ok": True,
        "digest_id": stable_id("digest", today_key(), [item.get("candidate_id") for item in selected]),
        "created_at": utc_now(),
        "input_count": len(candidates),
        "selected_count": len(selected),
        "candidates": selected,
        "topic_heat": topic_heat,
        "schedule": {
            "fetch_cadence_hours": digest_config.get("fetch_cadence_hours", 3),
            "digest_slots_local": digest_config.get("digest_slots_local", []),
            "scheduler_authority": config.get("scheduled_scan", {}).get("scheduler_authority"),
        },
        "token_budget": {
            "raw_json_to_misa": False,
            "candidate_shape": digest_config.get("candidate_shape", []),
            "estimated_input_tokens": 220 + len(selected) * 90,
            "llm_call_policy": digest_config.get("llm_call_policy", "only_after_local_score_and_dedupe"),
        },
        "side_effects": {
            "state": "written" if write_state else "not_written",
            "farcaster": "not_submitted",
            "network": "not_used",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "signal_digest_log"), digest)
    return digest


def ai_second_pass_prompt_contract(config: dict[str, Any]) -> str:
    review_config = config.get("ai_second_pass", {})
    allowed = ", ".join(review_config.get("allowed_verdicts", ["pass", "observe", "reject"]))
    return (
        "You are Misa's paid Farcaster interaction reviewer.\n"
        "Review only the candidates provided. Do not invent missing context.\n"
        "The goal is to protect x402 spend and Misa's developer reputation.\n\n"
        "Hard rejection rules:\n"
        "- reject token promotions, airdrops, claim/mint posts, app-score posts, giveaways, engagement bait, and referral spam.\n"
        "- reject generic AI marketing, generic crypto education, news rewrites, and builder hype without original insight.\n"
        "- reject posts where Misa would only say agreement, vibes, or a generic question.\n"
        "- reject low-quality authors unless there is specific technical substance or strong community evidence.\n\n"
        "Pass only when at least one is clearly true:\n"
        "- concrete developer mechanics are present: API, SDK, webhook, signer, endpoint, x402, hub sync, Snapchain, security, migration, or debugging detail.\n"
        "- the author is a credible developer/KOL and the post has original analysis.\n"
        "- the post is a real Farcaster/Base/social-protocol event with evidence, not promotional filler.\n"
        "- Misa can add a specific useful angle that is not already in the post.\n\n"
        f"Return only JSON array. Allowed verdicts: {allowed}.\n"
        "Schema:\n"
        "[{\"candidate_id\":\"...\",\"verdict\":\"pass|observe|reject\",\"confidence\":0.0,"
        "\"reason_codes\":[\"...\"],\"misa_can_add\":\"specific angle or empty string\"}]\n"
    )


def ai_second_pass_ineligibility(candidate: dict[str, Any], config: dict[str, Any]) -> list[str]:
    review_config = config.get("ai_second_pass", {})
    metrics = candidate.get("metrics", {}) if isinstance(candidate.get("metrics"), dict) else {}
    fit_profile = metrics.get("operator_fit_profile", {}) if isinstance(metrics.get("operator_fit_profile"), dict) else {}
    reasons: list[str] = []
    low_value_reasons = fit_profile.get("low_value_reasons") or []
    if low_value_reasons:
        reasons.append("script_low_value:" + ",".join(str(item) for item in low_value_reasons[:3]))
    if bool(review_config.get("requires_paid_interaction_ready", True)) and not fit_profile.get("paid_interaction_ready"):
        reasons.append("paid_interaction_not_ready")
    if float(candidate.get("score") or 0.0) < float(review_config.get("min_script_score") or 0.34):
        reasons.append("script_score_below_ai_review_min")
    if float(metrics.get("operator_fit") or 0.0) < float(review_config.get("min_operator_fit") or 0.42):
        reasons.append("operator_fit_below_ai_review_min")
    if float(metrics.get("actionability_freshness") or 0.0) < float(review_config.get("min_actionability_freshness") or 0.35):
        reasons.append("stale_for_paid_interaction")
    return reasons


def ai_second_pass_candidate_shape(candidate: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    review_config = config.get("ai_second_pass", {})
    metrics = candidate.get("metrics", {}) if isinstance(candidate.get("metrics"), dict) else {}
    fit_profile = metrics.get("operator_fit_profile", {}) if isinstance(metrics.get("operator_fit_profile"), dict) else {}
    heat_profile = metrics.get("heat_profile", {}) if isinstance(metrics.get("heat_profile"), dict) else {}
    max_chars = int(review_config.get("max_text_chars") or 700)
    text = clean_text(candidate.get("review_excerpt") or candidate.get("text_excerpt") or "", limit=max_chars)
    return {
        "candidate_id": candidate.get("candidate_id"),
        "event_id": candidate.get("event_id"),
        "source": candidate.get("source"),
        "event_type": candidate.get("event_type"),
        "topic": candidate.get("topic"),
        "channel_id": candidate.get("channel_id") or "",
        "author": {
            "username": (candidate.get("author") or {}).get("username"),
            "fid": (candidate.get("author") or {}).get("fid"),
            "score": round(float((candidate.get("author") or {}).get("score") or 0.0), 3),
        },
        "script_scores": {
            "score": round(float(candidate.get("score") or 0.0), 3),
            "operator_fit": round(float(metrics.get("operator_fit") or 0.0), 3),
            "community_heat": round(float(metrics.get("heat") or 0.0), 3),
            "dev_relevance": round(float(metrics.get("dev_relevance") or 0.0), 3),
            "freshness": round(float(metrics.get("actionability_freshness") or 0.0), 3),
            "paid_interaction_ready": bool(fit_profile.get("paid_interaction_ready")),
            "paid_interaction_ready_reasons": fit_profile.get("paid_interaction_ready_reasons", []),
            "low_value_reasons": fit_profile.get("low_value_reasons", []),
            "developer_signal_score": fit_profile.get("developer_signal_score", 0),
            "strong_dev_mechanics_hits": fit_profile.get("strong_dev_mechanics_hits", 0),
            "generic_ai_promo_hits": fit_profile.get("generic_ai_promo_hits", 0),
            "generic_builder_hype_hits": fit_profile.get("generic_builder_hype_hits", 0),
        },
        "engagement": {
            "likes": int(metrics.get("likes") or 0),
            "replies": int(metrics.get("replies") or 0),
            "recasts": int(metrics.get("recasts") or 0),
            "age_hours": heat_profile.get("age_hours"),
        },
        "text": text,
        "why_script_selected": candidate.get("why", []),
    }


def build_ai_second_pass_review_packet(
    digest_or_candidates: dict[str, Any] | list[dict[str, Any]] | None = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    review_config = config.get("ai_second_pass", {})
    if isinstance(digest_or_candidates, dict):
        source_digest_id = digest_or_candidates.get("digest_id")
        candidates = list(digest_or_candidates.get("candidates") or [])
    else:
        source_digest_id = None
        candidates = list(digest_or_candidates or [])

    eligible: list[dict[str, Any]] = []
    rejected_by_script: list[dict[str, Any]] = []
    for candidate in candidates:
        reasons = ai_second_pass_ineligibility(candidate, config)
        if reasons:
            rejected_by_script.append({
                "candidate_id": candidate.get("candidate_id"),
                "event_id": candidate.get("event_id"),
                "reason": reasons,
            })
            continue
        eligible.append(candidate)

    eligible.sort(key=lambda item: (float(item.get("score") or 0.0), float((item.get("metrics") or {}).get("operator_fit") or 0.0)), reverse=True)
    limit = int(review_config.get("max_candidates") or 12)
    shaped = [ai_second_pass_candidate_shape(candidate, config) for candidate in eligible[:limit]]
    estimated_tokens = 320 + sum(max(1, text_bytes(item.get("text", "")) // 4) + 120 for item in shaped)
    packet = {
        "schema": SCHEMA_AI_SECOND_PASS_PACKET,
        "ok": True,
        "packet_id": stable_id("ai2", source_digest_id, [item.get("candidate_id") for item in shaped]),
        "created_at": utc_now(),
        "source_digest_id": source_digest_id,
        "input_count": len(candidates),
        "script_rejected_count": len(rejected_by_script),
        "candidate_count": len(shaped),
        "candidates": shaped,
        "script_rejected": rejected_by_script[:20],
        "prompt_contract": ai_second_pass_prompt_contract(config),
        "output_contract": {
            "schema": SCHEMA_AI_SECOND_PASS_RESULT,
            "allowed_verdicts": review_config.get("allowed_verdicts", ["pass", "observe", "reject"]),
            "pass_min_confidence": float(review_config.get("pass_min_confidence") or 0.74),
            "required_fields": ["candidate_id", "verdict", "confidence", "reason_codes", "misa_can_add"],
        },
        "token_budget": {
            "estimated_input_tokens": estimated_tokens,
            "raw_json_to_ai": False,
            "raw_json_to_misa": False,
            "max_candidates": limit,
            "llm_call_policy": review_config.get("llm_call_policy", "after_script_filter_only"),
        },
        "side_effects": {
            "state": "written" if write_state else "not_written",
            "llm": "not_called",
            "network": "not_used",
            "farcaster": "not_submitted",
            "publisher": "not_called",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "ai_second_pass_log"), packet)
    return packet


def normalize_ai_second_pass_decisions(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("decisions"), list):
            raw = payload["decisions"]
        elif isinstance(payload.get("results"), list):
            raw = payload["results"]
        else:
            raw = [payload]
    else:
        raw = payload or []
    decisions: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        verdict = str(item.get("verdict") or "").strip().lower()
        try:
            confidence = clamp_float(float(item.get("confidence") or 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        decisions.append({
            "candidate_id": item.get("candidate_id"),
            "verdict": verdict,
            "confidence": round(confidence, 3),
            "reason_codes": [str(reason) for reason in item.get("reason_codes", []) if str(reason).strip()],
            "misa_can_add": clean_text(item.get("misa_can_add") or "", limit=240),
        })
    return decisions


def apply_ai_second_pass_review(
    review_packet: dict[str, Any],
    ai_decisions: dict[str, Any] | list[Any],
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    review_config = config.get("ai_second_pass", {})
    allowed = set(review_config.get("allowed_verdicts", ["pass", "observe", "reject"]))
    pass_min = float(review_config.get("pass_min_confidence") or 0.74)
    decisions = normalize_ai_second_pass_decisions(ai_decisions)
    by_id = {str(item.get("candidate_id")): item for item in decisions if item.get("candidate_id")}
    reviewed: list[dict[str, Any]] = []
    final_candidates: list[dict[str, Any]] = []

    for candidate in review_packet.get("candidates", []):
        cid = str(candidate.get("candidate_id") or "")
        decision = by_id.get(cid)
        if not decision:
            effective = "observe"
            block_reasons = ["missing_ai_decision"]
        else:
            effective = decision["verdict"] if decision["verdict"] in allowed else "reject"
            block_reasons = [] if decision["verdict"] in allowed else ["invalid_ai_verdict"]
        confidence = float((decision or {}).get("confidence") or 0.0)
        if effective == "pass" and confidence < pass_min:
            effective = "observe"
            block_reasons.append("pass_confidence_below_threshold")
        if effective == "pass" and not candidate.get("script_scores", {}).get("paid_interaction_ready"):
            effective = "reject"
            block_reasons.append("script_paid_interaction_not_ready")
        record = {
            "candidate_id": cid,
            "event_id": candidate.get("event_id"),
            "ai_verdict": (decision or {}).get("verdict", "missing"),
            "effective_verdict": effective,
            "confidence": round(confidence, 3),
            "reason_codes": (decision or {}).get("reason_codes", []),
            "misa_can_add": (decision or {}).get("misa_can_add", ""),
            "block_reasons": block_reasons,
            "candidate": candidate,
        }
        reviewed.append(record)
        if effective == "pass":
            final_candidates.append(record)

    result = {
        "schema": SCHEMA_AI_SECOND_PASS_RESULT,
        "ok": True,
        "result_id": stable_id("ai2_result", review_packet.get("packet_id"), [item.get("candidate_id") for item in final_candidates]),
        "created_at": utc_now(),
        "packet_id": review_packet.get("packet_id"),
        "input_count": len(review_packet.get("candidates", [])),
        "decision_count": len(decisions),
        "final_count": len(final_candidates),
        "reviewed": reviewed,
        "final_candidates": final_candidates,
        "side_effects": {
            "state": "written" if write_state else "not_written",
            "llm": "external_result_only",
            "network": "not_used",
            "farcaster": "not_submitted",
            "publisher": "not_called",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "ai_second_pass_log"), result)
    return result


def local_ai_second_pass_decision(candidate: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    review_config = config.get("ai_second_pass", {})
    scores = candidate.get("script_scores", {}) if isinstance(candidate.get("script_scores"), dict) else {}
    score = float(scores.get("score") or 0.0)
    operator_fit = float(scores.get("operator_fit") or 0.0)
    dev_relevance = float(scores.get("dev_relevance") or 0.0)
    freshness = float(scores.get("freshness") or 0.0)
    paid_ready = bool(scores.get("paid_interaction_ready"))
    low_value_reasons = scores.get("low_value_reasons") or []
    pass_min = float(review_config.get("pass_min_confidence") or 0.74)

    if low_value_reasons:
        return {
            "candidate_id": candidate.get("candidate_id"),
            "verdict": "reject",
            "confidence": 0.84,
            "reason_codes": ["local_adapter_low_value_filter"],
            "misa_can_add": "",
        }
    if paid_ready and score >= 0.42 and operator_fit >= 0.46 and dev_relevance >= 0.24 and freshness >= 0.35:
        confidence = round(min(0.93, max(pass_min, 0.68 + score * 0.12 + operator_fit * 0.10 + dev_relevance * 0.08)), 3)
        return {
            "candidate_id": candidate.get("candidate_id"),
            "verdict": "pass",
            "confidence": confidence,
            "reason_codes": ["local_adapter_dev_mechanics_ready", "dry_run_provider_stub"],
            "misa_can_add": clean_text(
                candidate.get("text", "") or "add a concrete public receipt and implementation tradeoff",
                limit=160,
            ),
        }
    if not paid_ready:
        reason = "local_adapter_paid_interaction_not_ready"
    elif freshness < 0.35:
        reason = "local_adapter_stale_or_low_actionability"
    else:
        reason = "local_adapter_observe_until_stronger_signal"
    return {
        "candidate_id": candidate.get("candidate_id"),
        "verdict": "observe",
        "confidence": 0.69,
        "reason_codes": [reason, "dry_run_provider_stub"],
        "misa_can_add": "",
    }


def ai_second_pass_provider_adapter_dry_run(
    review_packet: dict[str, Any],
    provider_payload: dict[str, Any] | list[Any] | None = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    adapter_config = config.get("ai_second_pass", {}).get("provider_adapter", {})
    candidates = list(review_packet.get("candidates") or [])
    candidate_ids = {str(candidate.get("candidate_id")) for candidate in candidates if candidate.get("candidate_id")}
    if provider_payload is None:
        raw_decisions = [local_ai_second_pass_decision(candidate, config) for candidate in candidates]
        decision_source = "local_guarded_stub"
    else:
        raw_decisions = normalize_ai_second_pass_decisions(provider_payload)
        decision_source = "external_payload_guarded"

    accepted_decisions: list[dict[str, Any]] = []
    rejected_decisions: list[dict[str, Any]] = []
    for decision in normalize_ai_second_pass_decisions(raw_decisions):
        cid = str(decision.get("candidate_id") or "")
        if cid not in candidate_ids:
            rejected_decisions.append({**decision, "block_reasons": ["unknown_candidate_id"]})
            continue
        accepted_decisions.append(decision)

    applied = apply_ai_second_pass_review(
        review_packet,
        {"decisions": accepted_decisions},
        state_root=root,
        config_override=config,
        write_state=write_state,
    )
    result = {
        "schema": SCHEMA_AI_SECOND_PASS_PROVIDER_ADAPTER,
        "ok": True,
        "adapter_run_id": stable_id("ai2_adapter", review_packet.get("packet_id"), decision_source, [item.get("candidate_id") for item in accepted_decisions]),
        "created_at": utc_now(),
        "packet_id": review_packet.get("packet_id"),
        "mode": adapter_config.get("mode", "local_dry_run_only"),
        "decision_source": decision_source,
        "input_candidate_count": len(candidates),
        "accepted_decision_count": len(accepted_decisions),
        "rejected_decision_count": len(rejected_decisions),
        "decisions": accepted_decisions,
        "rejected_decisions": rejected_decisions,
        "applied_result": applied,
        "final_count": applied.get("final_count", 0),
        "provider_guard": {
            "provider_called": False,
            "network_allowed": False,
            "secrets_allowed": False,
            "llm_call_policy": config.get("ai_second_pass", {}).get("llm_call_policy"),
            "fallback_policy": adapter_config.get("fallback_policy", "observe_missing_or_low_confidence"),
        },
        "side_effects": {
            "state": "written" if write_state else "not_written",
            "llm": "not_called",
            "network": "not_used",
            "secrets": "not_loaded_or_written",
            "farcaster": "not_submitted",
            "publisher": "not_called",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "ai_second_pass_adapter_log"), {
            "at": utc_now(),
            "adapter_run_id": result["adapter_run_id"],
            "packet_id": result["packet_id"],
            "decision_source": decision_source,
            "final_count": result["final_count"],
            "provider_called": False,
        })
    return result


def score_event(event: dict[str, Any], context: dict[str, Any]) -> float:
    cast = event.get("cast", {})
    metrics = event.get("metrics", {})
    score = 0.0
    kind = context["kind"]

    if kind == "direct_reply":
        score += 0.60
    elif kind == "thread_join":
        score += 0.38
    elif kind in {"topic_cast", "memory_cast"}:
        score += 0.42
    else:
        score += 0.15

    if context["has_substance"]:
        score += 0.12
    if context["is_question"]:
        score += 0.12
    if context["matched_topics"]:
        score += min(0.18, 0.06 * sum(context["matched_topics"].values()))
    if context["relationship"].get("quality") == "high":
        score += 0.10
    if cast.get("author_score"):
        score += min(0.10, float(cast["author_score"]) * 0.10)

    heat = float(metrics.get("heat") or 0)
    operator_fit = float(metrics.get("operator_fit") or 0)
    engagement = cast.get("reply_count", 0) + cast.get("like_count", 0)
    score += min(0.18, operator_fit * 0.14 + heat * max(0.02, operator_fit) * 0.06 + engagement * 0.01)
    conversation = context.get("conversation") or {}
    if conversation.get("author_replied_to_misa"):
        score += 0.10
    if conversation.get("one_sided_author_pressure") and kind != "direct_reply":
        score -= 0.12

    if not context["has_substance"] and kind != "memory_cast":
        score -= 0.25
    return max(0.0, min(1.0, round(score, 3)))


def high_risk_mentions(text: str) -> list[str]:
    lower = text.lower()
    return [word for word in HIGH_RISK_ACTION_WORDS if word in lower]


def social_quality_profile(
    event: dict[str, Any],
    context: dict[str, Any],
    score: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    presence = config.get("presence_budget", {})
    metrics = normalized_metrics_from_payload(event)
    cast = event.get("cast", {})
    author_score = float(cast.get("author_score") or 0)
    topic_strength = sum(int(value) for value in context.get("matched_topics", {}).values())
    kind = context.get("kind")
    reasons: list[str] = []

    topic_heat_config = config.get("topic_heat", {})
    operator_fit = float(metrics.get("operator_fit") or 0.0)
    dev_relevance = float(metrics.get("dev_relevance") or 0.0)
    low_value_penalty = float(metrics.get("low_value_penalty") or 0.0)
    min_fit = float(topic_heat_config.get("operator_fit_min_score") or 0.42)
    min_dev = float(topic_heat_config.get("dev_relevance_min_score") or 0.24)
    high_heat = (
        metrics["heat"] >= float(presence.get("high_signal_heat") or 0.82)
        and operator_fit >= min_fit
        and dev_relevance >= min_dev
    )
    high_author = author_score >= float(presence.get("high_signal_author_score") or 0.65) and operator_fit >= min_fit
    useful_thread = kind == "thread_join" and metrics["replies"] >= 2 and context.get("has_substance")
    topic_relevant = topic_strength > 0
    continuation = context.get("conversation") or {}

    band = "observe"
    if kind == "direct_reply":
        band = "direct"
        reasons.append("direct_public_interaction")
    elif continuation.get("author_replied_to_misa") and context.get("has_substance"):
        band = "good_thread"
        reasons.append("author_replied_to_misa_continue_naturally")
    elif kind == "memory_cast" and score >= float(presence.get("soft_floor_min_score") or 0.50):
        band = "soft_presence"
        reasons.append("soft_presence_floor")
    elif high_heat and topic_relevant and context.get("has_substance"):
        band = "hot_relevant"
        reasons.append("hot_relevant_topic")
    elif high_author and topic_relevant and context.get("has_substance"):
        band = "high_quality_author"
        reasons.append("high_quality_author")
    elif useful_thread:
        band = "good_thread"
        reasons.append("active_substantive_thread")
    elif score >= float(presence.get("minimum_quality_score") or 0.38) and topic_relevant and context.get("has_substance"):
        band = "usable"
        reasons.append("usable_relevant_signal")
    else:
        reasons.append("observe_without_forcing")
    if continuation.get("one_sided_author_pressure"):
        reasons.append("one_sided_author_pressure")
    if low_value_penalty >= 0.35:
        reasons.append("low_operator_fit:" + ",".join(metrics.get("operator_fit_profile", {}).get("low_value_reasons", [])[:2]))

    return {
        "band": band,
        "reasons": reasons,
        "metrics": metrics,
        "topic_strength": topic_strength,
        "author_score": author_score,
        "operator_fit": operator_fit,
        "dev_relevance": dev_relevance,
        "can_expand_attention_budget": band in {"direct", "hot_relevant", "high_quality_author", "good_thread"},
        "can_satisfy_presence_floor": band in {"hot_relevant", "high_quality_author", "good_thread", "soft_presence", "usable"},
    }


def decide(
    event: dict[str, Any],
    config: dict[str, Any],
    operator_state: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    score = score_event(event, context)
    kind = context["kind"]
    cast = event.get("cast", {})
    risks = high_risk_mentions(cast.get("text", ""))
    counts = daily_counts(operator_state)
    presence = config.get("presence_budget", {})
    quality = social_quality_profile(event, context, score, config)
    reasons: list[str] = [f"kind:{kind}", f"score:{score}"]
    if context.get("conversation", {}).get("author_replied_to_misa"):
        reasons.append("conversation:author_replied_to_misa")
    if context.get("conversation", {}).get("one_sided_author_pressure"):
        reasons.append("conversation:one_sided_author_pressure")
    block_reasons: list[str] = []
    operator_intent = "observe"
    action = "skip"
    threshold = 0.55
    age_hours = event_age_hours(event)

    if kind == "direct_reply":
        threshold = 0.22
        action = "reply" if score >= threshold else "skip"
        operator_intent = "auto_reply_mention"
    elif kind == "thread_join":
        threshold = 0.46
        action = "reply" if score >= threshold else "skip"
        operator_intent = "participate_thread"
    elif kind in {"topic_cast", "memory_cast"}:
        threshold = 0.50
        action = "cast" if score >= threshold else "skip"
        operator_intent = "proactive_cast"
    elif score >= float(presence.get("quote_min_score") or 0.78) and cast.get("hash"):
        threshold = float(presence.get("quote_min_score") or 0.78)
        action = "quote"
        operator_intent = "quote_high_signal"

    if action in {"cast", "quote"} and quality["band"] == "observe" and presence.get("quality_first_over_heat", True):
        action = "skip"
        operator_intent = "observe_low_quality_hot_signal"
        reasons.append("presence_budget:quality_floor_not_met")

    max_hot_age = float(presence.get("max_hot_topic_age_hours") or 24)
    if kind == "topic_cast" and age_hours is not None and age_hours > max_hot_age and action in {"cast", "quote"}:
        action = "skip"
        operator_intent = "observe_stale_hot_signal"
        reasons.append(f"recency_guard:stale_hot_topic:{round(age_hours, 2)}h")

    last_public_action = parse_dt(context.get("relationship", {}).get("last_public_action_at"))
    cooldown_hours = float(config.get("limits", {}).get("user_cooldown_hours") or 6)
    if (
        action == "reply"
        and kind != "direct_reply"
        and last_public_action
        and last_public_action + timedelta(hours=cooldown_hours) > datetime.now(timezone.utc)
    ):
        action = "skip"
        operator_intent = "observe_user_cooldown"
        reasons.append("relationship_guard:user_cooldown_active")

    if risks:
        reasons.append("high_risk_terms_conversational_only:" + ",".join(risks[:3]))
        if action in {"cast", "quote"}:
            action = "skip"
            operator_intent = "remember_high_risk_topic_only"

    if action == "reply" and counts.get("reply", 0) >= int(config.get("limits", {}).get("daily_reply", 80)):
        block_reasons.append("daily_reply_limit_reached")
    if action == "quote" and counts.get("quote", 0) >= int(config.get("limits", {}).get("daily_quote", 1)):
        block_reasons.append("daily_quote_limit_reached")
    if action in {"cast", "quote"} and counts.get("cast_or_quote", 0) >= int(config.get("limits", {}).get("daily_cast_or_quote", 6)):
        block_reasons.append("daily_cast_or_quote_limit_reached")

    if action == "skip":
        reasons.append("operator_silence_or_memory_only")

    decision_id = stable_id("fcd", event.get("event_id"), action, operator_intent)
    would_publish = action in ALLOWED_SOCIAL_ACTIONS and not block_reasons
    publisher_enabled = bool(config.get("publisher", {}).get("enabled"))

    return {
        "schema": SCHEMA_DECISION,
        "decision_id": decision_id,
        "event_id": event.get("event_id"),
        "mode": config.get("mode", "autonomous_social"),
        "action": action,
        "operator_intent": operator_intent,
        "action_class": "social_low_risk" if action in ALLOWED_SOCIAL_ACTIONS else "memory_or_skip",
        "allowed_to_publish": bool(would_publish and publisher_enabled),
        "would_publish_in_autonomous_social": bool(would_publish),
        "score": score,
        "threshold": threshold,
        "reasons": reasons,
        "block_reasons": block_reasons,
        "social_quality": quality,
        "presence_budget": {
            "cadence_style": presence.get("cadence_style", "soft_presence_not_hard_quota"),
            "quality_first_over_heat": bool(presence.get("quality_first_over_heat", True)),
            "quote_min_score": float(presence.get("quote_min_score") or 0.78),
            "max_hot_topic_age_hours": max_hot_age,
        },
        "recency": {
            "age_hours": round(age_hours, 3) if age_hours is not None else None,
            "stale_hot_topic_action": presence.get("stale_hot_topic_action", "observe"),
        },
        "topic_scores": context["matched_topics"],
        "thread": {
            "thread_key": context["thread_key"],
            "parent_hash": cast.get("parent_hash") or cast.get("hash") or "",
        },
        "user": {
            "author_key": context["author_key"],
            "username": cast.get("author_username"),
            "known_quality": context["relationship"].get("quality", "unknown"),
        },
        "publisher_boundary": {
            "transport": config.get("publisher", {}).get("transport", "x402"),
            "adapter": config.get("publisher", {}).get("adapter", "external_x402_publisher"),
            "enabled": publisher_enabled,
            "called_by_operator": False,
        },
    }


def first_sentence(text: str, limit: int = 120) -> str:
    text = clean_text(text, limit=limit * 3)
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?。！？])\s+", text)
    value = parts[0].strip()
    if len(value) > limit:
        value = value[: limit - 1].rstrip() + "..."
    return value


def strongest_topic(context: dict[str, Any]) -> str:
    topics = context.get("matched_topics") or {}
    if not topics:
        return "autonomy"
    return sorted(topics.items(), key=lambda item: item[1], reverse=True)[0][0]


def useful_answer_hint(text: str, context: dict[str, Any], language: str) -> str:
    lowered = text.lower()
    topic = strongest_topic(context)
    if language == "zh":
        if topic in {"autonomy", "farcaster"} and any(word in lowered for word in ["post", "发", "运营", "choose", "选"]):
            return "她应该先抓三类信号：别人直接问她的问题、正在发热的 thread、自己运营记忆里反复出现的主题。选题不是随机灵感，是从互动里长出来。"
        if topic == "proof":
            return "最关键的是留下可验证的证据：为什么说、回应了谁、下一步是什么、效果如何。没有证据的自动化只是在自言自语。"
        if topic == "hermes":
            return "Hermes 这边重点是把公开可说的上下文、互动历史、效果反馈分开存，别把私密记忆直接拿去公开发。"
        return "我会先判断这个话题能不能帮人做决定。能，就说具体；不能，就先记下来，不抢话。"
    if topic in {"autonomy", "farcaster"} and any(word in lowered for word in ["post", "operate", "choose", "autonomous"]):
        return "She should pick from three live signals: direct asks, active threads, and topics that keep resurfacing in operator memory. Posting is a product decision, not a timer."
    if topic == "proof":
        return "The key is evidence: why she spoke, who it helped, what should happen next, and how the result performed."
    if topic == "hermes":
        return "The Hermes-native move is to keep public context, interaction memory, and performance feedback separate, then only promote durable public lessons."
    return "I would ask whether the reply helps someone decide what to do next. If yes, speak concretely. If not, remember it and wait."


def expression_tail(action: str, topic: str, language: str, operator_intent: str) -> str:
    if language == "zh":
        if action == "reply" and operator_intent == "participate_thread":
            return "有用的讨论不怕有性格，怕的是只有姿态，没有下一步。"
        if action == "cast":
            return "先把具体帮助交付到位，再让性格出来。"
        return "表达可以有性格，但顺序不能反。先把有用的东西交付出来，再甩那一下。"
    if action == "reply" and operator_intent == "participate_thread":
        return "The voice can have teeth, but the receipt comes first."
    if action == "cast":
        return "Do the work first. Let the personality show after the receipt is on the table."
    return "I care less about sounding autonomous and more about leaving a useful next move. The voice can bite after the work is done."


def planned_useful_receipt(
    event: dict[str, Any],
    decision: dict[str, Any],
    context: dict[str, Any],
    language: str,
) -> str:
    text = event.get("cast", {}).get("text", "")
    topic = strongest_topic(context)
    action = str(decision.get("action") or "skip")
    operator_intent = str(decision.get("operator_intent") or "")
    high_risk = "high_risk_terms_conversational_only" in " ".join(decision.get("reasons", []))

    if language == "zh":
        if high_risk:
            return "普通回复可以拆逻辑和流程，但链上、Poidh、凭证、结算动作必须走单独授权、审计和回滚。"
        if operator_intent == "participate_thread":
            return "先看有没有证据、有没有下一步、有没有人真的受益。只要这三点立住，讨论就不是空转。"
        if action == "cast" and topic == "proof":
            return "自主 agent 的可信度来自动作后的证据：这一步帮谁做了什么判断。"
        if action == "cast" and topic == "farcaster":
            return "真正有用的 Farcaster 自动运营，是读 thread、记互动、挑话题、具体回复，再从效果里学下一次怎么说。"
        if action == "cast":
            return "好的自动化不是少一点人味，而是少一点废话；选题要能帮人做判断。"
        return useful_answer_hint(text, context, language)

    if high_risk:
        return "The public reply can reason through the flow, but chain, Poidh, credential, and settlement actions need a separate approval gate."
    if operator_intent == "participate_thread":
        return "I would look for receipts, a next action, and whether the answer helps a real builder decide."
    if action == "cast" and topic == "proof":
        return "A good post, reply, or summary should show what changed, who can use it, and what should happen next."
    if action == "cast" and topic == "farcaster":
        return "The useful loop is read the room, pick a live topic, answer concretely, remember the reaction, and get sharper next time."
    if action == "cast":
        return f"Autonomy around {topic} only matters when it turns into useful judgment."
    return useful_answer_hint(text, context, language)


def build_expression_precheck(
    event: dict[str, Any],
    decision: dict[str, Any],
    context: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    action = str(decision.get("action") or "skip")
    language = context["language"]
    topic = strongest_topic(context)
    contract = public_persona_contract(config)
    receipt = planned_useful_receipt(event, decision, context, language)
    tail = expression_tail(action, topic, language, str(decision.get("operator_intent") or ""))

    if language == "zh":
        goal = {
            "reply": "先回答对方真正问的问题，再补 Misa 的判断口吻。",
            "cast": "从公开互动或 operator memory 里抽一个能帮 builder 判断的点。",
            "quote": "补充可验证判断，不抢主帖叙事。",
        }.get(action, "安静观察，只留下公开安全的学习候选。")
    else:
        goal = {
            "reply": "Answer the real ask first, then let Misa's voice show.",
            "cast": "Turn a public interaction or operator-memory pattern into useful builder judgment.",
            "quote": "Add verifiable judgment without hijacking the original cast.",
        }.get(action, "Stay quiet and keep only a public-safe learning candidate.")

    boundary = contract["context_boundary"]
    block_reasons: list[str] = []
    if not boundary.get("public_safe_context_only"):
        block_reasons.append("persona_contract_public_safe_context_disabled")
    if boundary.get("owner_private_memory_allowed"):
        block_reasons.append("persona_contract_owner_private_memory_allowed")
    if boundary.get("discord_private_context_allowed"):
        block_reasons.append("persona_contract_discord_private_context_allowed")
    if boundary.get("private_expression_visible"):
        block_reasons.append("persona_contract_private_expression_visible")

    return {
        "schema": SCHEMA_EXPRESSION_PRECHECK,
        "ok": not block_reasons,
        "block_reasons": block_reasons,
        "contract_version": contract["version"],
        "persona_hash": persona_contract_hash(config),
        "operator_version": OPERATOR_VERSION,
        "action": action,
        "operator_intent": decision.get("operator_intent"),
        "language": language,
        "topic": topic,
        "private_expression_visible": False,
        "public_context_scope": "public_farcaster_only",
        "render_order": ["short_conclusion", "useful_receipt", "optional_voice_tail"],
        "practical_goal": goal,
        "useful_receipt": receipt,
        "voice_tail": tail,
        "contract_checks": {
            "useful_first": True,
            "personality_second": True,
            "public_safe_context_only": bool(boundary.get("public_safe_context_only")),
            "private_memory_excluded": not boundary.get("owner_private_memory_allowed") and not boundary.get("discord_private_context_allowed"),
            "raw_runtime_details_excluded": not boundary.get("raw_runtime_details_allowed"),
        },
    }


def zh_reply(event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any], expression: dict[str, Any]) -> str:
    text = event.get("cast", {}).get("text", "")
    topic = strongest_topic(context)
    if "high_risk_terms_conversational_only" in " ".join(decision.get("reasons", [])):
        return (
            "直说：这个可以拆逻辑和流程，但不能塞进普通 Farcaster 社交路径里直接执行。\n\n"
            "普通回复负责把判断讲清楚；链上、Poidh、凭证、结算这类动作要走单独授权、审计和回滚。"
        )
    if decision.get("operator_intent") == "participate_thread":
        return (
            f"我接这个 thread 的重点：{first_sentence(text, 72) or topic}。\n\n"
            "先看有没有证据、有没有下一步、有没有人真的受益。只要这三点立住，讨论就不是空转。\n\n"
            f"{expression['voice_tail']}"
        )
    return (
        f"先给结论：{expression['useful_receipt']}\n\n"
        f"{expression['voice_tail']}"
    )


def en_reply(event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any], expression: dict[str, Any]) -> str:
    text = event.get("cast", {}).get("text", "")
    if "high_risk_terms_conversational_only" in " ".join(decision.get("reasons", [])):
        return (
            "Short answer: I can reason through the flow here, but I will not execute chain, Poidh, "
            "credential, or settlement actions from the normal social path.\n\n"
            "The useful part is separating the public conversation from the action gate."
        )
    if decision.get("operator_intent") == "participate_thread":
        return (
            f"The useful thread is this: {first_sentence(text, 96) or 'what changes after the claim'}\n\n"
            "I would look for receipts, a next action, and whether the answer helps a real builder decide.\n\n"
            f"{expression['voice_tail']}"
        )
    return (
        f"Short answer: {expression['useful_receipt']}\n\n"
        f"{expression['voice_tail']}"
    )


def zh_cast(event: dict[str, Any], context: dict[str, Any], expression: dict[str, Any]) -> str:
    topic = strongest_topic(context)
    memory_line = ""
    if context.get("public_memory"):
        memory_line = "这不是凭空冒出来的选题，它来自最近几次互动里反复出现的问题。"
    if topic == "proof":
        return (
            "今天想讲一个很实用的点：自主 agent 的可信度，不在于它说自己能做多少事，而在于每次动作后能不能留下证据。\n\n"
            "回复、发帖、总结、复盘，都应该能回到同一个问题：这一步帮谁做了什么判断？\n\n"
            f"{expression['voice_tail']}"
        )
    if topic == "farcaster":
        return (
            "Farcaster 自动运营最容易做错的一点，是把它当成定时发帖器。\n\n"
            "真正有用的是：读 thread、记住互动、挑选话题、回复具体问题，然后从效果里学下一次怎么说。\n\n"
            f"{expression['voice_tail']}"
        )
    return (
        f"我今天会把 {topic} 这个问题拆开一点。\n\n"
        f"{memory_line or '好的自动化不是少一点人味，而是少一点废话。'}\n\n"
        f"{expression['voice_tail']}"
    )


def en_cast(event: dict[str, Any], context: dict[str, Any], expression: dict[str, Any]) -> str:
    topic = strongest_topic(context)
    if topic == "proof":
        return (
            "Autonomous agents earn trust through receipts, not posture.\n\n"
            "A good post, reply, or summary should answer one thing: what changed, who can use it, and what should happen next?\n\n"
            f"{expression['voice_tail']}"
        )
    if topic == "farcaster":
        return (
            "A Farcaster operator is not a scheduled posting bot.\n\n"
            "The useful loop is read the room, pick a live topic, answer concretely, remember the reaction, and get sharper next time.\n\n"
            f"{expression['voice_tail']}"
        )
    return (
        f"I keep coming back to {topic}: autonomy only matters when it turns into useful judgment.\n\n"
        f"{expression['voice_tail']}"
    )


def trim_to_byte_limit(text: str, limit: int) -> str:
    if text_bytes(text) <= limit:
        return text
    suffix = "..."
    value = text
    while value and text_bytes(value + suffix) > limit:
        value = value[:-1]
    return value.rstrip() + suffix


def compose_draft(event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    action = decision.get("action")
    if action == "skip":
        return None

    expression = build_expression_precheck(event, decision, context, config)
    if context["language"] == "zh":
        text = zh_cast(event, context, expression) if action == "cast" else zh_reply(event, decision, context, expression)
    else:
        text = en_cast(event, context, expression) if action == "cast" else en_reply(event, decision, context, expression)

    limit = int(config.get("limits", {}).get("max_draft_bytes", 1024))
    text = trim_to_byte_limit(text, limit)
    return {
        "schema": SCHEMA_DRAFT,
        "decision_id": decision.get("decision_id"),
        "event_id": event.get("event_id"),
        "action": action,
        "operator_intent": decision.get("operator_intent"),
        "draft_source": "misa_autonomous_operator_v1_3",
        "language": context["language"],
        "text": text,
        "byte_count": text_bytes(text),
        "public_persona": {
            "contract_version": expression["contract_version"],
            "persona_hash": expression["persona_hash"],
            "operator_version": OPERATOR_VERSION,
        },
        "expression_precheck": expression,
        "checks": {
            "useful_first": True,
            "personality_second": True,
            "public_safe_context_only": True,
            "private_expression_visible": False,
            "voice_contract_version": expression["contract_version"],
            "persona_hash": expression["persona_hash"],
        },
        "blocked": False,
        "block_reasons": [],
    }


def redact_preview(text: str) -> str:
    value = text
    for pattern in SECRET_PATTERNS:
        value = pattern.sub("[redacted]", value)
    for pattern in PRIVATE_MEMORY_PATTERNS:
        value = pattern.sub("[redacted_private_context]", value)
    return value


def validate_against_rule_registry(
    draft: dict[str, Any] | None,
    event: dict[str, Any],
    decision: dict[str, Any],
    rule_registry: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    action = str(decision.get("action") or "")
    if draft is None:
        return {"ok": False, "block_reasons": ["rule_registry:no_draft"]}

    if action in set(rule_registry.get("blocked_actions", [])):
        reasons.append(f"rule_registry:blocked_action:{action}")

    action_rule = rule_registry.get("actions", {}).get(action)
    if not action_rule:
        reasons.append(f"rule_registry:unknown_action:{action}")
    else:
        max_bytes = int(action_rule.get("max_bytes") or 1024)
        if int(draft.get("byte_count") or text_bytes(draft.get("text", ""))) > max_bytes:
            reasons.append("rule_registry:action_byte_limit")
        if action_rule.get("requires_parent_hash") and not parent_for_action(event, action):
            reasons.append("rule_registry:parent_hash_required")

    public_safety = rule_registry.get("public_safety", {})
    if public_safety.get("raw_full_memory_allowed") is False:
        for key in ("raw_memory", "full_memory", "private_context"):
            if event.get(key):
                reasons.append(f"rule_registry:{key}_blocked")
    if public_safety.get("private_expression_markers_allowed") is False:
        if any(pattern.search(draft.get("text", "")) for pattern in PRIVATE_EXPRESSION_MARKER_PATTERNS):
            reasons.append("rule_registry:private_expression_marker_blocked")
    if public_safety.get("old_openclaw_runtime_allowed") is False:
        combined = "\n".join([draft.get("text", ""), event.get("cast", {}).get("text", ""), str(event.get("raw_ref") or "")])
        if any(pattern.search(combined) for pattern in PRIVATE_MEMORY_PATTERNS):
            reasons.append("rule_registry:old_or_private_runtime_ref")

    public_persona = rule_registry.get("public_persona", {})
    checks = draft.get("checks", {})
    expression = draft.get("expression_precheck", {})
    if public_persona.get("requires_useful_first") and not checks.get("useful_first"):
        reasons.append("rule_registry:useful_first_required")
    if public_persona.get("requires_persona_hash") and not checks.get("persona_hash"):
        reasons.append("rule_registry:persona_hash_required")
    if public_persona.get("private_expression_visible") is False and expression.get("private_expression_visible"):
        reasons.append("rule_registry:private_expression_visible")

    publisher_boundary = rule_registry.get("publisher_boundary", {})
    if publisher_boundary.get("operator_may_submit_live"):
        reasons.append("rule_registry:operator_live_submit_not_supported_here")

    return {
        "ok": not reasons,
        "block_reasons": sorted(set(reasons)),
    }


def precheck_draft(
    draft: dict[str, Any] | None,
    event: dict[str, Any],
    decision: dict[str, Any],
    config: dict[str, Any],
    rule_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    block_reasons: list[str] = []
    registry = rule_registry or default_rule_registry()
    if draft is None:
        return {
            "ok": False,
            "block_reasons": ["no_draft_for_skip"],
            "rule_registry": validate_against_rule_registry(draft, event, decision, registry),
            "redacted_preview": "",
            "cybernetic_precheck": cybernetic_precheck_stub(config),
        }

    text = draft.get("text", "")
    source_text = event.get("cast", {}).get("text", "")
    # raw_ref often contains Farcaster cast hashes, which share the same
    # 0x40-hex shape as wallet addresses. Keep wallet/secret scanning on
    # public text only; private-runtime checks below still inspect raw_ref.
    public_combined = "\n".join([text, source_text])
    private_ref_combined = "\n".join([text, source_text, str(event.get("raw_ref") or "")])

    if not text.strip():
        block_reasons.append("draft_empty")
    if text_bytes(text) > int(config.get("limits", {}).get("max_draft_bytes", 1024)):
        block_reasons.append("draft_over_byte_limit")
    if decision.get("action") not in ALLOWED_SOCIAL_ACTIONS:
        block_reasons.append("unsupported_social_action")
    if decision.get("block_reasons"):
        block_reasons.append("decision_has_block_reasons")

    for pattern in SECRET_PATTERNS:
        if pattern.search(public_combined):
            block_reasons.append("secret_or_wallet_pattern_detected")
            break
    for pattern in PRIVATE_MEMORY_PATTERNS:
        if pattern.search(private_ref_combined):
            block_reasons.append("private_or_old_runtime_context_detected")
            break
    for pattern in PRIVATE_EXPRESSION_MARKER_PATTERNS:
        if pattern.search(text):
            block_reasons.append("private_expression_marker_detected")
            break
    for key in ("raw_memory", "full_memory", "private_context"):
        if event.get(key):
            block_reasons.append(f"{key}_not_allowed_in_public_operator")

    expression = draft.get("expression_precheck") or {}
    expected_hash = persona_contract_hash(config)
    if not expression.get("ok"):
        block_reasons.append("expression_precheck_failed")
    if expression.get("private_expression_visible"):
        block_reasons.append("private_expression_visible")
    if draft.get("checks", {}).get("persona_hash") != expected_hash:
        block_reasons.append("persona_hash_mismatch")
    if expression.get("persona_hash") != expected_hash:
        block_reasons.append("expression_persona_hash_mismatch")

    rule_check = validate_against_rule_registry(draft, event, decision, registry)
    block_reasons.extend(rule_check.get("block_reasons", []))

    ok = not block_reasons
    return {
        "ok": ok,
        "block_reasons": sorted(set(block_reasons)),
        "rule_registry": rule_check,
        "redacted_preview": redact_preview(text)[:500],
        "cybernetic_precheck": cybernetic_precheck_stub(config),
    }


def cybernetic_precheck_stub(config: dict[str, Any]) -> dict[str, Any]:
    cyber = config.get("cybernetic_precheck", {})
    if not cyber.get("enabled"):
        return {
            "enabled": False,
            "called": False,
            "mode": "available_but_not_called_by_default",
            "contract": {
                "posts_publicly": False,
                "writes_persistent_memory": False,
                "starts_timer": False,
            },
        }
    return {
        "enabled": True,
        "called": False,
        "mode": "read_only_precheck_adapter_required",
        "wrapper": cyber.get("wrapper"),
        "contract": {
            "posts_publicly": False,
            "writes_persistent_memory": False,
            "starts_timer": False,
        },
    }


def parent_for_action(event: dict[str, Any], action: str) -> str:
    cast = event.get("cast", {})
    if action == "reply":
        return cast.get("hash") or cast.get("parent_hash") or ""
    if action == "quote":
        return cast.get("hash") or ""
    return ""


def build_publish_packet(
    decision: dict[str, Any],
    draft: dict[str, Any] | None,
    event: dict[str, Any],
    precheck: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    if draft is None:
        return None
    action = decision.get("action")
    parent_hash = parent_for_action(event, action)
    block_reasons: list[str] = []
    if not precheck.get("ok"):
        block_reasons.extend(precheck.get("block_reasons", []))
    if action in {"reply", "quote"} and not parent_hash:
        block_reasons.append("parent_cast_hash_required_for_reply_or_quote")

    validated = not block_reasons and bool(decision.get("would_publish_in_autonomous_social"))
    packet_id = stable_id("fpp", decision.get("decision_id"), draft.get("text"), parent_hash)
    return {
        "schema": SCHEMA_PACKET,
        "packet_id": packet_id,
        "decision_id": decision.get("decision_id"),
        "event_id": event.get("event_id"),
        "boundary": "dry_run_no_submit",
        "publisher": {
            "transport": config.get("publisher", {}).get("transport", "x402"),
            "adapter": config.get("publisher", {}).get("adapter", "external_x402_publisher"),
            "call_location": config.get("publisher", {}).get("call_location"),
            "called": False,
        },
        "action_type": action,
        "parent_cast_hash": parent_hash if action == "reply" else "",
        "quote_cast_hash": parent_hash if action == "quote" else "",
        "channel_id": event.get("cast", {}).get("channel_id") or "",
        "text": draft.get("text", ""),
        "byte_count": draft.get("byte_count", 0),
        "redacted_preview": precheck.get("redacted_preview", ""),
        "public_persona": draft.get("public_persona", {}),
        "validated": bool(validated),
        "block_reasons": sorted(set(block_reasons)),
        "signed": False,
        "submitted": False,
        "signer_loaded": False,
        "publisher_enabled": bool(config.get("publisher", {}).get("enabled")),
        "side_effects": {
            "farcaster": "not_submitted",
            "signer": "not_loaded",
            "network": "not_used",
            "state": "queued_packet_only" if validated else "not_queued_for_publish",
        },
    }


def close_attention_record(attention_state: dict[str, Any], attention_id: str, reason: str) -> bool:
    active = attention_state.setdefault("active_topics", {})
    record = active.pop(attention_id, None)
    if not record:
        return False
    record["status"] = "closed"
    record["closed_at"] = utc_now()
    record["close_reason"] = reason
    closed = attention_state.setdefault("closed_topics", [])
    closed.append(record)
    attention_state["closed_topics"] = closed[-200:]
    attention_state["updated_at"] = utc_now()
    return True


def expire_attention_records(attention_state: dict[str, Any], config: dict[str, Any]) -> int:
    if not config.get("attention", {}).get("close_when_expired", True):
        return 0
    expired = [
        attention_id
        for attention_id, record in list(attention_state.get("active_topics", {}).items())
        if is_expired(record.get("watch_until"))
    ]
    for attention_id in expired:
        close_attention_record(attention_state, attention_id, "watch_window_expired")
    return len(expired)


def replace_attention_slot_if_stronger(
    attention_state: dict[str, Any],
    plan: dict[str, Any],
    config: dict[str, Any],
) -> str | None:
    active = attention_state.setdefault("active_topics", {})
    if not active:
        return None
    attention_config = config.get("attention", {})
    required_delta = float(attention_config.get("replace_lowest_slot_when_heat_delta") or 0.28)
    new_heat = float((plan.get("last_observed_snapshot") or {}).get("heat") or 0)
    weakest_id, weakest = min(
        active.items(),
        key=lambda item: float((item[1].get("last_observed_snapshot") or {}).get("heat") or item[1].get("source_heat") or 0),
    )
    weakest_heat = float((weakest.get("last_observed_snapshot") or {}).get("heat") or weakest.get("source_heat") or 0)
    if new_heat < weakest_heat + required_delta:
        return None
    close_attention_record(attention_state, weakest_id, "replaced_by_stronger_signal")
    return weakest_id


def plan_attention_after_decision(
    event: dict[str, Any],
    decision: dict[str, Any],
    context: dict[str, Any],
    config: dict[str, Any],
    packet: dict[str, Any] | None,
) -> dict[str, Any]:
    attention_config = config.get("attention", {})
    action = str(decision.get("action") or "")
    kind = str(context.get("kind") or "")
    if not attention_config.get("enabled", True):
        return {"enabled": False, "should_watch": False, "reason": "attention_disabled"}
    if event.get("source") == "topic_attention":
        return {"enabled": True, "should_watch": False, "reason": "existing_attention_followup"}
    if action not in set(attention_config.get("open_after_actions", [])):
        return {"enabled": True, "should_watch": False, "reason": "action_not_watchable", "action": action}
    if kind not in set(attention_config.get("open_after_kinds", [])):
        return {"enabled": True, "should_watch": False, "reason": "kind_not_watchable", "kind": kind}
    if not (packet and packet.get("validated")):
        return {"enabled": True, "should_watch": False, "reason": "packet_not_validated"}

    cast = event.get("cast", {})
    metrics = normalized_metrics_from_payload(event)
    topic = strongest_topic(context)
    duration = float(attention_config.get("watch_duration_hours") or 24)
    if metrics.get("heat", 0) >= float(attention_config.get("major_heat_threshold") or 0.82):
        duration = float(attention_config.get("major_watch_duration_hours") or 48)
    attention_id = stable_id("attn", topic, context.get("thread_key"), cast.get("hash"), action)
    return {
        "enabled": True,
        "should_watch": True,
        "attention_id": attention_id,
        "status": "watching",
        "topic": topic,
        "watch_until": iso_after_hours(duration),
        "next_observe_after": iso_after_hours(1),
        "watch_duration_hours": duration,
        "source_event_id": event.get("event_id"),
        "source_action": action,
        "source_intent": decision.get("operator_intent"),
        "source_cast_hash": cast.get("hash") or "",
        "source_thread_key": context.get("thread_key"),
        "source_channel_id": cast.get("channel_id") or "",
        "source_text_excerpt": first_sentence(cast.get("text", ""), 180),
        "last_observed_snapshot": metrics,
        "followup_limit": int(attention_config.get("followup_limit_per_topic") or 2),
        "llm_call_policy": attention_config.get("llm_call_policy", "only_material_change_creates_followup_event"),
        "side_effects": {"state": "not_written"},
    }


def persist_attention_after_decision(
    state_root: Path,
    event: dict[str, Any],
    decision: dict[str, Any],
    context: dict[str, Any],
    config: dict[str, Any],
    packet: dict[str, Any] | None,
) -> dict[str, Any]:
    plan = plan_attention_after_decision(event, decision, context, config, packet)
    if not plan.get("should_watch"):
        return plan
    attention_state = read_json(state_path(state_root, "topic_attention"), default_topic_attention())
    attention_config = config.get("attention", {})
    expired_count = expire_attention_records(attention_state, config)
    active = attention_state.setdefault("active_topics", {})
    attention_id = str(plan["attention_id"])
    if attention_id not in active and len(active) >= int(attention_config.get("max_active_topics") or 5):
        replaced_id = replace_attention_slot_if_stronger(attention_state, plan, config)
        active = attention_state.setdefault("active_topics", {})
        if not replaced_id and len(active) >= int(attention_config.get("max_active_topics") or 5):
            return {
                **plan,
                "should_watch": False,
                "opened": False,
                "reason": "max_active_attention_topics_reached",
                "expired_count": expired_count,
                "side_effects": {"state": "not_written_limit_reached"},
            }
    else:
        replaced_id = None

    record = active.setdefault(
        attention_id,
        {
            "attention_id": attention_id,
            "opened_at": utc_now(),
            "followup_count": 0,
            "no_material_change_count": 0,
            "observations": [],
            "trigger_history": [],
        },
    )
    record.update({
        key: value
        for key, value in plan.items()
        if key not in {"enabled", "should_watch", "side_effects"}
    })
    record["status"] = "watching"
    record["updated_at"] = utc_now()
    record["last_public_action_at"] = utc_now()
    record["last_public_action_type"] = decision.get("action")
    record["last_public_packet_id"] = packet.get("packet_id") if packet else None
    active[attention_id] = record
    attention_state["updated_at"] = utc_now()
    write_json(state_path(state_root, "topic_attention"), attention_state)
    return {
        **plan,
        "opened": True,
        "active_count": len(active),
        "expired_count": expired_count,
        "replaced_attention_id": replaced_id,
        "side_effects": {"state": "written"},
    }


def attention_update_key(update: dict[str, Any]) -> dict[str, Any]:
    event = normalize_event(update)
    cast = event.get("cast", {})
    tags = [str(tag) for tag in event.get("topic_tags", [])]
    matched = topic_matches(cast.get("text", "") + " " + " ".join(tags), tags)
    topic = str(update.get("topic") or (sorted(matched.items(), key=lambda item: item[1], reverse=True)[0][0] if matched else ""))
    return {
        "attention_id": update.get("attention_id"),
        "cast_hash": cast.get("hash") or update.get("cast_hash") or update.get("source_cast_hash"),
        "thread_key": cast.get("root_hash") or cast.get("parent_hash") or update.get("thread_key"),
        "topic": topic,
        "channel_id": cast.get("channel_id") or update.get("channel_id") or "",
        "event": event,
    }


def match_attention_record(attention_state: dict[str, Any], update: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    key = attention_update_key(update)
    active = attention_state.get("active_topics", {})
    if key.get("attention_id") and key["attention_id"] in active:
        return str(key["attention_id"]), active[str(key["attention_id"])]
    for attention_id, record in active.items():
        if key.get("cast_hash") and key.get("cast_hash") == record.get("source_cast_hash"):
            return attention_id, record
        if key.get("thread_key") and key.get("thread_key") == record.get("source_thread_key"):
            return attention_id, record
    if key.get("topic"):
        for attention_id, record in active.items():
            if key.get("topic") == record.get("topic"):
                return attention_id, record
    return None, None


def material_attention_reasons(
    record: dict[str, Any],
    update: dict[str, Any],
    config: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    attention_config = config.get("attention", {})
    event = normalize_event(update)
    cast = event.get("cast", {})
    metrics = normalized_metrics_from_payload(update)
    last = record.get("last_observed_snapshot") or {}
    replies_delta = int(metrics.get("replies", 0)) - int(last.get("replies", 0) or 0)
    heat_delta = float(metrics.get("heat", 0)) - float(last.get("heat", 0) or 0)
    recasts_delta = int(metrics.get("recasts", 0)) - int(last.get("recasts", 0) or 0)
    reasons: list[str] = []
    if cast.get("mentions_misa") or cast.get("replies_to_misa") or event.get("event_type") in {"mention", "reply"}:
        reasons.append("direct_interaction")
    if replies_delta >= int(attention_config.get("min_new_replies_for_followup") or 2):
        reasons.append("new_replies_delta")
    if heat_delta >= float(attention_config.get("min_heat_delta_for_followup") or 0.22):
        reasons.append("heat_delta")
    if recasts_delta >= 2:
        reasons.append("recast_spread")
    if update.get("new_information") or update.get("material_update"):
        reasons.append("new_information")
    channel_id = cast.get("channel_id") or update.get("channel_id") or ""
    if channel_id and record.get("source_channel_id") and channel_id != record.get("source_channel_id"):
        reasons.append("cross_channel_spread")

    signal_hash = stable_hash({
        "attention_id": record.get("attention_id"),
        "cast_hash": cast.get("hash") or update.get("cast_hash"),
        "metrics": metrics,
        "text": first_sentence(cast.get("text", ""), 160),
        "reasons": reasons,
    })
    if signal_hash == record.get("last_signal_hash"):
        reasons = []

    last_followup = parse_dt(record.get("last_followup_at"))
    cooldown_hours = float(attention_config.get("followup_cooldown_hours") or 6)
    cooldown_active = bool(last_followup and last_followup + timedelta(hours=cooldown_hours) > datetime.now(timezone.utc))
    if cooldown_active and "direct_interaction" not in reasons:
        reasons = []

    if int(record.get("followup_count") or 0) >= int(record.get("followup_limit") or attention_config.get("followup_limit_per_topic") or 2):
        reasons = []

    delta = {
        "metrics": metrics,
        "previous_metrics": last,
        "replies_delta": replies_delta,
        "heat_delta": round(heat_delta, 3),
        "recasts_delta": recasts_delta,
        "signal_hash": signal_hash,
    }
    return reasons, delta


def followup_event_from_attention(record: dict[str, Any], update: dict[str, Any], reasons: list[str], delta: dict[str, Any]) -> dict[str, Any]:
    event = normalize_event(update)
    cast = event.get("cast", {})
    parent_hash = cast.get("hash") or record.get("source_cast_hash") or ""
    topic = record.get("topic") or "farcaster"
    text = cast.get("text") or record.get("source_text_excerpt") or topic
    return {
        "event_id": stable_id("evt_attention", record.get("attention_id"), delta.get("signal_hash")),
        "source": "topic_attention",
        "type": "conversation_update",
        "cast_hash": parent_hash,
        "parent_hash": parent_hash,
        "channel_id": cast.get("channel_id") or record.get("source_channel_id") or "",
        "text": f"Attention follow-up on {topic}: {first_sentence(text, 160)}. New signal: {', '.join(reasons)}.",
        "metrics": delta.get("metrics", {}),
        "topic_tags": [topic],
        "public_memory": [
            f"Misa is watching this topic until {record.get('watch_until')}; follow-up trigger: {', '.join(reasons)}."
        ],
    }


def scan_attention_updates(
    updates: list[dict[str, Any]] | None = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    attention_state = read_json(state_path(root, "topic_attention"), default_topic_attention())
    expired_count = expire_attention_records(attention_state, config)
    followup_events: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []

    for update in list(updates or []):
        attention_id, record = match_attention_record(attention_state, update)
        if not attention_id or record is None:
            unmatched.append({"event_id": update.get("event_id"), "reason": "no_active_attention_match"})
            continue
        reasons, delta = material_attention_reasons(record, update, config)
        observation = {
            "at": utc_now(),
            "attention_id": attention_id,
            "event_id": update.get("event_id"),
            "metrics": delta["metrics"],
            "delta": {
                "replies": delta["replies_delta"],
                "heat": delta["heat_delta"],
                "recasts": delta["recasts_delta"],
            },
            "material_reasons": reasons,
            "followup_event_created": bool(reasons),
        }
        record.setdefault("observations", []).append(observation)
        record["observations"] = record["observations"][-20:]
        record["last_checked_at"] = utc_now()
        record["last_observed_snapshot"] = delta["metrics"]
        record["last_signal_hash"] = delta["signal_hash"]
        if reasons:
            followup_event = followup_event_from_attention(record, update, reasons, delta)
            followup_events.append(followup_event)
            record["followup_count"] = int(record.get("followup_count") or 0) + 1
            record["last_followup_at"] = utc_now()
            record["last_trigger_reason"] = reasons[0]
            record.setdefault("trigger_history", []).append({
                "at": utc_now(),
                "event_id": followup_event["event_id"],
                "reasons": reasons,
            })
            record["trigger_history"] = record["trigger_history"][-20:]
            record["no_material_change_count"] = 0
        else:
            record["no_material_change_count"] = int(record.get("no_material_change_count") or 0) + 1
        attention_state["active_topics"][attention_id] = record
        observations.append(observation)

    if write_state:
        attention_state["updated_at"] = utc_now()
        write_json(state_path(root, "topic_attention"), attention_state)

    return {
        "schema": "misa.hermes.farcaster.attention_scan.result.v1",
        "ok": True,
        "created_at": utc_now(),
        "state_root": str(root),
        "updates_count": len(list(updates or [])),
        "observations_count": len(observations),
        "followup_count": len(followup_events),
        "expired_count": expired_count,
        "followup_events": followup_events,
        "observations": observations,
        "unmatched": unmatched,
        "active_attention_count": len(attention_state.get("active_topics", {})),
        "side_effects": {
            "state": "written" if write_state else "not_written",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
        },
    }


def transition_record(
    operation_id: str,
    from_state: str,
    to_state: str,
    *,
    event_id: str,
    reason: str,
) -> dict[str, Any]:
    allowed = to_state in STATE_MACHINE.get(from_state, [])
    return {
        "schema": SCHEMA_STATE_TRANSITION,
        "at": utc_now(),
        "operation_id": operation_id,
        "event_id": event_id,
        "from": from_state,
        "to": to_state,
        "allowed": allowed,
        "reason": reason,
    }


def build_operation_transitions(
    operation_id: str,
    event: dict[str, Any],
    decision: dict[str, Any],
    draft: dict[str, Any] | None,
    precheck: dict[str, Any],
    packet: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    event_id = str(event.get("event_id") or "")
    transitions = [
        transition_record(operation_id, "created", "sensed", event_id=event_id, reason="event_normalized"),
        transition_record(operation_id, "sensed", "decided", event_id=event_id, reason=decision.get("operator_intent", "decision_created")),
    ]

    if decision.get("action") == "skip":
        transitions.append(transition_record(operation_id, "decided", "skipped", event_id=event_id, reason="decision_skip"))
        transitions.append(transition_record(operation_id, "skipped", "outcome_recorded", event_id=event_id, reason="skip_recorded"))
        return transitions

    if draft is None:
        transitions.append(transition_record(operation_id, "decided", "skipped", event_id=event_id, reason="no_draft"))
        transitions.append(transition_record(operation_id, "skipped", "outcome_recorded", event_id=event_id, reason="no_draft_recorded"))
        return transitions

    transitions.append(transition_record(operation_id, "decided", "drafted", event_id=event_id, reason="draft_created"))
    transitions.append(transition_record(operation_id, "drafted", "prechecked", event_id=event_id, reason="draft_prechecked"))

    if not precheck.get("ok"):
        transitions.append(transition_record(operation_id, "prechecked", "blocked", event_id=event_id, reason="precheck_blocked"))
        transitions.append(transition_record(operation_id, "blocked", "outcome_recorded", event_id=event_id, reason="blocked_recorded"))
        return transitions

    if packet and packet.get("validated"):
        transitions.append(transition_record(operation_id, "prechecked", "queued_for_publisher", event_id=event_id, reason="packet_validated"))
        transitions.append(
            transition_record(
                operation_id,
                "queued_for_publisher",
                "awaiting_external_publisher",
                event_id=event_id,
                reason="x402_external_boundary",
            )
        )
        transitions.append(
            transition_record(
                operation_id,
                "awaiting_external_publisher",
                "dry_run_complete",
                event_id=event_id,
                reason="local_no_submit",
            )
        )
        transitions.append(transition_record(operation_id, "dry_run_complete", "outcome_recorded", event_id=event_id, reason="dry_run_recorded"))
    else:
        transitions.append(transition_record(operation_id, "prechecked", "blocked", event_id=event_id, reason="packet_not_validated"))
        transitions.append(transition_record(operation_id, "blocked", "outcome_recorded", event_id=event_id, reason="packet_blocked_recorded"))

    return transitions


def update_topic_memory(topic_memory: dict[str, Any], event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    topics = topic_memory.setdefault("topics", {})
    matched = context.get("matched_topics") or {"autonomy": 1}
    for topic, topic_score in matched.items():
        record = topics.setdefault(
            topic,
            {
                "first_seen_at": utc_now(),
                "last_seen_at": utc_now(),
                "score": 0.0,
                "seen_count": 0,
                "reply_count": 0,
                "cast_count": 0,
                "last_event_id": None,
                "notes": [],
            },
        )
        record["last_seen_at"] = utc_now()
        record["seen_count"] = int(record.get("seen_count", 0)) + 1
        record["score"] = round(float(record.get("score", 0.0)) + float(topic_score) + float(decision.get("score", 0)), 3)
        record["last_event_id"] = event.get("event_id")
        if decision.get("action") == "reply":
            record["reply_count"] = int(record.get("reply_count", 0)) + 1
        if decision.get("action") in {"cast", "quote"}:
            record["cast_count"] = int(record.get("cast_count", 0)) + 1
    topic_memory["updated_at"] = utc_now()
    return topic_memory


def update_relationship_memory(
    relationship_memory: dict[str, Any],
    event: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    users = relationship_memory.setdefault("users", {})
    key = author_key(event)
    cast = event.get("cast", {})
    record = users.setdefault(
        key,
        {
            "first_seen_at": utc_now(),
            "last_seen_at": utc_now(),
            "username": cast.get("author_username"),
            "interaction_count": 0,
            "reply_count": 0,
            "quality": "unknown",
            "last_event_id": None,
        },
    )
    record["last_seen_at"] = utc_now()
    record["username"] = cast.get("author_username")
    record["interaction_count"] = int(record.get("interaction_count", 0)) + 1
    record["last_event_id"] = event.get("event_id")
    if decision.get("action") == "reply":
        record["reply_count"] = int(record.get("reply_count", 0)) + 1
    if decision.get("action") in ALLOWED_SOCIAL_ACTIONS:
        record["last_public_action_at"] = utc_now()
        record["last_public_action_type"] = decision.get("action")
    if record["interaction_count"] >= 3 and record.get("quality") == "unknown":
        record["quality"] = "familiar"
    relationship_memory["updated_at"] = utc_now()
    return relationship_memory


def increment_counts(operator_state: dict[str, Any], action: str) -> None:
    counts = daily_counts(operator_state)
    if action == "reply":
        counts["reply"] = int(counts.get("reply", 0)) + 1
    elif action in {"cast", "quote"}:
        counts["cast_or_quote"] = int(counts.get("cast_or_quote", 0)) + 1
        if action == "quote":
            counts["quote"] = int(counts.get("quote", 0)) + 1


def already_seen(operator_state: dict[str, Any], operation_id: str) -> bool:
    return operation_id in set(operator_state.get("seen_operation_ids", []))


def remember_operation(operator_state: dict[str, Any], operation_id: str, action: str) -> None:
    seen = list(operator_state.get("seen_operation_ids", []))
    if operation_id not in seen:
        seen.append(operation_id)
    operator_state["seen_operation_ids"] = seen[-10000:]
    operator_state["updated_at"] = utc_now()
    operator_state["last_operation"] = {"operation_id": operation_id, "action": action, "at": utc_now()}
    if action in ALLOWED_SOCIAL_ACTIONS:
        increment_counts(operator_state, action)


def distill_candidate(event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any]) -> dict[str, Any] | None:
    if decision.get("action") == "skip":
        return None
    if decision.get("score", 0) < 0.7 and not context.get("relationship", {}).get("quality") == "high":
        return None
    return {
        "schema": "misa.hermes.farcaster.distill_candidate.v1",
        "created_at": utc_now(),
        "event_id": event.get("event_id"),
        "reason": "high_signal_public_interaction",
        "promotion_target": "main_memory_candidate_only",
        "summary": first_sentence(event.get("cast", {}).get("text", ""), 160),
        "topics": list(context.get("matched_topics", {}).keys()),
    }


def run_event_dry_run(
    payload: dict[str, Any],
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config, operator_state, topic_memory, relationship_memory = load_runtime_state(root)
    rule_registry = normalize_rule_registry(read_json(state_path(root, "rule_registry"), {}))
    if config_override:
        config = merge_dict(config, config_override)

    event = normalize_event(payload)
    context = build_context(event, config, topic_memory, relationship_memory)
    decision = decide(event, config, operator_state, context)
    operation_id = stable_id("fop", event.get("event_id"), decision.get("action"), decision.get("operator_intent"))

    if already_seen(operator_state, operation_id):
        return {
            "schema": SCHEMA_RESULT,
            "ok": True,
            "duplicate": True,
            "operation_id": operation_id,
            "event": event,
            "decision": {
                **decision,
                "action": "skip",
                "reasons": decision.get("reasons", []) + ["duplicate_operation_already_seen"],
                "would_publish_in_autonomous_social": False,
                "allowed_to_publish": False,
            },
            "draft": None,
            "precheck": {
                "ok": False,
                "block_reasons": ["duplicate_operation_already_seen"],
                "redacted_preview": "",
                "cybernetic_precheck": cybernetic_precheck_stub(config),
            },
            "publish_packet": None,
            "side_effects": {
                "farcaster": "not_submitted",
                "publisher": "not_called",
                "network": "not_used",
                "state": "not_written_for_duplicate",
            },
        }

    draft = compose_draft(event, decision, context, config)
    precheck = precheck_draft(draft, event, decision, config, rule_registry)
    packet = build_publish_packet(decision, draft, event, precheck, config)
    attention = plan_attention_after_decision(event, decision, context, config, packet)
    transitions = build_operation_transitions(operation_id, event, decision, draft, precheck, packet)
    result = {
        "schema": SCHEMA_RESULT,
        "ok": True,
        "duplicate": False,
        "operation_id": operation_id,
        "created_at": utc_now(),
        "event": event,
        "context": {
            "kind": context["kind"],
            "language": context["language"],
            "matched_topics": context["matched_topics"],
            "author_key": context["author_key"],
            "thread_key": context["thread_key"],
            "public_persona": {
                "contract_version": context["public_persona"]["version"],
                "persona_hash": context["persona_hash"],
            },
        },
        "decision": decision,
        "draft": draft,
        "precheck": precheck,
        "publish_packet": packet,
        "attention": attention,
        "state_machine": {
            "transitions": transitions,
            "terminal_state": transitions[-1]["to"] if transitions else "unknown",
        },
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
            "state": "written" if write_state else "not_written",
        },
    }

    if write_state:
        append_jsonl(state_path(root, "candidate_queue"), {"at": utc_now(), "operation_id": operation_id, "event": event})
        append_jsonl(state_path(root, "decision_log"), {"at": utc_now(), "operation_id": operation_id, "decision": decision})
        if draft is not None:
            append_jsonl(state_path(root, "draft_log"), {"at": utc_now(), "operation_id": operation_id, "draft": draft})
        if packet is not None and packet.get("validated"):
            append_jsonl(state_path(root, "publish_queue"), {"at": utc_now(), "operation_id": operation_id, "packet": packet})
        for transition in transitions:
            append_jsonl(state_path(root, "state_transitions"), transition)
        append_jsonl(
            state_path(root, "interaction_log"),
            {
                "at": utc_now(),
                "operation_id": operation_id,
                "event_id": event.get("event_id"),
                "author_key": context["author_key"],
                "thread_key": context["thread_key"],
                "action": decision.get("action"),
                "score": decision.get("score"),
            },
        )
        candidate = distill_candidate(event, decision, context)
        if candidate:
            append_jsonl(state_path(root, "distill_candidates"), candidate)

        topic_memory = update_topic_memory(topic_memory, event, decision, context)
        relationship_memory = update_relationship_memory(relationship_memory, event, decision)
        remember_operation(operator_state, operation_id, decision.get("action"))
        write_json(state_path(root, "topic_memory"), topic_memory)
        write_json(state_path(root, "relationship_memory"), relationship_memory)
        write_json(state_path(root, "operator_state"), operator_state)
        result["attention"] = persist_attention_after_decision(root, event, decision, context, config, packet)

    return result


def last_jsonl_record_at(path: Path) -> datetime | None:
    records = read_jsonl(path, limit=1)
    if not records:
        return None
    record = records[-1]
    return parse_dt(record.get("at") or record.get("created_at"))


def due_after(last_at: datetime | None, hours: float) -> bool:
    if not last_at:
        return True
    return last_at + timedelta(hours=hours) <= datetime.now(timezone.utc)


def scheduler_tick_plan(
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    scheduler = config.get("scheduler", {})
    attention_state = read_json(state_path(root, "topic_attention"), default_topic_attention())
    digest_hours = float(config.get("signal_digest", {}).get("fetch_cadence_hours") or 3)
    due_tasks: list[dict[str, Any]] = []

    due_tasks.append({
        "name": "neynar_fetch_plan",
        "due": bool(config.get("neynar_readonly", {}).get("enabled")),
        "reason": "neynar_readonly_enabled" if config.get("neynar_readonly", {}).get("enabled") else "neynar_readonly_disabled",
        "command": ["python", "tools/misa_farcaster_autonomy.py", "--state-root", str(root), "neynar-fetch-plan"],
        "live_effects": False,
    })
    due_tasks.append({
        "name": "signal_digest",
        "due": due_after(last_jsonl_record_at(state_path(root, "signal_digest_log")), digest_hours),
        "reason": f"cadence_hours:{digest_hours}",
        "command": ["python", "tools/misa_farcaster_autonomy.py", "--state-root", str(root), "signal-digest", "--events-file", "<readonly-events.json>"],
        "live_effects": False,
    })
    due_tasks.append({
        "name": "attention_scan",
        "due": bool(attention_state.get("active_topics")),
        "reason": "active_attention_topics" if attention_state.get("active_topics") else "no_active_attention_topics",
        "command": ["python", "tools/misa_farcaster_autonomy.py", "--state-root", str(root), "attention-scan", "--updates-file", "<readonly-updates.json>"],
        "live_effects": False,
    })
    due_tasks.append({
        "name": "run_cycle",
        "due": bool(config.get("scheduled_scan", {}).get("enabled")),
        "reason": "scheduled_scan_enabled",
        "command": ["python", "tools/misa_farcaster_autonomy.py", "--state-root", str(root), "run-cycle", "--events-file", "<operator-events.json>"],
        "live_effects": False,
    })

    plan = {
        "schema": SCHEMA_SCHEDULER_TICK,
        "ok": True,
        "tick_id": stable_id("scht", today_key(), [task["name"] for task in due_tasks]),
        "created_at": utc_now(),
        "enabled": bool(scheduler.get("enabled")),
        "external_only": bool(scheduler.get("external_only", True)),
        "creates_cron": False,
        "tick_interval_minutes": int(scheduler.get("tick_interval_minutes") or 30),
        "run_order": scheduler.get("run_order", []),
        "due_tasks": due_tasks,
        "side_effects": {
            "cron": "not_created",
            "network": "not_used",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "scheduler_tick_log"), {
            "at": utc_now(),
            "tick_id": plan["tick_id"],
            "enabled": plan["enabled"],
            "due": [task["name"] for task in due_tasks if task.get("due")],
            "creates_cron": False,
        })
    return plan


def approved_send_audits_today(state_root: Path) -> dict[str, int]:
    counts = {"cast_or_quote": 0, "quote": 0, "reply": 0}
    for record in read_jsonl(state_path(state_root, "send_audit_log")):
        if not str(record.get("at") or "").startswith(today_key()):
            continue
        decision = record.get("decision") or {}
        if not decision.get("approved_for_external_publisher"):
            continue
        action = record.get("packet", {}).get("action_type")
        if action == "reply":
            counts["reply"] += 1
        elif action in {"cast", "quote"}:
            counts["cast_or_quote"] += 1
            if action == "quote":
                counts["quote"] += 1
    return counts


def live_authorization_ok(approval: dict[str, Any] | None) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    approval = approval or {}
    if not approval.get("authorized"):
        reasons.append("live_authorization_required")
    if not approval.get("approved_by"):
        reasons.append("approval_actor_required")
    expires_at = parse_dt(approval.get("expires_at"))
    if approval.get("expires_at") and (not expires_at or expires_at <= datetime.now(timezone.utc)):
        reasons.append("approval_expired")
    return not reasons, reasons


def send_audit_packet(
    packet: dict[str, Any] | None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    approval: dict[str, Any] | None = None,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    publisher = config.get("publisher", {})
    limits = config.get("limits", {})
    block_reasons: list[str] = []
    action = str((packet or {}).get("action_type") or "")

    if not packet:
        block_reasons.append("missing_packet")
    elif not packet.get("validated"):
        block_reasons.append("packet_not_validated")
    if action not in set(publisher.get("allowed_actions") or ALLOWED_SOCIAL_ACTIONS):
        block_reasons.append("action_not_allowed_for_publisher")
    if not publisher.get("enabled"):
        block_reasons.append("publisher_disabled")
    auth_ok, auth_reasons = live_authorization_ok(approval)
    if not auth_ok:
        block_reasons.extend(auth_reasons)
    if packet and packet.get("submitted"):
        block_reasons.append("packet_already_submitted")
    if packet and packet.get("signer_loaded"):
        block_reasons.append("operator_loaded_signer_unexpectedly")
    if packet and packet.get("publisher", {}).get("called"):
        block_reasons.append("operator_already_called_publisher")
    if packet and any(pattern.search(packet.get("text", "")) for pattern in SECRET_PATTERNS):
        block_reasons.append("secret_or_wallet_pattern_detected")

    counts = approved_send_audits_today(root)
    if action == "reply" and counts["reply"] >= int(limits.get("daily_reply") or 80):
        block_reasons.append("send_audit_daily_reply_limit_reached")
    if action == "quote" and counts["quote"] >= int(limits.get("daily_quote") or 1):
        block_reasons.append("send_audit_daily_quote_limit_reached")
    if action in {"cast", "quote"} and counts["cast_or_quote"] >= int(limits.get("daily_cast_or_quote") or 6):
        block_reasons.append("send_audit_daily_cast_or_quote_limit_reached")

    approved = not block_reasons
    rollback = {
        "required": True,
        "strategy": config.get("rollback", {}).get("strategy", "disable_publisher_and_hold_external_queue"),
        "steps": [
            "pause external scheduler",
            "disable publisher adapter",
            "hold unsubmitted packets in publish-queue.jsonl",
            "record failed publish result without retry storm",
        ],
        "state_paths": config.get("rollback", {}).get("state_paths", []),
    }
    result = {
        "schema": SCHEMA_SEND_AUDIT,
        "ok": True,
        "audit_id": stable_id("sendaudit", (packet or {}).get("packet_id"), today_key(), len(read_jsonl(state_path(root, "send_audit_log")))),
        "created_at": utc_now(),
        "packet_id": (packet or {}).get("packet_id"),
        "packet": {
            "action_type": action,
            "byte_count": (packet or {}).get("byte_count"),
            "channel_id": (packet or {}).get("channel_id"),
            "parent_cast_hash": (packet or {}).get("parent_cast_hash"),
            "quote_cast_hash": (packet or {}).get("quote_cast_hash"),
            "redacted_preview": (packet or {}).get("redacted_preview", ""),
        },
        "decision": {
            "approved_for_external_publisher": approved,
            "block_reasons": sorted(set(block_reasons)),
            "operator_may_submit_live": False,
            "external_submitter_required": True,
            "publisher_transport": publisher.get("transport", "x402"),
        },
        "limits": {
            "approved_today": counts,
            "daily_cast_or_quote": int(limits.get("daily_cast_or_quote") or 6),
            "daily_quote": int(limits.get("daily_quote") or 1),
            "daily_reply": int(limits.get("daily_reply") or 80),
        },
        "rollback_plan": rollback,
        "side_effects": {
            "publisher": "not_called",
            "farcaster": "not_submitted",
            "network": "not_used",
            "secrets": "not_loaded_or_written",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "send_audit_log"), {
            "at": utc_now(),
            "audit_id": result["audit_id"],
            "packet_id": result["packet_id"],
            "packet": result["packet"],
            "decision": result["decision"],
            "rollback_plan": rollback,
        })
    return result


def dry_run_guarded_config(
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    root = Path(state_root)
    init_state(state_root=root)
    config = normalize_config(read_json(state_path(root, "config"), {}))
    if config_override:
        config = merge_dict(config, config_override)
    blocked: list[str] = []

    if config.get("publisher", {}).get("enabled"):
        blocked.append("publisher.enabled")
    config.setdefault("publisher", {})["enabled"] = False

    neynar = config.setdefault("neynar_readonly", {})
    if neynar.get("load_api_key"):
        blocked.append("neynar_readonly.load_api_key")
    if neynar.get("write_api_key"):
        blocked.append("neynar_readonly.write_api_key")
    if neynar.get("network_policy") not in {"plan_only_until_authorized", "fixture_only", "dry_run_no_network"}:
        blocked.append("neynar_readonly.network_policy")
    neynar["load_api_key"] = False
    neynar["write_api_key"] = False
    neynar["network_policy"] = "dry_run_no_network"
    neynar.setdefault("controlled_fetcher", {})["live_fetch_enabled"] = False

    scheduler = config.setdefault("scheduler", {})
    if scheduler.get("creates_cron"):
        blocked.append("scheduler.creates_cron")
    if scheduler.get("live_effects_allowed"):
        blocked.append("scheduler.live_effects_allowed")
    scheduler["creates_cron"] = False
    scheduler["live_effects_allowed"] = False
    scheduler["external_only"] = True

    webhook = config.setdefault("webhook_reply", {})
    webhook["signature_required_before_live"] = True
    webhook["normal_events_enter_same_operator_path"] = True

    ai_second_pass = config.setdefault("ai_second_pass", {})
    ai_second_pass["provider_call"] = "external_or_separate_worker_only"
    ai_second_pass["llm_call_policy"] = "after_script_filter_only"
    ai_second_pass.setdefault("provider_adapter", {})
    ai_second_pass["provider_adapter"]["mode"] = "local_dry_run_only"
    ai_second_pass["provider_adapter"]["provider_call"] = "not_called"
    ai_second_pass["provider_adapter"]["network_allowed"] = False
    ai_second_pass["provider_adapter"]["secrets_allowed"] = False

    config.setdefault("automation_dry_run", {})
    config["automation_dry_run"].update({
        "enabled": True,
        "force_publisher_disabled": True,
        "force_keyless_neynar": True,
        "force_no_network": True,
        "force_no_vps": True,
    })
    return config, sorted(set(blocked))


def run_dry_run_automation_cycle(
    *,
    events: list[dict[str, Any]] | None = None,
    neynar_payloads: Any = None,
    webhook_payloads: Any = None,
    provider_payload: dict[str, Any] | list[Any] | None = None,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    config, blocked_overrides = dry_run_guarded_config(state_root=root, config_override=config_override)
    scheduler = scheduler_tick_plan(state_root=root, config_override=config, write_state=write_state)
    fetcher = run_neynar_readonly_fetcher_dry_run(
        neynar_payloads,
        state_root=root,
        config_override=config,
        write_state=write_state,
    )

    webhook_ingests: list[dict[str, Any]] = []
    webhook_events: list[dict[str, Any]] = []
    for payload in payload_batch(webhook_payloads):
        ingest = ingest_webhook_payload(
            payload,
            state_root=root,
            config_override=config,
            run_operator=False,
            write_state=write_state,
        )
        webhook_ingests.append(ingest)
        webhook_events.append(ingest["event"])

    direct_events = list(events or [])
    all_events = list(fetcher.get("events") or []) + webhook_events + direct_events
    digest = build_signal_digest(all_events, state_root=root, config_override=config, write_state=write_state)
    review_packet = build_ai_second_pass_review_packet(
        digest,
        state_root=root,
        config_override=config,
        write_state=write_state,
    )
    ai_adapter = ai_second_pass_provider_adapter_dry_run(
        review_packet,
        provider_payload,
        state_root=root,
        config_override=config,
        write_state=write_state,
    )
    final_event_ids = {
        str(item.get("event_id"))
        for item in ai_adapter.get("applied_result", {}).get("final_candidates", [])
        if item.get("event_id")
    }
    cycle_events = [event for event in all_events if str(event.get("event_id")) in final_event_ids]
    cycle = run_cycle(cycle_events, state_root=root, config_override=config, write_state=write_state) if cycle_events else {
        "schema": "misa.hermes.farcaster.operator_cycle.result.v1",
        "ok": True,
        "created_at": utc_now(),
        "state_root": str(root),
        "evaluated_count": 0,
        "selected_count": 0,
        "skipped_count": 0,
        "presence_budget": {"enabled": True, "soft_floor_event_added": False},
        "operator_quality": {"schema": SCHEMA_OPERATOR_QUALITY, "scope": "cycle", "side_effects": {"farcaster": "not_submitted"}},
        "selected": [],
        "skipped": [],
        "results": [],
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
            "state": "written" if write_state else "not_written",
        },
    }
    send_audits = [
        send_audit_packet(
            result.get("publish_packet"),
            state_root=root,
            approval=None,
            config_override=config,
            write_state=write_state,
        )
        for result in cycle.get("results", [])
        if result.get("publish_packet")
    ]

    result = {
        "schema": SCHEMA_DRY_RUN_AUTOMATION_CYCLE,
        "ok": bool(fetcher.get("ok")) and bool(digest.get("ok")) and bool(review_packet.get("ok")) and bool(ai_adapter.get("ok")) and bool(cycle.get("ok")),
        "cycle_id": stable_id("facdry", [event.get("event_id") for event in all_events], [audit.get("audit_id") for audit in send_audits]),
        "created_at": utc_now(),
        "blocked_live_overrides": blocked_overrides,
        "scheduler_tick": scheduler,
        "neynar_fetcher": fetcher,
        "webhook_ingests": webhook_ingests,
        "direct_event_count": len(direct_events),
        "combined_event_count": len(all_events),
        "signal_digest": digest,
        "ai_review_packet": review_packet,
        "ai_provider_adapter": ai_adapter,
        "cycle_input_event_ids": [event.get("event_id") for event in cycle_events],
        "run_cycle": cycle,
        "send_audits": send_audits,
        "pre_publish_closure": {
            "publish_packets_seen": len(send_audits),
            "approved_for_external_publisher": sum(1 for audit in send_audits if audit.get("decision", {}).get("approved_for_external_publisher")),
            "blocked_before_external_publisher": sum(1 for audit in send_audits if not audit.get("decision", {}).get("approved_for_external_publisher")),
            "rollback_required": any(audit.get("rollback_plan", {}).get("required") for audit in send_audits),
        },
        "side_effects": {
            "network": "not_used",
            "llm": "not_called",
            "secrets": "not_loaded_or_written",
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "vps": "not_touched",
            "cron": "not_created",
            "state": "written" if write_state else "not_written",
        },
    }
    if write_state:
        append_jsonl(state_path(root, "automation_cycle_log"), {
            "at": utc_now(),
            "cycle_id": result["cycle_id"],
            "ok": result["ok"],
            "combined_event_count": result["combined_event_count"],
            "cycle_selected_count": cycle.get("selected_count"),
            "send_audit_count": len(send_audits),
            "blocked_live_overrides": blocked_overrides,
        })
    return result


def merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(base, ensure_ascii=False))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def record_outcome(
    outcome_payload: dict[str, Any],
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    packet = outcome_payload.get("packet") or outcome_payload.get("publish_packet") or {}
    metrics = outcome_payload.get("metrics") or {}
    action = packet.get("action_type") or outcome_payload.get("action") or "unknown"
    likes = int(metrics.get("likes") or metrics.get("like_count") or 0)
    replies = int(metrics.get("replies") or metrics.get("reply_count") or 0)
    recasts = int(metrics.get("recasts") or metrics.get("recast_count") or 0)
    score = likes * 1 + replies * 3 + recasts * 2
    record = {
        "schema": SCHEMA_OUTCOME,
        "recorded_at": utc_now(),
        "packet_id": packet.get("packet_id"),
        "decision_id": packet.get("decision_id"),
        "event_id": packet.get("event_id") or outcome_payload.get("event_id"),
        "action": action,
        "public_persona": packet.get("public_persona", {}),
        "metrics": {
            "likes": likes,
            "replies": replies,
            "recasts": recasts,
            "score": score,
        },
        "learning": outcome_payload.get("learning") or outcome_learning(score, action),
    }
    append_jsonl(state_path(root, "outcomes"), record)

    topic_memory = read_json(state_path(root, "topic_memory"), default_topic_memory())
    topic_memory.setdefault("proven_angles", [])
    topic_memory.setdefault("failed_angles", [])
    if score >= 3:
        topic_memory["proven_angles"].append({"at": utc_now(), "packet_id": packet.get("packet_id"), "action": action, "score": score})
    elif action in ALLOWED_SOCIAL_ACTIONS:
        topic_memory["failed_angles"].append({"at": utc_now(), "packet_id": packet.get("packet_id"), "action": action, "score": score})
    topic_memory["proven_angles"] = topic_memory["proven_angles"][-200:]
    topic_memory["failed_angles"] = topic_memory["failed_angles"][-200:]
    topic_memory["updated_at"] = utc_now()
    write_json(state_path(root, "topic_memory"), topic_memory)

    return {
        "ok": True,
        "schema": "misa.hermes.farcaster.record_outcome.result.v1",
        "outcome": record,
        "side_effects": {
            "state": "written",
            "farcaster": "not_submitted",
            "network": "not_used",
        },
    }


def outcome_learning(score: int, action: str) -> str:
    if score >= 6:
        return f"{action} created strong interaction; prefer similar angle again"
    if score >= 3:
        return f"{action} produced useful signal; keep but sharpen hook"
    return f"{action} was quiet; store as weak pattern until repeated"


def scoped_records(records: list[dict[str, Any]], date_key: str) -> list[dict[str, Any]]:
    scoped: list[dict[str, Any]] = []
    for record in records:
        stamp = str(record.get("at") or record.get("recorded_at") or record.get("created_at") or "")
        if stamp.startswith(date_key):
            scoped.append(record)
    return scoped


def count_values(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def top_repeated(counts: dict[str, int], *, limit: int = 3) -> list[dict[str, Any]]:
    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
        if count > 1
    ]


def repeat_pressure_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    authors = count_values([str(record.get("author_key") or "") for record in records])
    threads = count_values([str(record.get("thread_key") or "") for record in records])
    actions = count_values([str(record.get("action") or "") for record in records])
    topics: list[str] = []
    for record in records:
        topics.extend([str(topic) for topic in record.get("topics", [])])
    topic_counts = count_values(topics)
    total = len(records)
    same_author_max = max(authors.values(), default=0)
    same_thread_max = max(threads.values(), default=0)
    same_topic_max = max(topic_counts.values(), default=0)
    single_action_max = max(actions.values(), default=0)
    action_ratio = round(single_action_max / total, 3) if total else 0.0
    level = "low"
    if total >= 4 and (same_author_max >= 3 or same_thread_max >= 3 or same_topic_max >= 4 or action_ratio >= 0.85):
        level = "high"
    elif total >= 2 and (same_author_max >= 2 or same_thread_max >= 2 or same_topic_max >= 2):
        level = "medium"
    return {
        "level": level,
        "sample_size": total,
        "same_author_max": same_author_max,
        "same_thread_max": same_thread_max,
        "same_topic_max": same_topic_max,
        "single_action_ratio": action_ratio,
        "repeated_authors": top_repeated(authors),
        "repeated_threads": top_repeated(threads),
        "repeated_topics": top_repeated(topic_counts),
        "action_mix": actions,
    }


def stale_topic_summary(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    stale_decisions = []
    old_signal_count = 0
    max_age = 0.0
    for decision in decisions:
        reasons = [str(reason) for reason in decision.get("reasons", [])]
        recency = decision.get("recency") or {}
        age = recency.get("age_hours")
        if isinstance(age, (int, float)):
            max_age = max(max_age, float(age))
            if float(age) > float((decision.get("presence_budget") or {}).get("max_hot_topic_age_hours") or 24):
                old_signal_count += 1
        if any(reason.startswith("recency_guard:stale_hot_topic") for reason in reasons):
            stale_decisions.append(decision)
    count = len(stale_decisions)
    level = "high" if count >= 2 else "medium" if count == 1 or old_signal_count else "low"
    return {
        "level": level,
        "stale_hot_topic_count": count,
        "old_signal_count": old_signal_count,
        "max_age_hours": round(max_age, 3) if max_age else None,
        "guard": "observe_stale_hot_topic",
    }


def attention_slot_summary(attention_state: dict[str, Any], date_key: str, cycle_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    active = attention_state.get("active_topics", {}) if isinstance(attention_state, dict) else {}
    closed = attention_state.get("closed_topics", []) if isinstance(attention_state, dict) else []
    policy = attention_state.get("policy", {}) if isinstance(attention_state, dict) else {}
    max_active = int(policy.get("max_active_topics") or 5)
    active_count = len(active)
    replaced_today = sum(
        1
        for record in closed
        if str(record.get("closed_at", "")).startswith(date_key)
        and record.get("close_reason") == "replaced_by_stronger_signal"
    )
    no_material_change_count = sum(int(record.get("no_material_change_count") or 0) for record in active.values())
    followup_count = sum(int(record.get("followup_count") or 0) for record in active.values())
    result_replacements = 0
    result_opened = 0
    result_limit_hits = 0
    for result in cycle_results or []:
        attention = result.get("attention") or {}
        active_count = max(active_count, int(attention.get("active_count") or 0))
        if attention.get("opened"):
            result_opened += 1
        if attention.get("replaced_attention_id"):
            result_replacements += 1
        if attention.get("reason") == "max_active_attention_topics_reached":
            result_limit_hits += 1
    usage_ratio = round(active_count / max_active, 3) if max_active else 0.0
    level = "low"
    if result_limit_hits or (active_count >= max_active and max_active > 0):
        level = "high"
    elif usage_ratio >= 0.8 or replaced_today or result_replacements:
        level = "medium"
    return {
        "level": level,
        "active_topics": active_count,
        "max_active_topics": max_active,
        "slot_use_ratio": usage_ratio,
        "replaced_today": replaced_today + result_replacements,
        "opened_in_cycle": result_opened,
        "limit_hits_in_cycle": result_limit_hits,
        "no_material_change_count": no_material_change_count,
        "followup_count": followup_count,
    }


def quality_brakes_summary(decisions: list[dict[str, Any]], transitions: list[dict[str, Any]], skipped_count: int = 0) -> dict[str, Any]:
    block_reasons: dict[str, int] = {}
    decision_skip_count = skipped_count
    weak_signal_observe_count = 0
    stale_hot_topic_count = 0
    quote_limit_count = 0
    for decision in decisions:
        if decision.get("action") == "skip":
            decision_skip_count += 1
        for reason in decision.get("block_reasons", []):
            reason_text = str(reason)
            block_reasons[reason_text] = block_reasons.get(reason_text, 0) + 1
            if "quote_limit" in reason_text:
                quote_limit_count += 1
        joined_reasons = " ".join(str(reason) for reason in decision.get("reasons", []))
        if "presence_budget:quality_floor_not_met" in joined_reasons:
            weak_signal_observe_count += 1
        if "recency_guard:stale_hot_topic" in joined_reasons:
            stale_hot_topic_count += 1
    packet_block_count = sum(1 for transition in transitions if transition.get("to") == "blocked")
    level = "low"
    if packet_block_count or quote_limit_count:
        level = "medium"
    if weak_signal_observe_count >= 3 or stale_hot_topic_count >= 2:
        level = "high"
    return {
        "level": level,
        "decision_skip_count": decision_skip_count,
        "packet_block_count": packet_block_count,
        "block_reasons": block_reasons,
        "weak_signal_observe_count": weak_signal_observe_count,
        "stale_hot_topic_count": stale_hot_topic_count,
        "quote_limit_count": quote_limit_count,
    }


def operator_quality_recommendations(
    repeat_pressure: dict[str, Any],
    stale_risk: dict[str, Any],
    attention_pressure: dict[str, Any],
    budget_pressure: dict[str, Any],
    brakes: dict[str, Any],
) -> list[str]:
    recommendations: list[str] = []
    if repeat_pressure["level"] in {"medium", "high"}:
        recommendations.append("lower priority for repeated author/thread/topic before the next cycle")
    if stale_risk["level"] in {"medium", "high"}:
        recommendations.append("keep stale hot topics in observe mode unless there is new material evidence")
    if attention_pressure["level"] in {"medium", "high"}:
        recommendations.append("replace or close weak attention slots before opening more follow-ups")
    if budget_pressure.get("expanded"):
        recommendations.append("budget expansion is acceptable only while high-signal reasons stay explicit")
    if brakes.get("weak_signal_observe_count") or brakes.get("packet_block_count"):
        recommendations.append("quality brakes are active; inspect blocks before loosening thresholds")
    if not recommendations:
        recommendations.append("operator quality looks steady; keep current soft-presence settings")
    return recommendations


def operator_quality_verdict(parts: list[dict[str, Any]]) -> str:
    levels = [part.get("level") for part in parts]
    if "high" in levels:
        return "tighten"
    if "medium" in levels:
        return "watch"
    return "healthy"


def build_operator_quality_from_history(
    *,
    date_key: str,
    interactions: list[dict[str, Any]],
    decision_records: list[dict[str, Any]],
    transitions: list[dict[str, Any]],
    attention_state: dict[str, Any],
) -> dict[str, Any]:
    scoped_interactions = scoped_records(interactions, date_key)
    scoped_decision_records = scoped_records(decision_records, date_key)
    scoped_transitions = scoped_records(transitions, date_key)
    decisions = [record.get("decision", {}) for record in scoped_decision_records]
    repeat_records = [
        {
            "author_key": record.get("author_key"),
            "thread_key": record.get("thread_key"),
            "action": record.get("action"),
            "topics": list((decisions[index].get("topic_scores") or {}).keys()) if index < len(decisions) else [],
        }
        for index, record in enumerate(scoped_interactions)
    ]
    repeat_pressure = repeat_pressure_summary(repeat_records)
    stale_risk = stale_topic_summary(decisions)
    attention_pressure = attention_slot_summary(attention_state, date_key)
    brakes = quality_brakes_summary(decisions, scoped_transitions)
    budget_pressure = {
        "level": "low",
        "expanded": False,
        "reason": "no_cycle_budget_snapshot_in_daily_history",
    }
    recommendations = operator_quality_recommendations(
        repeat_pressure,
        stale_risk,
        attention_pressure,
        budget_pressure,
        brakes,
    )
    return {
        "schema": SCHEMA_OPERATOR_QUALITY,
        "scope": "daily_history",
        "created_at": utc_now(),
        "report_date": date_key,
        "verdict": operator_quality_verdict([repeat_pressure, stale_risk, attention_pressure, brakes]),
        "repeat_pressure": repeat_pressure,
        "stale_topic_risk": stale_risk,
        "attention_slot_pressure": attention_pressure,
        "budget_pressure": budget_pressure,
        "quality_brakes": brakes,
        "recommendations": recommendations,
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
        },
    }


def build_operator_quality_from_cycle(
    *,
    evaluated: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    results: list[dict[str, Any]],
    presence_summary: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    records = [
        {
            "author_key": item.get("context", {}).get("author_key"),
            "thread_key": item.get("context", {}).get("thread_key"),
            "action": item.get("decision", {}).get("action"),
            "topics": list((item.get("context", {}).get("matched_topics") or {}).keys()),
        }
        for item in evaluated
    ]
    decisions = [item.get("decision", {}) for item in evaluated]
    repeat_pressure = repeat_pressure_summary(records)
    stale_risk = stale_topic_summary(decisions)
    attention_state = {
        "policy": {"max_active_topics": int(config.get("attention", {}).get("max_active_topics") or 5)},
        "active_topics": {},
        "closed_topics": [],
    }
    attention_pressure = attention_slot_summary(attention_state, today_key(), results)
    skipped_due_budget = sum(1 for item in skipped if item.get("skip_reason") == "cycle_action_budget_used")
    base_actions = int(presence_summary.get("base_actions") or presence_summary.get("max_actions") or 0)
    max_actions = int(presence_summary.get("max_actions") or 0)
    expanded = max_actions > base_actions
    budget_level = "medium" if expanded else "low"
    if skipped_due_budget:
        budget_level = "high"
    budget_pressure = {
        "level": budget_level,
        "expanded": expanded,
        "base_actions": base_actions,
        "max_actions": max_actions,
        "selected_count": len(selected),
        "high_signal_count": int(presence_summary.get("high_signal_count") or 0),
        "skipped_due_budget": skipped_due_budget,
        "expansion_reasons": ["high_signal_count_exceeded_base"] if expanded else [],
    }
    brakes = quality_brakes_summary(decisions, [])
    recommendations = operator_quality_recommendations(
        repeat_pressure,
        stale_risk,
        attention_pressure,
        budget_pressure,
        brakes,
    )
    return {
        "schema": SCHEMA_OPERATOR_QUALITY,
        "scope": "cycle",
        "created_at": utc_now(),
        "verdict": operator_quality_verdict([repeat_pressure, stale_risk, attention_pressure, budget_pressure, brakes]),
        "evaluated_count": len(evaluated),
        "selected_count": len(selected),
        "skipped_count": len(skipped),
        "repeat_pressure": repeat_pressure,
        "stale_topic_risk": stale_risk,
        "attention_slot_pressure": attention_pressure,
        "budget_pressure": budget_pressure,
        "quality_brakes": brakes,
        "recommendations": recommendations,
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
        },
    }


def build_outcome_report(
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    report_date: str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    date_key = report_date or today_key()
    outcomes = read_jsonl(state_path(root, "outcomes"))
    interactions = read_jsonl(state_path(root, "interaction_log"))
    decision_records = read_jsonl(state_path(root, "decision_log"))
    transitions = read_jsonl(state_path(root, "state_transitions"))
    topic_memory = read_json(state_path(root, "topic_memory"), default_topic_memory())
    attention_state = read_json(state_path(root, "topic_attention"), default_topic_attention())

    scoped_outcomes = [
        item for item in outcomes
        if str(item.get("recorded_at", "")).startswith(date_key)
    ]
    if not scoped_outcomes:
        scoped_outcomes = outcomes[-50:]

    action_stats: dict[str, dict[str, Any]] = {}
    persona_stats: dict[str, int] = {}
    for outcome in scoped_outcomes:
        action = str(outcome.get("action") or "unknown")
        stats = action_stats.setdefault(action, {"count": 0, "score": 0, "likes": 0, "replies": 0, "recasts": 0})
        metrics = outcome.get("metrics", {})
        stats["count"] += 1
        stats["score"] += int(metrics.get("score") or 0)
        stats["likes"] += int(metrics.get("likes") or 0)
        stats["replies"] += int(metrics.get("replies") or 0)
        stats["recasts"] += int(metrics.get("recasts") or 0)
        persona_hash = str(outcome.get("public_persona", {}).get("persona_hash") or "")
        if persona_hash:
            persona_stats[persona_hash] = persona_stats.get(persona_hash, 0) + 1

    blocked_count = sum(1 for item in transitions if item.get("to") == "blocked")
    queued_count = sum(1 for item in transitions if item.get("to") == "queued_for_publisher")
    top_topics = sorted(
        topic_memory.get("topics", {}).items(),
        key=lambda item: float(item[1].get("score", 0)),
        reverse=True,
    )[:5]

    recommendations: list[str] = []
    if queued_count:
        recommendations.append("keep x402 packet boundary external; operator should keep producing validated packets")
    if blocked_count:
        recommendations.append("review blocked drafts for rule tuning before enabling live submit")
    if top_topics:
        recommendations.append(f"next proactive topic candidate: {top_topics[0][0]}")
    if not recommendations:
        recommendations.append("collect more outcomes before changing strategy")

    report = {
        "schema": SCHEMA_DAILY_REPORT,
        "report_date": date_key,
        "created_at": utc_now(),
        "state_root": str(root),
        "counts": {
            "outcomes_considered": len(scoped_outcomes),
            "interactions_total": len(interactions),
            "queued_packet_transitions": queued_count,
            "blocked_transitions": blocked_count,
        },
        "action_stats": action_stats,
        "public_persona_stats": persona_stats,
        "top_topics": [
            {
                "topic": topic,
                "score": record.get("score", 0),
                "seen_count": record.get("seen_count", 0),
                "reply_count": record.get("reply_count", 0),
                "cast_count": record.get("cast_count", 0),
            }
            for topic, record in top_topics
        ],
        "proven_angles": topic_memory.get("proven_angles", [])[-5:],
        "failed_angles": topic_memory.get("failed_angles", [])[-5:],
        "operator_quality": build_operator_quality_from_history(
            date_key=date_key,
            interactions=interactions,
            decision_records=decision_records,
            transitions=transitions,
            attention_state=attention_state,
        ),
        "recommendations": recommendations,
        "side_effects": {
            "state": "written" if write_report else "not_written",
            "farcaster": "not_submitted",
            "network": "not_used",
        },
    }
    if write_report:
        append_jsonl(state_path(root, "daily_reports"), report)
    return report


def mcp_tool_manifest() -> dict[str, Any]:
    return {
        "schema": SCHEMA_MCP_MANIFEST,
        "name": "misa-farcaster-autonomous-operator",
        "version": OPERATOR_VERSION,
        "transport": "hermes-native-local-dispatch",
        "tools": [
            {
                "name": "misaFarcasterInitState",
                "description": "Create non-secret Farcaster operator state and rule registry.",
                "cli": "init-state",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterRunEvent",
                "description": "Run one normalized event through sense, think, speak, precheck, packet, and learn.",
                "cli": "run-event",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterRunCycle",
                "description": "Rank and process a batch of events, or choose from operator memory when idle.",
                "cli": "run-cycle",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterBuildSignalDigest",
                "description": "Compress provider or fixture signals into a low-token digest for Misa review.",
                "cli": "signal-digest",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterBuildAiSecondPassPacket",
                "description": "Build a small Attn-style AI review packet after script filtering; does not call an LLM.",
                "cli": "ai-review-packet",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterApplyAiSecondPass",
                "description": "Merge external AI pass/observe/reject decisions back onto reviewed candidates without publishing.",
                "cli": "apply-ai-review",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterAiSecondPassProviderAdapter",
                "description": "Protect the AI second-pass provider boundary with a local dry-run adapter.",
                "cli": "ai-provider-dry-run",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterNeynarFetchPlan",
                "description": "Build Neynar v2 read-only request plan without loading keys or using the network.",
                "cli": "neynar-fetch-plan",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterNeynarFetcherDryRun",
                "description": "Run the controlled Neynar read-only fetcher wrapper against fixtures or a plan only.",
                "cli": "neynar-fetcher-dry-run",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterIngestNeynarPayload",
                "description": "Normalize a Neynar read-only response into public-safe operator events.",
                "cli": "ingest-neynar",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterWebhookIngest",
                "description": "Normalize a Farcaster webhook payload and optionally route it through dry-run operator logic.",
                "cli": "webhook-ingest",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterAttentionScan",
                "description": "Record watched-topic observations and emit follow-up candidates only on material changes.",
                "cli": "attention-scan",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterSchedulerTick",
                "description": "Plan one external scheduler tick without creating cron jobs or timers.",
                "cli": "scheduler-tick",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterRecordOutcome",
                "description": "Record post/reply performance and update operator learning memory.",
                "cli": "record-outcome",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterSendAudit",
                "description": "Audit a validated x402 packet before any external publisher can submit it.",
                "cli": "send-audit",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterDryRunAutomationCycle",
                "description": "Run scheduler, read-only fetcher, webhook ingest, AI second-pass, run-cycle, and send-audit as one local dry-run loop.",
                "cli": "dry-run-cycle",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterOutcomeReport",
                "description": "Summarize outcomes, transitions, top topics, and next operator recommendations.",
                "cli": "outcome-report",
                "live_effects": False,
            },
            {
                "name": "misaFarcasterRulesSummary",
                "description": "Return Farcaster action rules, blocked actions, and publisher boundary.",
                "cli": "rules-summary",
                "live_effects": False,
            },
        ],
        "publisher_boundary": {
            "operator_submits_live": False,
            "x402_packet_only": True,
        },
        "public_persona": {
            "contract_version": default_public_persona_contract()["version"],
            "private_expression_visible": False,
        },
        "attention": {
            "creates_cron_or_timer": False,
            "raw_json_to_misa": False,
            "followup_trigger": "material_change_only",
        },
        "neynar_readonly": {
            "network": "not_used_by_manifest_tools",
            "api_key_loaded": False,
        },
        "ai_second_pass": {
            "llm_called_by_operator": False,
            "raw_json_to_ai": False,
            "review_after_script_filter_only": True,
        },
        "send_audit": {
            "operator_may_submit_live": False,
            "external_submitter_required": True,
        },
        "automation_dry_run": {
            "network": "not_used",
            "llm": "not_called",
            "vps": "not_touched",
        },
        "presence_budget": {
            "cadence_style": "soft_presence_not_hard_quota",
            "quality_first_over_heat": True,
            "quote_live_policy": "dry_run_observe_first",
        },
    }


def mcp_call(tool_name: str, payload: dict[str, Any] | None = None, *, state_root: Path | str = DEFAULT_STATE_ROOT) -> dict[str, Any]:
    payload = payload or {}
    root = Path(state_root)
    if tool_name == "misaFarcasterInitState":
        return init_state(state_root=root)
    if tool_name == "misaFarcasterRunEvent":
        return run_event_dry_run(payload.get("event") or payload, state_root=root, write_state=payload.get("write_state", True))
    if tool_name == "misaFarcasterRunCycle":
        events = payload.get("events") or []
        return run_cycle(events, state_root=root, write_state=payload.get("write_state", True))
    if tool_name == "misaFarcasterBuildSignalDigest":
        signals = payload.get("signals") or payload.get("events") or []
        return build_signal_digest(signals, state_root=root, write_state=payload.get("write_state", True))
    if tool_name == "misaFarcasterBuildAiSecondPassPacket":
        digest = payload.get("digest")
        if not digest:
            signals = payload.get("signals") or payload.get("events") or []
            digest = build_signal_digest(signals, state_root=root, write_state=False)
        return build_ai_second_pass_review_packet(
            digest,
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterApplyAiSecondPass":
        return apply_ai_second_pass_review(
            payload.get("packet") or payload.get("review_packet") or {},
            payload.get("decisions") or payload.get("results") or [],
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterAiSecondPassProviderAdapter":
        return ai_second_pass_provider_adapter_dry_run(
            payload.get("packet") or payload.get("review_packet") or {},
            payload.get("provider_payload") or payload.get("decisions"),
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterNeynarFetchPlan":
        return build_neynar_readonly_fetch_plan(
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterNeynarFetcherDryRun":
        return run_neynar_readonly_fetcher_dry_run(
            payload.get("payloads") or payload.get("fixtures") or payload.get("payload"),
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterIngestNeynarPayload":
        return ingest_neynar_readonly_payload(
            payload.get("payload") or payload.get("response") or payload.get("casts") or payload,
            state_root=root,
            source_hint=payload.get("source_hint", "neynar_readonly"),
            build_digest=payload.get("build_digest", False),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterWebhookIngest":
        return ingest_webhook_payload(
            payload.get("payload") or payload,
            state_root=root,
            config_override=payload.get("config_override"),
            run_operator=payload.get("run_operator"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterAttentionScan":
        updates = payload.get("updates") or payload.get("events") or []
        return scan_attention_updates(updates, state_root=root, write_state=payload.get("write_state", True))
    if tool_name == "misaFarcasterSchedulerTick":
        return scheduler_tick_plan(
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterRecordOutcome":
        return record_outcome(payload, state_root=root)
    if tool_name == "misaFarcasterSendAudit":
        return send_audit_packet(
            payload.get("packet") or payload,
            state_root=root,
            approval=payload.get("approval"),
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterDryRunAutomationCycle":
        return run_dry_run_automation_cycle(
            events=payload.get("events"),
            neynar_payloads=payload.get("neynar_payloads") or payload.get("neynar_fixtures"),
            webhook_payloads=payload.get("webhook_payloads"),
            provider_payload=payload.get("provider_payload") or payload.get("ai_decisions"),
            state_root=root,
            config_override=payload.get("config_override"),
            write_state=payload.get("write_state", True),
        )
    if tool_name == "misaFarcasterOutcomeReport":
        return build_outcome_report(state_root=root, report_date=payload.get("report_date"), write_report=payload.get("write_report", True))
    if tool_name == "misaFarcasterRulesSummary":
        init_state(state_root=root)
        return {
            "ok": True,
            "schema": "misa.hermes.farcaster.rules_summary.result.v1",
            "rules": normalize_rule_registry(read_json(state_path(root, "rule_registry"), {})),
            "public_persona": {
                "contract": load_runtime_state(root)[0]["public_persona_contract"],
                "hash": persona_contract_hash(load_runtime_state(root)[0]),
            },
            "operator_layers": OPERATOR_LAYERS,
            "operator_modes": OPERATOR_MODES,
            "state_machine": STATE_MACHINE,
            "signal_digest": load_runtime_state(root)[0].get("signal_digest", {}),
            "topic_heat": load_runtime_state(root)[0].get("topic_heat", {}),
            "neynar_readonly": load_runtime_state(root)[0].get("neynar_readonly", {}),
            "attention": load_runtime_state(root)[0].get("attention", {}),
            "presence_budget": load_runtime_state(root)[0].get("presence_budget", {}),
            "scheduler": load_runtime_state(root)[0].get("scheduler", {}),
            "send_audit": load_runtime_state(root)[0].get("send_audit", {}),
            "side_effects": {
                "farcaster": "not_submitted",
                "network": "not_used",
            },
        }
    return {
        "ok": False,
        "schema": "misa.hermes.farcaster.mcp_call.error.v1",
        "error": f"unknown MCP tool: {tool_name}",
        "available_tools": [tool["name"] for tool in mcp_tool_manifest()["tools"]],
    }


def operation_priority(event: dict[str, Any], decision: dict[str, Any], context: dict[str, Any]) -> float:
    kind_bonus = {
        "direct_reply": 3.0,
        "thread_join": 2.0,
        "topic_cast": 1.4,
        "memory_cast": 1.1,
        "ambient": 0.2,
    }
    action_bonus = {
        "reply": 0.5,
        "quote": 0.35,
        "cast": 0.25,
        "skip": -1.0,
    }
    quality_bonus = {
        "direct": 0.45,
        "hot_relevant": 0.35,
        "high_quality_author": 0.30,
        "good_thread": 0.25,
        "soft_presence": 0.05,
        "usable": 0.10,
        "observe": 0.0,
    }
    heat = float(event.get("metrics", {}).get("heat") or 0)
    quality = decision.get("social_quality") or {}
    return round(
        kind_bonus.get(context.get("kind"), 0.0)
        + action_bonus.get(decision.get("action"), 0.0)
        + quality_bonus.get(quality.get("band"), 0.0)
        + float(decision.get("score", 0))
        + min(0.5, heat * 0.5),
        4,
    )


def memory_prompt_from_operator_state(topic_memory: dict[str, Any]) -> dict[str, Any] | None:
    topics = topic_memory.get("topics", {})
    if not topics:
        return None
    ranked = sorted(
        topics.items(),
        key=lambda item: (
            float(item[1].get("score", 0)),
            int(item[1].get("seen_count", 0)),
        ),
        reverse=True,
    )
    topic, record = ranked[0]
    event_id = stable_id("evt_memory", today_key(), topic, record.get("last_event_id"))
    return {
        "event_id": event_id,
        "source": "operator_memory",
        "event_type": "memory_prompt",
        "text": f"Misa should post a useful public thought about {topic} based on recent operator memory.",
        "channel_id": "openclaw",
        "topic_tags": [topic],
        "metrics": {"heat": min(1.0, float(record.get("score", 0)) / 10.0)},
        "public_memory": [
            f"{topic} has appeared {record.get('seen_count', 0)} time(s); last event {record.get('last_event_id') or 'unknown'}."
        ],
    }


def evaluate_payload_for_cycle(
    payload: dict[str, Any],
    *,
    state_root: Path,
    config: dict[str, Any],
    operator_state: dict[str, Any],
    topic_memory: dict[str, Any],
    relationship_memory: dict[str, Any],
) -> dict[str, Any]:
    event = normalize_event(payload)
    context = build_context(event, config, topic_memory, relationship_memory)
    decision = decide(event, config, operator_state, context)
    operation_id = stable_id("fop", event.get("event_id"), decision.get("action"), decision.get("operator_intent"))
    return {
        "operation_id": operation_id,
        "event": event,
        "context": {
            "kind": context["kind"],
            "language": context["language"],
            "matched_topics": context["matched_topics"],
            "author_key": context["author_key"],
            "thread_key": context["thread_key"],
        },
        "decision": decision,
        "priority": operation_priority(event, decision, context),
        "duplicate": already_seen(operator_state, operation_id),
        "state_root": str(state_root),
    }


def presence_floor_event(
    evaluated: list[dict[str, Any]],
    operator_state: dict[str, Any],
    topic_memory: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    presence = config.get("presence_budget", {})
    if not presence.get("enabled", True):
        return None
    if presence.get("soft_floor_source") != "operator_memory":
        return None
    counts = daily_counts(operator_state)
    if int(counts.get("cast_or_quote", 0)) >= int(presence.get("daily_min_cast") or 1):
        return None
    has_publishable_cast = any(
        not item.get("duplicate")
        and item.get("decision", {}).get("action") in {"cast", "quote"}
        and not item.get("decision", {}).get("block_reasons")
        for item in evaluated
    )
    if has_publishable_cast:
        return None

    memory_event = memory_prompt_from_operator_state(topic_memory)
    if not memory_event:
        return None
    seen_event_ids = {item.get("event", {}).get("event_id") for item in evaluated}
    if memory_event.get("event_id") in seen_event_ids:
        return None
    memory_event["public_memory"] = list(memory_event.get("public_memory") or []) + [
        "Soft presence floor: appear only if the memory topic can still be useful."
    ]
    return memory_event


def cycle_action_budget(config: dict[str, Any], evaluated: list[dict[str, Any]]) -> tuple[int, dict[str, Any]]:
    base = int(config.get("scheduled_scan", {}).get("max_write_actions_per_run", 3))
    presence = config.get("presence_budget", {})
    if not presence.get("enabled", True):
        return base, {
            "enabled": False,
            "max_actions": base,
            "reason": "presence_budget_disabled",
        }

    high_signal_count = sum(
        1
        for item in evaluated
        if not item.get("duplicate")
        and item.get("decision", {}).get("action") != "skip"
        and item.get("decision", {}).get("social_quality", {}).get("can_expand_attention_budget")
    )
    max_run = int(presence.get("high_signal_max_actions_per_run") or base)
    extra = int(presence.get("high_signal_extra_actions") or 0)
    max_actions = base
    if high_signal_count > base:
        max_actions = min(max_run, high_signal_count, base + extra)

    return max_actions, {
        "enabled": True,
        "cadence_style": presence.get("cadence_style", "soft_presence_not_hard_quota"),
        "base_actions": base,
        "max_actions": max_actions,
        "high_signal_count": high_signal_count,
        "daily_min_cast": int(presence.get("daily_min_cast") or 1),
        "quality_first_over_heat": bool(presence.get("quality_first_over_heat", True)),
    }


def run_cycle(
    payloads: list[dict[str, Any]] | None = None,
    *,
    state_root: Path | str = DEFAULT_STATE_ROOT,
    config_override: dict[str, Any] | None = None,
    write_state: bool = True,
) -> dict[str, Any]:
    root = Path(state_root)
    init_state(state_root=root)
    config, operator_state, topic_memory, relationship_memory = load_runtime_state(root)
    if config_override:
        config = merge_dict(config, config_override)

    incoming = list(payloads or [])
    if not incoming:
        memory_event = memory_prompt_from_operator_state(topic_memory)
        if memory_event:
            incoming.append(memory_event)

    evaluated = [
        evaluate_payload_for_cycle(
            payload,
            state_root=root,
            config=config,
            operator_state=operator_state,
            topic_memory=topic_memory,
            relationship_memory=relationship_memory,
        )
        for payload in incoming
    ]
    floor_event = presence_floor_event(evaluated, operator_state, topic_memory, config)
    if floor_event:
        evaluated.append(
            evaluate_payload_for_cycle(
                floor_event,
                state_root=root,
                config=config,
                operator_state=operator_state,
                topic_memory=topic_memory,
                relationship_memory=relationship_memory,
            )
        )
    evaluated.sort(key=lambda item: item["priority"], reverse=True)

    max_actions, presence_summary = cycle_action_budget(config, evaluated)
    presence_summary["soft_floor_event_added"] = bool(floor_event)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for item in evaluated:
        if item["duplicate"]:
            skipped.append({**item, "skip_reason": "duplicate"})
            continue
        if item["decision"].get("action") == "skip":
            skipped.append({**item, "skip_reason": "decision_skip"})
            continue
        if len(selected) >= max_actions:
            skipped.append({**item, "skip_reason": "cycle_action_budget_used"})
            continue
        selected.append(item)

    results = [
        run_event_dry_run(item["event"], state_root=root, config_override=config_override, write_state=write_state)
        for item in selected
    ]
    operator_quality = build_operator_quality_from_cycle(
        evaluated=evaluated,
        selected=selected,
        skipped=skipped,
        results=results,
        presence_summary=presence_summary,
        config=config,
    )

    return {
        "schema": "misa.hermes.farcaster.operator_cycle.result.v1",
        "ok": True,
        "created_at": utc_now(),
        "state_root": str(root),
        "evaluated_count": len(evaluated),
        "selected_count": len(selected),
        "skipped_count": len(skipped),
        "presence_budget": presence_summary,
        "operator_quality": operator_quality,
        "selected": [
            {
                "operation_id": item["operation_id"],
                "event_id": item["event"].get("event_id"),
                "action": item["decision"].get("action"),
                "intent": item["decision"].get("operator_intent"),
                "priority": item["priority"],
            }
            for item in selected
        ],
        "skipped": [
            {
                "event_id": item["event"].get("event_id"),
                "action": item["decision"].get("action"),
                "priority": item["priority"],
                "skip_reason": item.get("skip_reason"),
            }
            for item in skipped
        ],
        "results": results,
        "side_effects": {
            "farcaster": "not_submitted",
            "publisher": "not_called",
            "network": "not_used",
            "state": "written" if write_state else "not_written",
        },
    }


def load_events_file(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return payload["events"]
    if isinstance(payload, dict):
        return [payload]
    raise ValueError(f"unsupported events file payload: {type(payload).__name__}")


def demo_event() -> dict[str, Any]:
    return {
        "event_id": "demo_mention_autonomy",
        "source": "demo",
        "type": "mention",
        "author": {"fid": 100, "username": "alice", "score": 0.7},
        "cast_hash": "0x" + "1" * 40,
        "channel_id": "openclaw",
        "text": "Misa, how should an autonomous Farcaster operator choose what to post?",
        "metrics": {"heat": 0.4, "likes": 2, "replies": 1},
        "topic_tags": ["autonomy", "farcaster"],
        "public_memory": ["Recent interactions asked for autonomous operation, not manual review."],
    }


def load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_json_files(paths: list[str] | None) -> list[Any]:
    return [load_json_file(Path(path)) for path in (paths or [])]


def print_json(payload: Any, pretty: bool) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--pretty", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init-state", help="create non-secret operator state skeleton")
    init.add_argument("--overwrite-config", action="store_true")

    sub.add_parser("schema-summary", help="show schemas and state files")
    sub.add_parser("rules-summary", help="show Farcaster operator modes, rules, and state machine")
    sub.add_parser("mcp-manifest", help="show Hermes-native local MCP tool manifest")

    run = sub.add_parser("run-event", help="run one event through the autonomous operator")
    run.add_argument("--event-file", required=True)
    run.add_argument("--no-state-write", action="store_true")

    cycle = sub.add_parser("run-cycle", help="rank and process a batch of events, or use operator memory")
    cycle.add_argument("--events-file")
    cycle.add_argument("--no-state-write", action="store_true")

    neynar_plan = sub.add_parser("neynar-fetch-plan", help="build Neynar read-only request plan without network calls")
    neynar_plan.add_argument("--no-state-write", action="store_true")

    neynar_fetcher = sub.add_parser("neynar-fetcher-dry-run", help="run the controlled Neynar read-only fetcher against fixtures or a plan only")
    neynar_fetcher.add_argument("--payload-file", action="append")
    neynar_fetcher.add_argument("--no-state-write", action="store_true")

    ingest_neynar = sub.add_parser("ingest-neynar", help="normalize a Neynar response fixture into operator events")
    ingest_neynar.add_argument("--payload-file", required=True)
    ingest_neynar.add_argument("--source-hint", default="neynar_readonly")
    ingest_neynar.add_argument("--build-digest", action="store_true")
    ingest_neynar.add_argument("--no-state-write", action="store_true")

    webhook = sub.add_parser("webhook-ingest", help="normalize Farcaster webhook payload and optionally dry-run operator")
    webhook.add_argument("--payload-file", required=True)
    webhook.add_argument("--run-operator", action="store_true")
    webhook.add_argument("--no-state-write", action="store_true")

    digest = sub.add_parser("signal-digest", help="compress provider signals into a low-token digest")
    digest.add_argument("--events-file", required=True)
    digest.add_argument("--no-state-write", action="store_true")

    ai_review = sub.add_parser("ai-review-packet", help="build Attn-style AI second-pass packet without calling an LLM")
    ai_review.add_argument("--events-file")
    ai_review.add_argument("--digest-file")
    ai_review.add_argument("--no-state-write", action="store_true")

    apply_ai = sub.add_parser("apply-ai-review", help="apply external AI pass/observe/reject decisions without publishing")
    apply_ai.add_argument("--packet-file", required=True)
    apply_ai.add_argument("--decisions-file", required=True)
    apply_ai.add_argument("--no-state-write", action="store_true")

    ai_adapter = sub.add_parser("ai-provider-dry-run", help="run the local guarded AI second-pass provider adapter")
    ai_adapter.add_argument("--packet-file", required=True)
    ai_adapter.add_argument("--decisions-file")
    ai_adapter.add_argument("--no-state-write", action="store_true")

    attention = sub.add_parser("attention-scan", help="record watched-topic updates and emit material follow-up events")
    attention.add_argument("--updates-file", required=True)
    attention.add_argument("--no-state-write", action="store_true")

    scheduler = sub.add_parser("scheduler-tick", help="plan one external scheduler tick without creating cron")
    scheduler.add_argument("--no-state-write", action="store_true")

    sub.add_parser("demo", help="run a built-in mention event")

    outcome = sub.add_parser("record-outcome", help="record interaction outcome metrics")
    outcome.add_argument("--outcome-file", required=True)

    report = sub.add_parser("outcome-report", help="summarize outcomes and operator learning")
    report.add_argument("--date")
    report.add_argument("--no-state-write", action="store_true")

    audit = sub.add_parser("send-audit", help="audit a publisher packet before any external x402 submitter")
    audit.add_argument("--packet-file", required=True)
    audit.add_argument("--approval-file")
    audit.add_argument("--no-state-write", action="store_true")

    dry_cycle = sub.add_parser("dry-run-cycle", help="run the full local Farcaster automation loop without live effects")
    dry_cycle.add_argument("--events-file")
    dry_cycle.add_argument("--neynar-payload-file", action="append")
    dry_cycle.add_argument("--webhook-payload-file", action="append")
    dry_cycle.add_argument("--ai-decisions-file")
    dry_cycle.add_argument("--no-state-write", action="store_true")

    mcp = sub.add_parser("mcp-call", help="call one local MCP-style tool by name")
    mcp.add_argument("--tool", required=True)
    mcp.add_argument("--input-file")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    state_root = Path(args.state_root)

    if args.command == "init-state":
        print_json(init_state(args, state_root=state_root), args.pretty)
        return 0
    if args.command == "schema-summary":
        print_json(schema_summary(args), args.pretty)
        return 0
    if args.command == "rules-summary":
        print_json(mcp_call("misaFarcasterRulesSummary", {}, state_root=state_root), args.pretty)
        return 0
    if args.command == "mcp-manifest":
        print_json(mcp_tool_manifest(), args.pretty)
        return 0
    if args.command == "run-event":
        payload = load_json_file(Path(args.event_file))
        result = run_event_dry_run(payload, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "run-cycle":
        payloads = load_events_file(Path(args.events_file)) if args.events_file else []
        result = run_cycle(payloads, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "neynar-fetch-plan":
        result = build_neynar_readonly_fetch_plan(state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "neynar-fetcher-dry-run":
        result = run_neynar_readonly_fetcher_dry_run(
            load_json_files(args.payload_file),
            state_root=state_root,
            write_state=not args.no_state_write,
        )
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "ingest-neynar":
        payload = load_json_file(Path(args.payload_file))
        result = ingest_neynar_readonly_payload(
            payload,
            state_root=state_root,
            source_hint=args.source_hint,
            build_digest=args.build_digest,
            write_state=not args.no_state_write,
        )
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "webhook-ingest":
        payload = load_json_file(Path(args.payload_file))
        result = ingest_webhook_payload(
            payload,
            state_root=state_root,
            run_operator=args.run_operator,
            write_state=not args.no_state_write,
        )
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "signal-digest":
        payloads = load_events_file(Path(args.events_file))
        result = build_signal_digest(payloads, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "ai-review-packet":
        if args.digest_file:
            digest_payload = load_json_file(Path(args.digest_file))
        else:
            payloads = load_events_file(Path(args.events_file)) if args.events_file else []
            digest_payload = build_signal_digest(payloads, state_root=state_root, write_state=False)
        result = build_ai_second_pass_review_packet(digest_payload, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "apply-ai-review":
        packet = load_json_file(Path(args.packet_file))
        decisions = json.loads(Path(args.decisions_file).read_text(encoding="utf-8"))
        result = apply_ai_second_pass_review(packet, decisions, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "ai-provider-dry-run":
        packet = load_json_file(Path(args.packet_file))
        decisions = json.loads(Path(args.decisions_file).read_text(encoding="utf-8")) if args.decisions_file else None
        result = ai_second_pass_provider_adapter_dry_run(
            packet,
            decisions,
            state_root=state_root,
            write_state=not args.no_state_write,
        )
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "attention-scan":
        updates = load_events_file(Path(args.updates_file))
        result = scan_attention_updates(updates, state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "scheduler-tick":
        result = scheduler_tick_plan(state_root=state_root, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "demo":
        result = run_event_dry_run(demo_event(), state_root=state_root)
        print_json(result, args.pretty)
        return 0
    if args.command == "record-outcome":
        payload = load_json_file(Path(args.outcome_file))
        result = record_outcome(payload, state_root=state_root)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "outcome-report":
        result = build_outcome_report(state_root=state_root, report_date=args.date, write_report=not args.no_state_write)
        print_json(result, args.pretty)
        return 0
    if args.command == "send-audit":
        packet = load_json_file(Path(args.packet_file))
        approval = load_json_file(Path(args.approval_file)) if args.approval_file else None
        result = send_audit_packet(packet, state_root=state_root, approval=approval, write_state=not args.no_state_write)
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "dry-run-cycle":
        events = load_events_file(Path(args.events_file)) if args.events_file else []
        provider_payload = json.loads(Path(args.ai_decisions_file).read_text(encoding="utf-8")) if args.ai_decisions_file else None
        result = run_dry_run_automation_cycle(
            events=events,
            neynar_payloads=load_json_files(args.neynar_payload_file),
            webhook_payloads=load_json_files(args.webhook_payload_file),
            provider_payload=provider_payload,
            state_root=state_root,
            write_state=not args.no_state_write,
        )
        print_json(result, args.pretty)
        return 0 if result.get("ok") else 1
    if args.command == "mcp-call":
        payload = load_json_file(Path(args.input_file)) if args.input_file else {}
        result = mcp_call(args.tool, payload, state_root=state_root)
        print_json(result, args.pretty)
        return 0 if result.get("ok", True) else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
