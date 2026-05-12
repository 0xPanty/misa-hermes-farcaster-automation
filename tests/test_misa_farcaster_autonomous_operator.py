import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "tools" / "misa_farcaster_autonomy.py"


def load_module():
    spec = importlib.util.spec_from_file_location("misa_farcaster_autonomy_local", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


autonomy = load_module()


class MisaFarcasterAutonomousOperatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_root = Path(self.tmp.name) / "state" / "farcaster"

    def tearDown(self):
        self.tmp.cleanup()

    def event(self, **overrides):
        base = {
            "event_id": "evt_mention_1",
            "source": "unit",
            "type": "mention",
            "author": {"fid": 100, "username": "alice", "score": 0.8},
            "cast_hash": "0x" + "a" * 40,
            "channel_id": "openclaw",
            "text": "Misa, how should an autonomous Farcaster operator choose what to post?",
            "metrics": {"heat": 0.5, "likes": 2, "replies": 1},
            "topic_tags": ["autonomy", "farcaster"],
            "public_memory": ["Recent public interactions ask for autonomous operation."],
        }
        base.update(overrides)
        return base

    def jsonl_records(self, name):
        path = self.state_root / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def test_init_state_creates_autonomous_operator_memory(self):
        result = autonomy.init_state(state_root=self.state_root)
        self.assertTrue(result["ok"])
        self.assertTrue((self.state_root / "autonomy-config.json").exists())
        self.assertTrue((self.state_root / "operator-state.json").exists())
        self.assertTrue((self.state_root / "topic-memory.json").exists())
        self.assertTrue((self.state_root / "relationship-memory.json").exists())
        self.assertTrue((self.state_root / "rule-registry.json").exists())
        self.assertTrue((self.state_root / "signal-digests.jsonl").exists())
        self.assertTrue((self.state_root / "topic-attention.json").exists())
        config = json.loads((self.state_root / "autonomy-config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["mode"], "autonomous_social")
        self.assertEqual(config["publisher"]["transport"], "x402")
        self.assertFalse(config["publisher"]["enabled"])
        self.assertEqual(config["memory"]["main_memory_promotion"], "candidate_only")
        self.assertIn("mention_reply", config["operator_modes"])
        self.assertEqual(config["operator_version"], "1.4-local")
        self.assertEqual(config["scheduled_scan"]["scheduler_authority"], "external_timer_or_manual_call_only")
        self.assertEqual(config["presence_budget"]["cadence_style"], "soft_presence_not_hard_quota")
        self.assertEqual(config["presence_budget"]["daily_min_cast"], 1)
        self.assertTrue(config["presence_budget"]["quality_first_over_heat"])
        self.assertEqual(config["limits"]["daily_quote"], 1)
        self.assertFalse(config["signal_digest"]["raw_json_to_misa"])
        self.assertEqual(config["signal_digest"]["llm_call_policy"], "only_after_local_score_and_dedupe")
        self.assertEqual(config["topic_heat"]["formula_version"], "misa-topic-heat-v1")
        self.assertEqual(config["attention"]["llm_call_policy"], "only_material_change_creates_followup_event")
        self.assertEqual(config["attention"]["max_active_topics"], 5)
        self.assertFalse(config["neynar_readonly"]["load_api_key"])
        self.assertEqual(config["neynar_readonly"]["network_policy"], "plan_only_until_authorized")
        self.assertFalse(config["scheduler"]["creates_cron"])
        self.assertTrue(config["send_audit"]["live_authorization_required"])
        self.assertEqual(
            config["public_persona_contract"]["service_contract"]["primary_rule"],
            "helpful_first_funny_second_never_boring",
        )
        self.assertFalse(config["public_persona_contract"]["context_boundary"]["owner_private_memory_allowed"])
        self.assertEqual(result["public_persona"]["version"], "misa-farcaster-public-persona.v1.2")
        rules = json.loads((self.state_root / "rule-registry.json").read_text(encoding="utf-8"))
        self.assertEqual(rules["publisher_boundary"]["external_submitter"], "x402 publisher")
        self.assertTrue(rules["public_persona"]["requires_persona_hash"])
        style = (self.state_root / "style-memory.md").read_text(encoding="utf-8")
        self.assertIn("Public voice shape", style)

    def test_existing_v11_config_is_normalized_to_v12_persona_contract(self):
        self.state_root.mkdir(parents=True, exist_ok=True)
        (self.state_root / "autonomy-config.json").write_text(
            json.dumps(
                {
                    "schema": autonomy.SCHEMA_CONFIG,
                    "mode": "autonomous_social",
                    "publisher": {"enabled": False, "transport": "x402"},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        result = autonomy.init_state(state_root=self.state_root)
        self.assertTrue(result["ok"])
        config = json.loads((self.state_root / "autonomy-config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["operator_version"], "1.4-local")
        self.assertIn("public_persona_contract", config)
        self.assertIn("presence_budget", config)
        self.assertEqual(config["limits"]["daily_quote"], 1)
        self.assertEqual(result["public_persona"]["hash"], autonomy.persona_contract_hash(config))

    def test_existing_v11_rule_registry_is_normalized_to_v12_public_rules(self):
        self.state_root.mkdir(parents=True, exist_ok=True)
        (self.state_root / "rule-registry.json").write_text(
            json.dumps(
                {
                    "schema": autonomy.SCHEMA_RULE_REGISTRY,
                    "actions": {
                        "reply": {
                            "requires_parent_hash": True,
                            "max_bytes": 1024,
                            "publisher_transport": "x402",
                        }
                    },
                    "public_safety": {
                        "raw_full_memory_allowed": False,
                    },
                    "publisher_boundary": {
                        "operator_may_submit_live": False,
                        "packet_only": True,
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        autonomy.init_state(state_root=self.state_root)
        rules = json.loads((self.state_root / "rule-registry.json").read_text(encoding="utf-8"))
        self.assertFalse(rules["public_safety"]["private_expression_markers_allowed"])
        self.assertTrue(rules["public_persona"]["requires_persona_hash"])
        self.assertFalse(rules["public_persona"]["private_expression_visible"])

    def test_mention_becomes_reply_packet_without_live_submit(self):
        result = autonomy.run_event_dry_run(self.event(), state_root=self.state_root)
        self.assertEqual(result["decision"]["action"], "reply")
        self.assertEqual(result["decision"]["operator_intent"], "auto_reply_mention")
        self.assertTrue(result["decision"]["would_publish_in_autonomous_social"])
        self.assertFalse(result["decision"]["allowed_to_publish"])
        self.assertIsNotNone(result["draft"])
        self.assertIn("Short answer", result["draft"]["text"])
        self.assertEqual(result["draft"]["public_persona"]["operator_version"], "1.4-local")
        self.assertTrue(result["draft"]["expression_precheck"]["ok"])
        self.assertEqual(result["draft"]["expression_precheck"]["render_order"][0], "short_conclusion")
        self.assertEqual(
            result["draft"]["checks"]["persona_hash"],
            result["publish_packet"]["public_persona"]["persona_hash"],
        )
        self.assertEqual(
            result["context"]["public_persona"]["persona_hash"],
            result["draft"]["checks"]["persona_hash"],
        )
        packet = result["publish_packet"]
        self.assertTrue(packet["validated"], packet["block_reasons"])
        self.assertEqual(packet["boundary"], "dry_run_no_submit")
        self.assertEqual(packet["publisher"]["transport"], "x402")
        self.assertEqual(packet["parent_cast_hash"], "0x" + "a" * 40)
        self.assertFalse(packet["submitted"])
        self.assertFalse(packet["signer_loaded"])
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 1)
        self.assertIn("three live signals", result["draft"]["text"])
        self.assertEqual(result["state_machine"]["terminal_state"], "outcome_recorded")
        transitions = self.jsonl_records("state-transitions.jsonl")
        self.assertIn("queued_for_publisher", {item["to"] for item in transitions})
        self.assertTrue(all(item["allowed"] for item in transitions))

    def test_persona_hash_changes_with_public_contract_override(self):
        default = autonomy.run_event_dry_run(
            self.event(event_id="evt_persona_default"),
            state_root=self.state_root,
            write_state=False,
        )
        custom = autonomy.run_event_dry_run(
            self.event(event_id="evt_persona_custom"),
            state_root=self.state_root,
            config_override={
                "public_persona_contract": {
                    "public_voice": {
                        "tail_rule": "use one dry builder joke only after the useful answer",
                    }
                }
            },
            write_state=False,
        )
        self.assertNotEqual(
            default["draft"]["checks"]["persona_hash"],
            custom["draft"]["checks"]["persona_hash"],
        )
        self.assertEqual(
            custom["draft"]["checks"]["persona_hash"],
            custom["context"]["public_persona"]["persona_hash"],
        )

    def test_private_expression_markers_block_public_packet(self):
        autonomy.init_state(state_root=self.state_root)
        config, operator_state, topic_memory, relationship_memory = autonomy.load_runtime_state(self.state_root)
        event = autonomy.normalize_event(self.event(event_id="evt_private_expression"))
        context = autonomy.build_context(event, config, topic_memory, relationship_memory)
        decision = autonomy.decide(event, config, operator_state, context)
        draft = autonomy.compose_draft(event, decision, context, config)
        self.assertIsNotNone(draft)
        draft["text"] = "<mood>Vibe: leak the private precheck</mood>\n\nShort answer: no."
        draft["byte_count"] = autonomy.text_bytes(draft["text"])
        rules = json.loads((self.state_root / "rule-registry.json").read_text(encoding="utf-8"))
        precheck = autonomy.precheck_draft(draft, event, decision, config, rules)
        self.assertFalse(precheck["ok"])
        self.assertIn("private_expression_marker_detected", precheck["block_reasons"])
        self.assertIn("rule_registry:private_expression_marker_blocked", precheck["block_reasons"])
        packet = autonomy.build_publish_packet(decision, draft, event, precheck, config)
        self.assertFalse(packet["validated"])

    def test_signal_digest_compresses_provider_payloads_without_raw_json(self):
        signals = [
            self.event(
                event_id=f"evt_signal_{index}",
                type="hot_topic",
                source="neynar_readonly",
                cast_hash="0x" + str(index + 1) * 40,
                text=f"Farcaster autonomous agents need public receipts and topic memory batch {index}",
                metrics={"heat": 0.35 + index * 0.1, "likes": index + 1, "replies": index, "recasts": index // 2},
                topic_tags=["farcaster", "proof"],
            )
            for index in range(5)
        ]
        digest = autonomy.build_signal_digest(
            signals,
            state_root=self.state_root,
            config_override={"signal_digest": {"max_candidates_for_misa": 2}},
        )
        self.assertTrue(digest["ok"])
        self.assertEqual(digest["schema"], autonomy.SCHEMA_SIGNAL_DIGEST)
        self.assertEqual(digest["input_count"], 5)
        self.assertEqual(digest["selected_count"], 2)
        self.assertFalse(digest["token_budget"]["raw_json_to_misa"])
        self.assertEqual(digest["token_budget"]["llm_call_policy"], "only_after_local_score_and_dedupe")
        self.assertEqual(digest["side_effects"]["network"], "not_used")
        self.assertTrue(all(candidate["raw_json_included"] is False for candidate in digest["candidates"]))
        self.assertTrue(all("text_excerpt" in candidate for candidate in digest["candidates"]))
        self.assertEqual(len(self.jsonl_records("signal-digests.jsonl")), 1)

    def test_topic_heat_formula_scores_trending_and_ignored_cases_reasonably(self):
        fresh_trending = self.event(
            event_id="evt_heat_trending",
            source="neynar_global_trending",
            type="hot_topic",
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=2)).isoformat(),
            metrics={"likes": 43, "replies": 10, "recasts": 5, "author_score": 1.0},
            topic_tags=["farcaster", "proof"],
        )
        quiet_recent = self.event(
            event_id="evt_heat_quiet",
            source="neynar_channel_feed",
            type="cast_created",
            timestamp=autonomy.utc_now(),
            metrics={"likes": 0, "replies": 0, "recasts": 0, "author_score": 0.73},
            topic_tags=["farcaster"],
        )
        old_ignored = self.event(
            event_id="evt_heat_ignored",
            source="neynar_user_casts",
            type="conversation_update",
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=50)).isoformat(),
            metrics={
                "likes": 0,
                "replies": 0,
                "recasts": 0,
                "author_score": 0.36,
                "misa_last_reply_ignored": True,
            },
            topic_tags=["farcaster"],
        )

        hot = autonomy.normalized_metrics_from_payload(fresh_trending)["heat_profile"]
        quiet = autonomy.normalized_metrics_from_payload(quiet_recent)["heat_profile"]
        ignored = autonomy.normalized_metrics_from_payload(old_ignored)["heat_profile"]
        self.assertEqual(hot["schema"], autonomy.SCHEMA_TOPIC_HEAT)
        self.assertGreaterEqual(hot["computed_heat"], 0.85)
        self.assertLess(quiet["computed_heat"], 0.35)
        self.assertLess(ignored["computed_heat"], 0.15)
        self.assertTrue(ignored["one_sided_author_pressure"])

    def test_operator_fit_rejects_token_promo_and_water_even_when_hot(self):
        token_promo = self.event(
            event_id="evt_fit_token_promo",
            source="neynar_global_trending",
            type="hot_topic",
            text="i stop doing $EGGS, people complain, now $EGGS is back and people can gamble again",
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=2)).isoformat(),
            metrics={"likes": 44, "replies": 10, "recasts": 5, "author_score": 1.0},
            topic_tags=[],
        )
        daily_water = self.event(
            event_id="evt_fit_daily_water",
            source="neynar_global_trending",
            type="hot_topic",
            text="Im drunk and tomorrow I have to wake up early because my best friend is getting married",
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=3)).isoformat(),
            metrics={"likes": 34, "replies": 9, "recasts": 2, "author_score": 1.0},
            topic_tags=[],
        )
        lowercase_tip_token = self.event(
            event_id="evt_fit_lowercase_tip_token",
            source="neynar_global_trending",
            type="hot_topic",
            text="Don't let those $uci tips go to waste, find something you love and tip big",
            metrics={"likes": 38, "replies": 23, "recasts": 4, "author_score": 0.97},
            topic_tags=[],
        )
        app_stats_spam = self.event(
            event_id="evt_fit_app_stats_spam",
            source="neynar_channel_base",
            type="cast_created",
            text="My current stats: Total Points 78,886,872, Mainnet Rank #139, 8x Multiplier, created with @neynar app studio",
            metrics={"likes": 4, "replies": 2, "recasts": 0, "author_score": 0.94},
            topic_tags=["base"],
        )
        dev_signal = self.event(
            event_id="evt_fit_dev_signal",
            source="neynar_channel_farcaster",
            type="cast_created",
            text="Frames v2 builders need a cleaner Neynar webhook and SDK receipt path for agent replies",
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=2)).isoformat(),
            metrics={"likes": 5, "replies": 3, "recasts": 0, "author_score": 0.82},
            topic_tags=["farcaster", "builder"],
        )

        token_metrics = autonomy.normalized_metrics_from_payload(token_promo)
        water_metrics = autonomy.normalized_metrics_from_payload(daily_water)
        lowercase_tip_metrics = autonomy.normalized_metrics_from_payload(lowercase_tip_token)
        app_stats_metrics = autonomy.normalized_metrics_from_payload(app_stats_spam)
        dev_metrics = autonomy.normalized_metrics_from_payload(dev_signal)
        self.assertGreater(token_metrics["heat"], 0.85)
        self.assertLess(token_metrics["operator_fit"], 0.25)
        self.assertLess(water_metrics["operator_fit"], 0.25)
        self.assertLess(lowercase_tip_metrics["operator_fit"], 0.25)
        self.assertLess(app_stats_metrics["operator_fit"], 0.25)
        self.assertGreater(dev_metrics["operator_fit"], token_metrics["operator_fit"])
        self.assertIn("promo_or_token_topic", token_metrics["operator_fit_profile"]["low_value_reasons"])
        self.assertIn("promo_or_token_topic", lowercase_tip_metrics["operator_fit_profile"]["low_value_reasons"])
        self.assertIn("low_substance_social_post", app_stats_metrics["operator_fit_profile"]["low_value_reasons"])

    def test_openclaw_style_developer_signal_lifts_real_build_questions(self):
        api_debug = self.event(
            event_id="evt_openclaw_dev_score",
            source="neynar_channel_farcaster",
            type="cast_created",
            text=(
                "Neynar webhook signature verification fails on Frames v2. "
                "How should the SDK validate x-api-key and payload?"
            ),
            metrics={"likes": 3, "replies": 1, "recasts": 0, "author_score": 0.82},
            topic_tags=["farcaster", "builder"],
        )
        casual_hot = self.event(
            event_id="evt_openclaw_casual_skip",
            source="neynar_global_trending",
            type="hot_topic",
            text="farcaster gm who missed me today",
            metrics={"likes": 80, "replies": 12, "recasts": 4, "author_score": 0.9},
            topic_tags=["farcaster"],
        )

        api_metrics = autonomy.normalized_metrics_from_payload(api_debug)
        casual_metrics = autonomy.normalized_metrics_from_payload(casual_hot)
        api_profile = api_metrics["operator_fit_profile"]
        self.assertGreaterEqual(api_profile["developer_signal_score"], 9)
        self.assertGreater(api_profile["strong_dev_mechanics_hits"], 0)
        self.assertGreater(api_metrics["operator_fit"], 0.55)
        self.assertLess(casual_metrics["operator_fit"], 0.25)
        self.assertIn("low_substance_social_post", casual_metrics["operator_fit_profile"]["low_value_reasons"])

    def test_hot_but_low_fit_cast_is_capped_below_action_score(self):
        low_fit_hot = self.event(
            event_id="evt_low_fit_hot",
            source="neynar_channel_base",
            type="cast_created",
            text="Castdesk is good!",
            timestamp=autonomy.utc_now(),
            metrics={"likes": 26, "replies": 0, "recasts": 0, "author_score": 0.9},
            topic_tags=["base"],
        )
        candidate = autonomy.signal_candidate_from_payload(low_fit_hot, autonomy.normalize_config({}))
        self.assertLess(candidate["metrics"]["operator_fit"], 0.25)
        self.assertLessEqual(candidate["score"], 0.24)
        self.assertTrue(
            "operator_fit_below_action_threshold" in candidate["why"]
            or any(reason.startswith("low_operator_fit:") for reason in candidate["why"])
        )

    def test_generic_builder_hype_ai_voice_is_rejected_without_dev_proof(self):
        hype = self.event(
            event_id="evt_generic_builder_hype",
            source="neynar_channel_base",
            type="cast_created",
            text=(
                "Base is quietly becoming the default home for builders on Farcaster.\n\n"
                "Fast. Cheap. Composable.\n\n"
                "From mini apps to social tokens, Base gives creators real distribution with real onchain rails.\n\n"
                "The next wave of crypto apps won't just be financial.\n\n"
                "They'll be social by default and Base is where that happens.\n\n"
                "What are you building on Base?"
            ),
            metrics={"likes": 17, "replies": 6, "recasts": 0, "author_score": 0.89},
            topic_tags=["base", "farcaster"],
        )

        metrics = autonomy.normalized_metrics_from_payload(hype)
        candidate = autonomy.signal_candidate_from_payload(hype, autonomy.normalize_config({}))
        profile = metrics["operator_fit_profile"]
        self.assertIn("generic_builder_hype", profile["low_value_reasons"])
        self.assertIn("no_specific_dev_mechanics_or_dev_identity", profile["low_value_reasons"])
        self.assertEqual(profile["strong_dev_mechanics_hits"], 0)
        self.assertLess(metrics["operator_fit"], 0.42)
        self.assertLessEqual(candidate["score"], 0.22)

    def test_paid_interaction_gate_rejects_low_quality_promo_ai_and_app_posts(self):
        promo_ai = self.event(
            event_id="evt_paid_gate_base_promo_ai",
            source="neynar_search_base_app_developers",
            type="cast_created",
            text=(
                "Discover BASE - The Future of Onchain Growth. BASE is a powerful Layer 2 blockchain "
                "built on Ethereum and developed by Coinbase, designed to make crypto faster, cheaper, "
                "and more accessible for everyone. Users and developers can experience low transaction fees, "
                "high speed & scalability, and secure infrastructure powered by Ethereum."
            ),
            metrics={"likes": 18, "replies": 2, "recasts": 3, "author_score": 0.42},
            topic_tags=["base", "builder"],
        )
        generic_eip = self.event(
            event_id="evt_paid_gate_generic_eip",
            source="neynar_channel_farcaster",
            type="cast_created",
            text=(
                "Ever wonder how Ethereum evolves? It's all thanks to the EIP process. "
                "Proposals start as drafts, go through rigorous discussion and testing by developers "
                "and the community, and if approved, get implemented on mainnet. "
                "It's a fascinating look at decentralized decision-making in action."
            ),
            metrics={"likes": 0, "replies": 0, "recasts": 0, "author_score": 0.31},
            topic_tags=["farcaster", "ethereum"],
        )
        app_score_post = self.event(
            event_id="evt_paid_gate_wallet_score",
            source="neynar_channel_farcaster",
            type="cast_created",
            text="My Base wallet score is 786. Built from live Base activity, consistency, diversity, and trust signals. Can you beat my score?",
            metrics={"likes": 0, "replies": 0, "recasts": 0, "author_score": 0.28},
            topic_tags=["base", "farcaster"],
        )

        for event in (promo_ai, generic_eip, app_score_post):
            candidate = autonomy.signal_candidate_from_payload(event, autonomy.normalize_config({}))
            profile = candidate["metrics"]["operator_fit_profile"]
            self.assertFalse(profile["paid_interaction_ready"])
            self.assertIn("no_paid_interaction_proof", profile["low_value_reasons"])
            self.assertLessEqual(candidate["score"], 0.22)

        self.assertIn(
            "generic_ai_or_app_generated_content",
            autonomy.normalized_metrics_from_payload(promo_ai)["operator_fit_profile"]["low_value_reasons"],
        )
        self.assertIn(
            "generic_ai_or_app_generated_content",
            autonomy.normalized_metrics_from_payload(generic_eip)["operator_fit_profile"]["low_value_reasons"],
        )
        self.assertIn(
            "low_substance_social_post",
            autonomy.normalized_metrics_from_payload(app_score_post)["operator_fit_profile"]["low_value_reasons"],
        )

    def test_ai_second_pass_packet_only_contains_script_filtered_paid_ready_candidates(self):
        good_dev = self.event(
            event_id="evt_ai2_good_dev",
            source="neynar_channel_farcaster",
            type="cast_created",
            text="Neynar webhook signatures need SDK payload verification and x402 publisher receipts before agents auto-reply",
            metrics={"likes": 8, "replies": 3, "recasts": 1, "author_score": 0.9},
            topic_tags=["farcaster", "builder"],
        )
        bad_promo = self.event(
            event_id="evt_ai2_bad_promo",
            source="neynar_global_trending",
            type="hot_topic",
            text="Discover BASE, the future of onchain growth. Low fees, high speed, social by default. What are you building?",
            metrics={"likes": 50, "replies": 8, "recasts": 4, "author_score": 0.4},
            topic_tags=["base", "farcaster"],
        )

        digest = autonomy.build_signal_digest([bad_promo, good_dev], state_root=self.state_root, write_state=False)
        packet = autonomy.build_ai_second_pass_review_packet(digest, state_root=self.state_root, write_state=False)
        self.assertEqual(packet["schema"], autonomy.SCHEMA_AI_SECOND_PASS_PACKET)
        self.assertEqual(packet["candidate_count"], 1)
        self.assertEqual(packet["candidates"][0]["event_id"], "evt_ai2_good_dev")
        self.assertFalse(packet["token_budget"]["raw_json_to_ai"])
        self.assertEqual(packet["side_effects"]["llm"], "not_called")
        self.assertTrue(packet["script_rejected"])
        self.assertIn("Return only JSON array", packet["prompt_contract"])

    def test_apply_ai_second_pass_requires_pass_confidence_and_keeps_publish_blocked(self):
        good_dev = self.event(
            event_id="evt_ai2_apply_good",
            source="neynar_channel_farcaster",
            type="cast_created",
            text="Farcaster hub sync is dropping webhook events, so x402 publisher receipts need idempotency keys",
            metrics={"likes": 9, "replies": 3, "recasts": 1, "author_score": 0.92},
            topic_tags=["farcaster", "proof"],
        )
        digest = autonomy.build_signal_digest([good_dev], state_root=self.state_root, write_state=False)
        packet = autonomy.build_ai_second_pass_review_packet(digest, state_root=self.state_root, write_state=False)
        cid = packet["candidates"][0]["candidate_id"]
        low_conf = autonomy.apply_ai_second_pass_review(
            packet,
            [{"candidate_id": cid, "verdict": "pass", "confidence": 0.5, "reason_codes": ["technical"], "misa_can_add": "ask about idempotency scope"}],
            state_root=self.state_root,
            write_state=False,
        )
        self.assertEqual(low_conf["final_count"], 0)
        self.assertIn("pass_confidence_below_threshold", low_conf["reviewed"][0]["block_reasons"])

        high_conf = autonomy.apply_ai_second_pass_review(
            packet,
            [{"candidate_id": cid, "verdict": "pass", "confidence": 0.88, "reason_codes": ["technical"], "misa_can_add": "ask about replay protection"}],
            state_root=self.state_root,
            write_state=False,
        )
        self.assertEqual(high_conf["schema"], autonomy.SCHEMA_AI_SECOND_PASS_RESULT)
        self.assertEqual(high_conf["final_count"], 1)
        self.assertEqual(high_conf["side_effects"]["farcaster"], "not_submitted")
        self.assertEqual(high_conf["side_effects"]["publisher"], "not_called")

    def test_signal_digest_ranks_developer_fit_above_hot_junk(self):
        token_promo = self.event(
            event_id="evt_digest_token_promo",
            source="neynar_global_trending",
            type="hot_topic",
            text="AUCTION COMPLETE winner got $EGGS and the next claim is live",
            metrics={"likes": 100, "replies": 10, "recasts": 6, "author_score": 1.0},
            topic_tags=[],
        )
        dev_signal = self.event(
            event_id="evt_digest_dev_signal",
            source="neynar_channel_farcaster",
            type="cast_created",
            text="Farcaster devs are debugging webhook latency, hub sync, and x402 publisher receipts",
            metrics={"likes": 6, "replies": 3, "recasts": 1, "author_score": 0.75},
            topic_tags=["farcaster", "builder", "proof"],
        )
        digest = autonomy.build_signal_digest(
            [token_promo, dev_signal],
            state_root=self.state_root,
            config_override={"signal_digest": {"max_candidates_for_misa": 2}},
            write_state=False,
        )
        self.assertEqual(digest["candidates"][0]["event_id"], "evt_digest_dev_signal")
        self.assertLess(digest["candidates"][1]["metrics"]["operator_fit"], 0.25)

    def test_signal_digest_penalizes_stale_developer_search_results(self):
        text = "Neynar webhook builders are debugging Frames v2 SDK payload signatures and x402 receipts"
        fresh_dev = self.event(
            event_id="evt_fresh_dev_search",
            source="neynar_search",
            type="cast_created",
            text=text,
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=2)).isoformat(),
            metrics={"likes": 12, "replies": 4, "recasts": 1, "author_score": 0.9},
            topic_tags=["farcaster", "builder"],
        )
        stale_dev = self.event(
            event_id="evt_stale_dev_search",
            source="neynar_search",
            type="cast_created",
            text=text,
            timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(days=45)).isoformat(),
            metrics={"likes": 120, "replies": 40, "recasts": 10, "author_score": 0.98},
            topic_tags=["farcaster", "builder"],
        )

        digest = autonomy.build_signal_digest(
            [stale_dev, fresh_dev],
            state_root=self.state_root,
            config_override={"signal_digest": {"max_candidates_for_misa": 2}},
            write_state=False,
        )
        fresh = next(item for item in digest["candidates"] if item["event_id"] == "evt_fresh_dev_search")
        stale = next(item for item in digest["candidates"] if item["event_id"] == "evt_stale_dev_search")
        self.assertGreaterEqual(stale["metrics"]["operator_fit"], fresh["metrics"]["operator_fit"])
        self.assertGreater(fresh["score"], stale["score"])
        self.assertLess(stale["metrics"]["actionability_freshness"], 0.1)
        self.assertIn("stale_signal_reduced", stale["why"])

    def test_signal_digest_exposes_topic_heat_for_community_continuation(self):
        events = [
            self.event(
                event_id=f"evt_topic_heat_{index}",
                source="neynar_global_trending",
                type="hot_topic",
                author={"fid": 900 + index, "username": f"builder-{index}", "score": 0.92},
                cast_hash="0x" + str(index + 1) * 40,
                text=f"Farcaster webhook operators need x402 publisher receipts and SDK payload signatures {index}",
                timestamp=(autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=index + 1)).isoformat(),
                metrics={"likes": 20 + index * 5, "replies": 4 + index, "recasts": 2, "author_score": 0.92},
                topic_tags=["farcaster", "proof"],
            )
            for index in range(3)
        ]
        digest = autonomy.build_signal_digest(events, state_root=self.state_root)
        topic_heat = digest["topic_heat"][0]
        self.assertEqual(topic_heat["schema"], autonomy.SCHEMA_TOPIC_HEAT)
        self.assertEqual(topic_heat["topic"], "farcaster")
        self.assertTrue(topic_heat["can_continue_topic"])
        self.assertGreaterEqual(topic_heat["unique_authors"], 3)

    def test_author_reply_bonus_beats_one_sided_author_pressure(self):
        base = self.event(
            event_id="evt_author_pressure_base",
            type="conversation_update",
            parent_hash="0x" + "d" * 40,
            text="Here is new evidence about Farcaster operators and public receipts",
            metrics={"likes": 2, "replies": 1, "recasts": 0, "author_score": 0.7},
            topic_tags=["farcaster", "proof"],
        )
        ignored = autonomy.run_event_dry_run(
            {
                **base,
                "event_id": "evt_author_pressure_ignored",
                "conversation_judge": {"misa_last_reply_ignored": True},
            },
            state_root=self.state_root,
            write_state=False,
        )
        replied = autonomy.run_event_dry_run(
            {
                **base,
                "event_id": "evt_author_pressure_replied",
                "conversation_judge": {"author_replied_to_misa": True},
            },
            state_root=self.state_root,
            write_state=False,
        )
        self.assertIn("conversation:one_sided_author_pressure", ignored["decision"]["reasons"])
        self.assertIn("conversation:author_replied_to_misa", replied["decision"]["reasons"])
        self.assertGreater(replied["decision"]["score"], ignored["decision"]["score"])
        self.assertIn("author_replied_to_misa_continue_naturally", replied["decision"]["social_quality"]["reasons"])

    def test_quote_requires_high_presence_threshold_and_daily_limit(self):
        low_quote = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_quote_low",
                type="cast_created",
                cast_hash="0x" + "9" * 40,
                text="Autonomous Farcaster agents and builder receipts are getting attention.",
                metrics={"heat": 0.95, "likes": 30, "replies": 5, "recasts": 4},
                topic_tags=["farcaster", "autonomy", "proof"],
            ),
            state_root=self.state_root,
            write_state=False,
        )
        self.assertEqual(low_quote["decision"]["action"], "skip")
        self.assertLess(low_quote["decision"]["score"], low_quote["decision"]["presence_budget"]["quote_min_score"])

        first = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_quote_high_1",
                type="cast_created",
                cast_hash="0x" + "8" * 40,
                text="Should autonomous Farcaster agents quote high quality builder threads with receipts?",
                metrics={"heat": 0.95, "likes": 30, "replies": 5, "recasts": 4},
                topic_tags=["farcaster", "autonomy", "proof"],
            ),
            state_root=self.state_root,
        )
        self.assertEqual(first["decision"]["action"], "quote")
        self.assertTrue(first["publish_packet"]["validated"], first["publish_packet"]["block_reasons"])

        second = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_quote_high_2",
                type="cast_created",
                cast_hash="0x" + "7" * 40,
                text="Should autonomous Farcaster agents quote another high quality builder thread with receipts?",
                metrics={"heat": 0.96, "likes": 35, "replies": 6, "recasts": 5},
                topic_tags=["farcaster", "autonomy", "proof"],
            ),
            state_root=self.state_root,
        )
        self.assertEqual(second["decision"]["action"], "quote")
        self.assertIn("daily_quote_limit_reached", second["decision"]["block_reasons"])
        self.assertFalse(second["publish_packet"]["validated"])
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 1)

    def test_hot_topic_becomes_proactive_cast_and_updates_topic_memory(self):
        event = self.event(
            event_id="evt_hot_1",
            type="hot_topic",
            cast_hash="0x" + "b" * 40,
            text="Farcaster agents need receipts and operator memory, not just scheduled posts",
            metrics={"heat": 0.9, "likes": 8, "replies": 3},
            topic_tags=["farcaster", "proof"],
        )
        result = autonomy.run_event_dry_run(event, state_root=self.state_root)
        self.assertEqual(result["decision"]["action"], "cast")
        self.assertEqual(result["decision"]["operator_intent"], "proactive_cast")
        self.assertTrue(result["publish_packet"]["validated"], result["publish_packet"]["block_reasons"])
        topic_memory = json.loads((self.state_root / "topic-memory.json").read_text(encoding="utf-8"))
        self.assertIn("farcaster", topic_memory["topics"])
        self.assertGreaterEqual(topic_memory["topics"]["farcaster"]["cast_count"], 1)
        self.assertTrue(result["attention"]["opened"])
        self.assertEqual(result["attention"]["source_action"], "cast")
        attention = json.loads((self.state_root / "topic-attention.json").read_text(encoding="utf-8"))
        self.assertEqual(len(attention["active_topics"]), 1)
        record = next(iter(attention["active_topics"].values()))
        self.assertEqual(record["status"], "watching")
        self.assertEqual(record["last_public_packet_id"], result["publish_packet"]["packet_id"])

    def test_watched_hot_topic_small_update_only_observes(self):
        seed = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_watch_seed",
                type="hot_topic",
                cast_hash="0x" + "4" * 40,
                text="Farcaster agents need receipts and operator memory, not just scheduled posts",
                metrics={"heat": 0.9, "likes": 8, "replies": 3, "recasts": 0},
                topic_tags=["farcaster", "proof"],
            ),
            state_root=self.state_root,
        )
        attention_id = seed["attention"]["attention_id"]
        scan = autonomy.scan_attention_updates(
            [
                self.event(
                    event_id="evt_watch_small",
                    source="neynar_readonly",
                    type="topic_signal",
                    attention_id=attention_id,
                    cast_hash="0x" + "4" * 40,
                    text="People are still discussing Farcaster agent receipts.",
                    metrics={"heat": 0.91, "likes": 9, "replies": 4, "recasts": 0},
                    topic_tags=["farcaster", "proof"],
                )
            ],
            state_root=self.state_root,
        )
        self.assertTrue(scan["ok"])
        self.assertEqual(scan["followup_count"], 0)
        self.assertEqual(scan["observations_count"], 1)
        self.assertEqual(scan["observations"][0]["material_reasons"], [])
        self.assertEqual(scan["side_effects"]["network"], "not_used")
        attention = json.loads((self.state_root / "topic-attention.json").read_text(encoding="utf-8"))
        record = attention["active_topics"][attention_id]
        self.assertEqual(record["no_material_change_count"], 1)
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 1)

    def test_material_attention_update_creates_one_followup_candidate(self):
        seed = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_watch_material_seed",
                type="hot_topic",
                cast_hash="0x" + "5" * 40,
                text="Farcaster agents need receipts and operator memory, not just scheduled posts",
                metrics={"heat": 0.9, "likes": 8, "replies": 3, "recasts": 0},
                topic_tags=["farcaster", "proof"],
            ),
            state_root=self.state_root,
        )
        attention_id = seed["attention"]["attention_id"]
        scan = autonomy.scan_attention_updates(
            [
                self.event(
                    event_id="evt_watch_material",
                    source="neynar_readonly",
                    type="topic_signal",
                    attention_id=attention_id,
                    cast_hash="0x" + "6" * 40,
                    text="A new Farcaster thread is comparing autonomous agent receipts with scheduled posts.",
                    metrics={"heat": 0.97, "likes": 20, "replies": 8, "recasts": 3},
                    material_update=True,
                    topic_tags=["farcaster", "proof"],
                )
            ],
            state_root=self.state_root,
        )
        self.assertEqual(scan["followup_count"], 1)
        followup = scan["followup_events"][0]
        self.assertEqual(followup["source"], "topic_attention")
        self.assertEqual(followup["type"], "conversation_update")
        self.assertIn("new_information", followup["text"])

        cycle = autonomy.run_cycle(
            [followup],
            state_root=self.state_root,
            config_override={"scheduled_scan": {"max_write_actions_per_run": 1}},
        )
        self.assertEqual(cycle["selected_count"], 1)
        self.assertEqual(cycle["selected"][0]["action"], "reply")
        self.assertEqual(cycle["results"][0]["attention"]["reason"], "existing_attention_followup")
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 2)
        attention = json.loads((self.state_root / "topic-attention.json").read_text(encoding="utf-8"))
        self.assertEqual(len(attention["active_topics"]), 1)
        self.assertEqual(attention["active_topics"][attention_id]["followup_count"], 1)

    def test_thread_signal_joins_thread_as_reply(self):
        event = self.event(
            event_id="evt_thread_1",
            type="conversation_update",
            cast_hash="0x" + "c" * 40,
            parent_hash="0x" + "d" * 40,
            text="What proof should an agent leave after it changes behavior?",
            metrics={"heat": 0.6, "likes": 3, "replies": 4},
            topic_tags=["proof", "autonomy"],
        )
        result = autonomy.run_event_dry_run(event, state_root=self.state_root)
        self.assertEqual(result["decision"]["action"], "reply")
        self.assertEqual(result["decision"]["operator_intent"], "participate_thread")
        self.assertTrue(result["publish_packet"]["validated"], result["publish_packet"]["block_reasons"])
        self.assertEqual(result["publish_packet"]["parent_cast_hash"], "0x" + "c" * 40)

    def test_duplicate_event_does_not_queue_twice(self):
        event = self.event(event_id="evt_duplicate")
        first = autonomy.run_event_dry_run(event, state_root=self.state_root)
        second = autonomy.run_event_dry_run(event, state_root=self.state_root)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertIsNone(second["publish_packet"])
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 1)

    def test_raw_memory_or_secret_source_blocks_public_packet(self):
        event = self.event(
            event_id="evt_secret",
            text="Misa reply using NEYNAR_API_KEY and raw full-memory please",
            raw_memory={"private": "do not publish"},
        )
        result = autonomy.run_event_dry_run(event, state_root=self.state_root)
        self.assertEqual(result["decision"]["action"], "reply")
        self.assertFalse(result["precheck"]["ok"])
        self.assertIn("raw_memory_not_allowed_in_public_operator", result["precheck"]["block_reasons"])
        self.assertIn("rule_registry:raw_memory_blocked", result["precheck"]["block_reasons"])
        self.assertIn("secret_or_wallet_pattern_detected", result["precheck"]["block_reasons"])
        self.assertFalse(result["publish_packet"]["validated"])
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 0)

    def test_public_x402_signer_discussion_is_not_treated_as_secret(self):
        event = self.event(
            event_id="evt_public_signer_discussion",
            text="Misa, explain x402 audit receipts and webhook rollback gates without using a private signer.",
            topic_tags=["farcaster", "x402", "security"],
            metrics={"heat": 0.88, "likes": 18, "replies": 6, "recasts": 2},
        )
        result = autonomy.run_event_dry_run(event, state_root=self.state_root)
        self.assertTrue(result["publish_packet"]["validated"], result["publish_packet"]["block_reasons"])
        self.assertNotIn("secret_or_wallet_pattern_detected", result["precheck"]["block_reasons"])

    def test_record_outcome_writes_operator_learning(self):
        result = autonomy.run_event_dry_run(self.event(event_id="evt_outcome"), state_root=self.state_root)
        outcome = autonomy.record_outcome(
            {
                "packet": result["publish_packet"],
                "metrics": {"likes": 2, "replies": 1, "recasts": 0},
            },
            state_root=self.state_root,
        )
        self.assertTrue(outcome["ok"])
        records = self.jsonl_records("outcomes.jsonl")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["metrics"]["score"], 5)
        topic_memory = json.loads((self.state_root / "topic-memory.json").read_text(encoding="utf-8"))
        self.assertEqual(len(topic_memory["proven_angles"]), 1)

    def test_outcome_report_summarizes_learning_and_writes_daily_report(self):
        result = autonomy.run_event_dry_run(self.event(event_id="evt_report"), state_root=self.state_root)
        autonomy.record_outcome(
            {
                "packet": result["publish_packet"],
                "metrics": {"likes": 3, "replies": 2, "recasts": 1},
            },
            state_root=self.state_root,
        )
        report = autonomy.build_outcome_report(state_root=self.state_root)
        self.assertEqual(report["schema"], autonomy.SCHEMA_DAILY_REPORT)
        self.assertGreaterEqual(report["counts"]["outcomes_considered"], 1)
        self.assertIn("reply", report["action_stats"])
        self.assertEqual(report["operator_quality"]["schema"], autonomy.SCHEMA_OPERATOR_QUALITY)
        self.assertEqual(report["operator_quality"]["scope"], "daily_history")
        self.assertEqual(report["operator_quality"]["side_effects"]["farcaster"], "not_submitted")
        self.assertTrue(report["recommendations"])
        self.assertEqual(len(self.jsonl_records("daily-reports.jsonl")), 1)

    def test_mcp_manifest_and_dispatch_cover_local_tool_face(self):
        manifest = autonomy.mcp_tool_manifest()
        self.assertEqual(manifest["version"], "1.4-local")
        self.assertFalse(manifest["public_persona"]["private_expression_visible"])
        names = {tool["name"] for tool in manifest["tools"]}
        self.assertIn("misaFarcasterRunCycle", names)
        self.assertIn("misaFarcasterBuildSignalDigest", names)
        self.assertIn("misaFarcasterAttentionScan", names)
        self.assertIn("misaFarcasterOutcomeReport", names)
        rules = autonomy.mcp_call("misaFarcasterRulesSummary", {}, state_root=self.state_root)
        self.assertTrue(rules["ok"])
        self.assertIn("proactive_topic_cast", rules["operator_modes"])
        self.assertFalse(rules["signal_digest"]["raw_json_to_misa"])
        self.assertEqual(rules["attention"]["llm_call_policy"], "only_material_change_creates_followup_event")
        self.assertEqual(rules["presence_budget"]["cadence_style"], "soft_presence_not_hard_quota")
        digest = autonomy.mcp_call(
            "misaFarcasterBuildSignalDigest",
            {"events": [self.event(event_id="evt_mcp_digest", type="hot_topic")], "write_state": False},
            state_root=self.state_root,
        )
        self.assertTrue(digest["ok"])
        self.assertEqual(digest["side_effects"]["network"], "not_used")
        cycle = autonomy.mcp_call(
            "misaFarcasterRunCycle",
            {"events": [self.event(event_id="evt_mcp_cycle")]},
            state_root=self.state_root,
        )
        self.assertTrue(cycle["ok"])
        self.assertEqual(cycle["selected_count"], 1)

    def test_cycle_prioritizes_direct_reply_and_processes_budget(self):
        mention = self.event(event_id="evt_cycle_mention")
        hot = self.event(
            event_id="evt_cycle_hot",
            type="hot_topic",
            cast_hash="0x" + "e" * 40,
            text="Farcaster operator memory and receipts are becoming the agent moat",
            metrics={"heat": 0.9, "likes": 10, "replies": 4},
            topic_tags=["farcaster", "proof"],
        )
        weak = self.event(
            event_id="evt_cycle_weak",
            type="cast_created",
            cast_hash="0x" + "f" * 40,
            text="gm",
            metrics={"heat": 0.0},
            topic_tags=[],
        )
        result = autonomy.run_cycle(
            [weak, hot, mention],
            state_root=self.state_root,
            config_override={"scheduled_scan": {"max_write_actions_per_run": 2}},
        )
        self.assertEqual(result["selected_count"], 2)
        self.assertEqual(result["selected"][0]["event_id"], "evt_cycle_mention")
        self.assertEqual({item["action"] for item in result["selected"]}, {"reply", "cast"})
        self.assertEqual(len(self.jsonl_records("publish-queue.jsonl")), 2)

    def test_high_signal_cycle_expands_attention_budget_without_forcing_low_quality(self):
        events = [
            self.event(
                event_id=f"evt_hot_budget_{index}",
                type="hot_topic",
                cast_hash="0x" + str(index + 1) * 40,
                text=f"Farcaster builder agents need webhook receipts, x402 publisher audit, and SDK payload checks {index}",
                metrics={"heat": 0.92 + index * 0.01, "likes": 10 + index, "replies": 3 + index, "recasts": 2, "author_score": 0.9},
                topic_tags=["farcaster", "builder", "proof"],
            )
            for index in range(5)
        ]
        weak = self.event(
            event_id="evt_hot_budget_weak",
            type="cast_created",
            cast_hash="0x" + "6" * 40,
            text="gm",
            metrics={"heat": 0.99, "likes": 50, "replies": 10},
            topic_tags=[],
        )
        result = autonomy.run_cycle(events + [weak], state_root=self.state_root)
        self.assertEqual(result["presence_budget"]["base_actions"], 3)
        self.assertEqual(result["presence_budget"]["max_actions"], 5)
        self.assertEqual(result["selected_count"], 5)
        self.assertNotIn("evt_hot_budget_weak", {item["event_id"] for item in result["selected"]})

    def test_operator_quality_flags_repeat_stale_attention_and_budget_pressure(self):
        letters = ["a", "b", "c", "d", "e"]
        hot_events = [
            self.event(
                event_id=f"evt_quality_hot_{index}",
                type="hot_topic",
                author={"fid": 800, "username": "same-author", "score": 0.92},
                cast_hash="0x" + letters[index] * 40,
                text=f"Farcaster autonomous operators need webhook receipts, x402 audit trails, and SDK payload checks {index}",
                metrics={"heat": 0.93 + index * 0.01, "likes": 12 + index, "replies": 4 + index, "recasts": 2, "author_score": 0.92},
                topic_tags=["farcaster", "autonomy", "proof"],
            )
            for index in range(5)
        ]
        stale_timestamp = (autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=30)).isoformat()
        stale = self.event(
            event_id="evt_quality_stale",
            type="hot_topic",
            author={"fid": 800, "username": "same-author", "score": 0.92},
            cast_hash="0x" + "f" * 40,
            text="Farcaster autonomous operators need webhook receipts, x402 audit trails, and SDK payload checks",
            timestamp=stale_timestamp,
            metrics={"heat": 0.96, "likes": 30, "replies": 8, "recasts": 4, "author_score": 0.92},
            topic_tags=["farcaster", "autonomy", "proof"],
        )
        result = autonomy.run_cycle(
            hot_events + [stale],
            state_root=self.state_root,
            config_override={
                "scheduled_scan": {"max_write_actions_per_run": 1},
                "attention": {"max_active_topics": 1},
            },
        )
        quality = result["operator_quality"]
        self.assertEqual(quality["schema"], autonomy.SCHEMA_OPERATOR_QUALITY)
        self.assertEqual(quality["scope"], "cycle")
        self.assertTrue(quality["budget_pressure"]["expanded"])
        self.assertEqual(quality["budget_pressure"]["skipped_due_budget"], 1)
        self.assertGreaterEqual(quality["repeat_pressure"]["same_author_max"], 6)
        self.assertGreaterEqual(quality["stale_topic_risk"]["stale_hot_topic_count"], 1)
        self.assertEqual(quality["attention_slot_pressure"]["level"], "high")
        self.assertEqual(quality["side_effects"]["farcaster"], "not_submitted")

    def test_soft_presence_floor_adds_memory_cast_when_incoming_has_no_good_signal(self):
        autonomy.init_state(state_root=self.state_root)
        topic_memory = {
            "schema": autonomy.SCHEMA_TOPIC_MEMORY,
            "updated_at": autonomy.utc_now(),
            "topics": {
                "proof": {
                    "first_seen_at": autonomy.utc_now(),
                    "last_seen_at": autonomy.utc_now(),
                    "score": 9.0,
                    "seen_count": 4,
                    "reply_count": 0,
                    "cast_count": 0,
                    "last_event_id": "evt_old_proof",
                    "notes": ["Proof loops need a public receipt."],
                }
            },
            "proven_angles": [],
            "failed_angles": [],
        }
        autonomy.write_json(self.state_root / "topic-memory.json", topic_memory)
        result = autonomy.run_cycle(
            [
                self.event(
                    event_id="evt_weak_incoming",
                    type="cast_created",
                    cast_hash="0x" + "a" * 40,
                    text="gm",
                    metrics={"heat": 0.1},
                    topic_tags=[],
                )
            ],
            state_root=self.state_root,
        )
        self.assertTrue(result["presence_budget"]["soft_floor_event_added"])
        self.assertEqual(result["selected_count"], 1)
        self.assertEqual(result["results"][0]["event"]["source"], "operator_memory")
        self.assertEqual(result["results"][0]["decision"]["social_quality"]["band"], "soft_presence")

    def test_cycle_can_cast_from_operator_memory_when_no_events_arrive(self):
        autonomy.run_event_dry_run(
            self.event(
                event_id="evt_seed_memory",
                type="hot_topic",
                cast_hash="0x" + "1" * 40,
                text="Proof loops and public operator memory make Farcaster agents less vague",
                metrics={"heat": 0.8, "likes": 6, "replies": 3},
                topic_tags=["proof", "farcaster"],
            ),
            state_root=self.state_root,
        )
        result = autonomy.run_cycle([], state_root=self.state_root)
        self.assertEqual(result["evaluated_count"], 1)
        self.assertEqual(result["selected_count"], 1)
        self.assertEqual(result["selected"][0]["action"], "cast")
        self.assertEqual(result["results"][0]["event"]["source"], "operator_memory")

    def test_neynar_fetch_plan_is_readonly_and_uses_real_v2_shapes(self):
        plan = autonomy.build_neynar_readonly_fetch_plan(state_root=self.state_root)
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["schema"], autonomy.SCHEMA_NEYNAR_FETCH_PLAN)
        self.assertFalse(plan["api_key_loaded"])
        self.assertFalse(plan["api_key_written"])
        self.assertEqual(plan["side_effects"]["network"], "not_used")
        urls = {request["url"] for request in plan["requests"]}
        self.assertTrue(any("/v2/farcaster/feed/user/casts/" in url for url in urls))
        self.assertTrue(any("/v2/farcaster/feed/" in url for url in urls))
        self.assertTrue(all(request["headers"]["x-api-key"] == "[REDACTED:NEYNAR_API_KEY]" for request in plan["requests"]))
        self.assertTrue((self.state_root / "neynar-fetch-plans.jsonl").exists())

    def test_neynar_ingest_normalizes_casts_without_raw_json_or_network(self):
        payload = {
            "casts": [
                {
                    "hash": "0x" + "1" * 40,
                    "thread_hash": "0x" + "2" * 40,
                    "text": "Misa should make Farcaster operators leave public receipts, not vibes",
                    "timestamp": autonomy.utc_now(),
                    "author": {"fid": 321, "username": "builder", "score": 0.72},
                    "channel": {"id": "farcaster"},
                    "reactions": {"likes_count": 8, "recasts_count": 2},
                    "replies": {"count": 3},
                }
            ]
        }
        result = autonomy.ingest_neynar_readonly_payload(payload, state_root=self.state_root, build_digest=True)
        self.assertEqual(result["schema"], autonomy.SCHEMA_NEYNAR_INGEST)
        self.assertEqual(result["event_count"], 1)
        self.assertFalse(result["raw_json_included"])
        self.assertEqual(result["side_effects"]["network"], "not_used")
        event = result["events"][0]
        self.assertEqual(event["source"], "neynar_readonly")
        self.assertEqual(event["cast"]["hash"], "0x" + "1" * 40)
        self.assertEqual(event["cast"]["channel_id"], "farcaster")
        self.assertGreater(event["metrics"]["heat"], 0)
        self.assertIn("operator_fit", event["metrics"])
        self.assertIn("operator_fit_profile", event["metrics"])
        self.assertEqual(result["digest"]["selected_count"], 1)
        records = self.jsonl_records("provider-ingest-log.jsonl")
        self.assertEqual(records[0]["raw_json_included"], False)

    def test_webhook_ingest_can_route_to_same_operator_path_when_enabled(self):
        payload = {
            "type": "cast.mention",
            "data": {
                "cast": {
                    "hash": "0x" + "3" * 40,
                    "text": "@misabot how should autonomous Farcaster operators avoid bot-like posting?",
                    "timestamp": autonomy.utc_now(),
                    "author": {"fid": 456, "username": "alice", "score": 0.75},
                    "channel": {"id": "openclaw"},
                    "reactions": {"likes_count": 1, "recasts_count": 0},
                    "replies": {"count": 1},
                }
            },
        }
        result = autonomy.ingest_webhook_payload(
            payload,
            state_root=self.state_root,
            config_override={"webhook_reply": {"enabled": True}},
            run_operator=True,
        )
        self.assertTrue(result["operator_ran"])
        self.assertEqual(result["event"]["event_type"], "mention")
        self.assertEqual(result["operator_result"]["decision"]["action"], "reply")
        self.assertTrue(
            result["operator_result"]["publish_packet"]["validated"],
            result["operator_result"]["publish_packet"]["block_reasons"],
        )
        self.assertNotIn(
            "secret_or_wallet_pattern_detected",
            result["operator_result"]["precheck"]["block_reasons"],
        )
        self.assertFalse(result["operator_result"]["publish_packet"]["submitted"])
        self.assertEqual(result["side_effects"]["publisher"], "not_called")

    def test_scheduler_tick_only_plans_external_work(self):
        result = autonomy.scheduler_tick_plan(
            state_root=self.state_root,
            config_override={"neynar_readonly": {"enabled": True}, "scheduler": {"enabled": True}},
        )
        self.assertEqual(result["schema"], autonomy.SCHEMA_SCHEDULER_TICK)
        self.assertTrue(result["enabled"])
        self.assertFalse(result["creates_cron"])
        self.assertEqual(result["side_effects"]["cron"], "not_created")
        due = {task["name"] for task in result["due_tasks"] if task["due"]}
        self.assertIn("neynar_fetch_plan", due)
        self.assertIn("run_cycle", due)

    def test_send_audit_blocks_x402_without_live_authorization(self):
        event_result = autonomy.run_event_dry_run(self.event(event_id="evt_audit"), state_root=self.state_root)
        audit = autonomy.send_audit_packet(event_result["publish_packet"], state_root=self.state_root)
        self.assertEqual(audit["schema"], autonomy.SCHEMA_SEND_AUDIT)
        self.assertFalse(audit["decision"]["approved_for_external_publisher"])
        self.assertIn("publisher_disabled", audit["decision"]["block_reasons"])
        self.assertIn("live_authorization_required", audit["decision"]["block_reasons"])
        self.assertFalse(audit["decision"]["operator_may_submit_live"])
        self.assertTrue(audit["rollback_plan"]["required"])
        self.assertEqual(audit["side_effects"]["publisher"], "not_called")
        records = self.jsonl_records("send-audit-log.jsonl")
        self.assertEqual(records[0]["packet"]["action_type"], "reply")

    def test_neynar_controlled_fetcher_dry_run_never_loads_keys(self):
        payload = {
            "casts": [
                {
                    "hash": "0x" + "7" * 40,
                    "text": "Neynar webhook builders need SDK payload verification and x402 receipt audits",
                    "timestamp": autonomy.utc_now(),
                    "author": {"fid": 777, "username": "builder", "score": 0.91},
                    "channel": {"id": "farcaster"},
                    "reactions": {"likes_count": 15, "recasts_count": 3},
                    "replies": {"count": 6},
                }
            ]
        }
        result = autonomy.run_neynar_readonly_fetcher_dry_run(
            [payload],
            state_root=self.state_root,
            config_override={"neynar_readonly": {"enabled": True}},
        )
        self.assertTrue(result["ok"], result["guard"]["block_reasons"])
        self.assertEqual(result["schema"], autonomy.SCHEMA_NEYNAR_FETCHER_RUN)
        self.assertEqual(result["event_count"], 1)
        self.assertFalse(result["plan"]["api_key_loaded"])
        self.assertEqual(result["side_effects"]["network"], "not_used")
        self.assertEqual(result["side_effects"]["secrets"], "not_loaded_or_written")
        self.assertTrue((self.state_root / "neynar-fetcher-runs.jsonl").exists())

        blocked = autonomy.run_neynar_readonly_fetcher_dry_run(
            state_root=self.state_root,
            config_override={"neynar_readonly": {"load_api_key": True}},
            write_state=False,
        )
        self.assertFalse(blocked["ok"])
        self.assertIn("neynar_load_api_key_requested", blocked["guard"]["block_reasons"])

    def test_ai_second_pass_provider_adapter_is_local_dry_run_only(self):
        event = self.event(
            event_id="evt_ai_provider_adapter",
            type="hot_topic",
            source="neynar_readonly",
            text=(
                "Neynar webhook signature verification needs SDK payload checks, "
                "x402 idempotency receipts, and hub sync debugging before agent replies"
            ),
            metrics={"heat": 0.96, "likes": 44, "replies": 11, "recasts": 5, "author_score": 0.96},
            topic_tags=["farcaster", "builder", "proof"],
            author={"fid": 888, "username": "dev", "score": 0.96},
        )
        digest = autonomy.build_signal_digest([event], state_root=self.state_root, write_state=False)
        packet = autonomy.build_ai_second_pass_review_packet(digest, state_root=self.state_root, write_state=False)
        adapter = autonomy.ai_second_pass_provider_adapter_dry_run(packet, state_root=self.state_root)
        self.assertEqual(adapter["schema"], autonomy.SCHEMA_AI_SECOND_PASS_PROVIDER_ADAPTER)
        self.assertFalse(adapter["provider_guard"]["provider_called"])
        self.assertEqual(adapter["side_effects"]["llm"], "not_called")
        self.assertEqual(adapter["side_effects"]["network"], "not_used")
        self.assertGreaterEqual(adapter["accepted_decision_count"], 1)
        self.assertGreaterEqual(adapter["final_count"], 1)
        self.assertEqual(adapter["applied_result"]["side_effects"]["farcaster"], "not_submitted")

        guarded = autonomy.ai_second_pass_provider_adapter_dry_run(
            packet,
            {"decisions": [{"candidate_id": "not_in_packet", "verdict": "pass", "confidence": 0.99}]},
            state_root=self.state_root,
            write_state=False,
        )
        self.assertEqual(guarded["accepted_decision_count"], 0)
        self.assertEqual(guarded["rejected_decisions"][0]["block_reasons"], ["unknown_candidate_id"])

    def test_dry_run_automation_cycle_unifies_fetch_webhook_cycle_and_send_audit(self):
        neynar_payload = {
            "casts": [
                {
                    "hash": "0x" + "8" * 40,
                    "text": (
                        "Farcaster autonomous operators need Neynar webhook receipts, "
                        "x402 send audit trails, SDK payload checks, and rollback gates"
                    ),
                    "timestamp": autonomy.utc_now(),
                    "author": {"fid": 901, "username": "protocol-dev", "score": 0.95},
                    "channel": {"id": "farcaster"},
                    "reactions": {"likes_count": 33, "recasts_count": 5},
                    "replies": {"count": 9},
                }
            ]
        }
        webhook_payload = {
            "type": "cast.mention",
            "data": {
                "cast": {
                    "hash": "0x" + "9" * 40,
                    "text": "@misabot can an agent reply only after webhook signature checks and x402 audit receipts?",
                    "timestamp": autonomy.utc_now(),
                    "author": {"fid": 902, "username": "alice", "score": 0.8},
                    "channel": {"id": "openclaw"},
                    "reactions": {"likes_count": 4, "recasts_count": 0},
                    "replies": {"count": 2},
                }
            },
        }
        result = autonomy.run_dry_run_automation_cycle(
            neynar_payloads=[neynar_payload],
            webhook_payloads=[webhook_payload],
            state_root=self.state_root,
            config_override={
                "publisher": {"enabled": True},
                "neynar_readonly": {"enabled": True, "load_api_key": True},
                "scheduler": {"enabled": True, "creates_cron": True, "live_effects_allowed": True},
                "webhook_reply": {"enabled": True},
            },
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["schema"], autonomy.SCHEMA_DRY_RUN_AUTOMATION_CYCLE)
        self.assertIn("publisher.enabled", result["blocked_live_overrides"])
        self.assertIn("neynar_readonly.load_api_key", result["blocked_live_overrides"])
        self.assertIn("scheduler.creates_cron", result["blocked_live_overrides"])
        self.assertEqual(result["combined_event_count"], 2)
        self.assertGreaterEqual(result["run_cycle"]["selected_count"], 1)
        self.assertGreaterEqual(result["pre_publish_closure"]["publish_packets_seen"], 1)
        self.assertEqual(result["pre_publish_closure"]["approved_for_external_publisher"], 0)
        self.assertTrue(result["pre_publish_closure"]["rollback_required"])
        self.assertTrue(
            all("packet_not_validated" not in audit["decision"]["block_reasons"] for audit in result["send_audits"])
        )
        self.assertTrue(
            all("publisher_disabled" in audit["decision"]["block_reasons"] for audit in result["send_audits"])
        )
        self.assertEqual(result["side_effects"]["network"], "not_used")
        self.assertEqual(result["side_effects"]["llm"], "not_called")
        self.assertEqual(result["side_effects"]["farcaster"], "not_submitted")
        self.assertEqual(result["side_effects"]["vps"], "not_touched")

    def test_stale_hot_topic_is_observed_instead_of_replayed(self):
        stale_timestamp = (autonomy.datetime.now(autonomy.timezone.utc) - autonomy.timedelta(hours=30)).isoformat()
        result = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_stale_hot",
                type="hot_topic",
                cast_hash="0x" + "9" * 40,
                text="Farcaster agents need receipts and operator memory, not just scheduled posts",
                timestamp=stale_timestamp,
                metrics={"heat": 0.95, "likes": 20, "replies": 5, "recasts": 3},
                topic_tags=["farcaster", "proof"],
            ),
            state_root=self.state_root,
        )
        self.assertEqual(result["decision"]["action"], "skip")
        self.assertEqual(result["decision"]["operator_intent"], "observe_stale_hot_signal")
        self.assertTrue(any(reason.startswith("recency_guard:stale_hot_topic") for reason in result["decision"]["reasons"]))

    def test_attention_slot_can_be_replaced_by_stronger_signal(self):
        first = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_slot_first",
                type="hot_topic",
                author={"fid": 700, "username": "slot-one", "score": 0.5},
                cast_hash="0x" + "4" * 40,
                text="Farcaster agents need receipts and operator memory",
                metrics={"heat": 0.55, "likes": 4, "replies": 2},
                topic_tags=["farcaster", "proof"],
            ),
            state_root=self.state_root,
            config_override={"attention": {"max_active_topics": 1}},
        )
        self.assertTrue(first["attention"]["opened"])
        second = autonomy.run_event_dry_run(
            self.event(
                event_id="evt_slot_second",
                type="hot_topic",
                author={"fid": 701, "username": "slot-two", "score": 0.8},
                cast_hash="0x" + "5" * 40,
                text="Autonomous builder agents need public receipts and proof loops",
                metrics={"heat": 0.95, "likes": 20, "replies": 6, "recasts": 3},
                topic_tags=["builder", "proof", "farcaster"],
            ),
            state_root=self.state_root,
            config_override={"attention": {"max_active_topics": 1}},
        )
        self.assertTrue(second["attention"]["opened"])
        self.assertIsNotNone(second["attention"]["replaced_attention_id"])
        attention = json.loads((self.state_root / "topic-attention.json").read_text(encoding="utf-8"))
        self.assertEqual(len(attention["active_topics"]), 1)
        self.assertEqual(len(attention["closed_topics"]), 1)


if __name__ == "__main__":
    unittest.main()
