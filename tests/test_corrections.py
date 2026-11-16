from __future__ import annotations

import json
import unittest

from talent_harvest.audit import Auditor
from talent_harvest.cli import main, parser
from talent_harvest.contracts import HarvestError, Page
from talent_harvest.export import Delivery, verify_snapshot
from talent_harvest.inventory import Inventory
from talent_harvest.lineage import Lineage
from tests.support import Workspace, manifest, record, source


def correction(effective_at, *targets):
    return {"effective_at": effective_at,
            "supersedes": [dict(target) for target in targets]}


class CorrectionContractTests(unittest.TestCase):
    def test_page_without_correction_keeps_legacy_document(self):
        parsed = Page.parse({"ordinal": 1, "path": "a.json", "sha256": "a" * 64})
        self.assertEqual(parsed.document(), {"ordinal": 1, "path": "a.json", "sha256": "a" * 64})

    def test_correction_document_round_trips(self):
        declared = {"ordinal": 1, "path": "a.json", "sha256": "a" * 64,
                    "correction": {"effective_at": "2026-09-15",
                                   "supersedes": [{"record_id": "role-1", "version": 1},
                                                  {"record_id": "role-2", "version": 2, "source": "company"}]}}
        parsed = Page.parse(declared)
        self.assertEqual(parsed.correction.effective_at, "2026-09-15")
        self.assertEqual(len(parsed.correction.supersedes), 2)
        self.assertEqual(Page.parse(parsed.document()).document(), parsed.document())

    def test_correction_rejects_bad_declarations(self):
        base = {"ordinal": 1, "path": "a.json", "sha256": "a" * 64}
        invalid = [
            {"effective_at": "2026-13-01", "supersedes": [{"record_id": "role-1", "version": 1}]},
            {"effective_at": "2026-09-15"},
            {"effective_at": "2026-09-15", "supersedes": []},
            {"effective_at": "2026-09-15", "supersedes": [{"record_id": "role-1", "version": 0}]},
            {"effective_at": "2026-09-15", "supersedes": [{"record_id": "role-1"}]},
            {"effective_at": "2026-09-15", "supersedes": [{"record_id": "BAD ID", "version": 1}]},
        ]
        for broken in invalid:
            with self.subTest(broken=broken), self.assertRaises(HarvestError):
                Page.parse({**base, "correction": broken})


class CorrectionTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.work.register()
        self.delivery = Delivery(self.work.ledger)

    def tearDown(self):
        self.work.close()

    def latest_arrival(self):
        rows = self.work.ledger.observations()
        return max(row["observed_at"] for row in rows)

    def ingest_correction(self, batch, effective_at, targets, pages=None, key="company",
                          captured_at="2026-10-05T08:00:00+00:00"):
        pages = pages if pages is not None else [[record(quantity=6)]]
        return self.work.ingest(key=key, batch=batch, pages=pages, captured_at=captured_at,
                                corrections={1: correction(effective_at, *targets)})

    def test_late_correction_commits_arrival_effective_and_supersession(self):
        self.work.ingest()
        _, result = self.ingest_correction("batch-2", "2026-09-15", [{"record_id": "role-1", "version": 1}])
        self.assertEqual(result["state"], "complete")
        page = self.work.ledger.page("company", "batch-2", 1)
        self.assertEqual(page["state"], "complete")
        history = self.delivery.history(source="company")["observations"]
        self.assertEqual([row["version"] for row in history], [1, 2])
        arrived = history[1]
        self.assertEqual(arrived["captured_at"], "2026-10-05T08:00:00+00:00")
        self.assertEqual(arrived["effective_at"], "2026-09-15")
        self.assertTrue(arrived["observed_at"] > history[0]["observed_at"])
        chain = Lineage(self.work.ledger).record("company", "role-1")
        self.assertEqual(chain["versions"][0]["effective_at"], "2026-09-01")
        self.assertEqual(chain["versions"][0]["superseded_by"][0]["batch"], "batch-2")
        self.assertEqual(chain["versions"][1]["correction"]["effective_at"], "2026-09-15")
        self.assertEqual(chain["versions"][1]["correction"]["supersedes"],
                         [{"record_id": "role-1", "version": 1}])
        listed = Lineage(self.work.ledger).corrections()["corrections"]
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["state"], "active")
        self.assertEqual(listed[0]["records"], ["role-1"])
        self.assertEqual(self.delivery.current()["accepted"][0]["record"]["quantity"], 6)
        events = [event["event_type"] for event in self.work.ledger.events()]
        self.assertIn("correction_registered", events)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_unknown_target_fails_recoverably_until_target_arrives(self):
        self.work.ingest()
        path, failed = self.ingest_correction("batch-2", "2026-10-01",
                                              [{"record_id": "role-2", "version": 1}],
                                              pages=[[record(quantity=6)]])
        self.assertEqual(failed["state"], "failed")
        page = self.work.ledger.page("company", "batch-2", 1)
        self.assertEqual(page["state"], "failed")
        self.assertEqual(page["error_code"], "correction_target_unknown")
        self.assertEqual(len(self.work.ledger.observations()), 1)
        self.assertEqual(self.delivery.current()["accepted"][0]["record"]["quantity"], 4)
        self.assertEqual(Lineage(self.work.ledger).corrections()["corrections"], [])
        self.assertEqual(Inventory(self.work.ledger).recovery_plan()["retryable"], 1)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])
        self.work.ingest(batch="batch-3", pages=[[record(record_id="role-2", title="Researcher")]])
        recovered = self.work.harvester.ingest(path)
        self.assertEqual(recovered["state"], "complete")
        self.assertEqual(self.work.ledger.page("company", "batch-2", 1)["state"], "complete")
        current = self.delivery.current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["record"]["quantity"], 6)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_cross_source_reference_is_rejected(self):
        self.work.ingest()
        self.work.register(key="campus")
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-1", title="Platform Engineer")]])
        _, explicit = self.ingest_correction(
            "batch-2", "2026-10-01",
            [{"record_id": "vacancy-1", "version": 1, "source": "campus"}])
        self.assertEqual(explicit["state"], "failed")
        self.assertEqual(self.work.ledger.page("company", "batch-2", 1)["error_code"],
                         "correction_cross_source")
        _, implicit = self.ingest_correction(
            "batch-3", "2026-10-01", [{"record_id": "vacancy-1", "version": 1}])
        self.assertEqual(implicit["state"], "failed")
        self.assertEqual(self.work.ledger.page("company", "batch-3", 1)["error_code"],
                         "correction_target_unknown")
        self.assertEqual(len(self.work.ledger.observations()), 2)
        self.assertEqual(self.delivery.current()["counts"]["accepted"], 2)
        self.assertEqual(Lineage(self.work.ledger).corrections()["corrections"], [])

    def test_correction_cycle_fails_then_recovers_after_withdrawal(self):
        self.work.ingest(pages=[[record(record_id="role-a", title="Alpha Engineer"),
                                 record(record_id="role-b", title="Beta Engineer")]])
        _, first = self.ingest_correction(
            "batch-2", "2026-10-01", [{"record_id": "role-b", "version": 1}],
            pages=[[record(record_id="role-a", title="Alpha Engineer", quantity=5)]])
        self.assertEqual(first["state"], "complete")
        current = self.delivery.current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["record"]["title"], "Alpha Engineer")
        path, failed = self.ingest_correction(
            "batch-3", "2026-10-02", [{"record_id": "role-a", "version": 2}],
            pages=[[record(record_id="role-b", title="Beta Engineer", quantity=7)]])
        self.assertEqual(failed["state"], "failed")
        page = self.work.ledger.page("company", "batch-3", 1)
        self.assertEqual(page["state"], "failed")
        self.assertEqual(page["error_code"], "correction_cycle")
        self.assertEqual(len(self.work.ledger.observations()), 3)
        self.assertEqual(self.delivery.current()["counts"]["accepted"], 1)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])
        self.work.harvester.withdraw_correction("company", "batch-2", 1, "supersession revoked")
        recovered = self.work.harvester.ingest(path)
        self.assertEqual(recovered["state"], "complete")
        view = self.delivery.current(include_quarantine=True)
        quantities = {entity["record"]["title"]: entity["record"]["quantity"]
                      for entity in view["accepted"]}
        self.assertEqual(quantities, {"Alpha Engineer": 4, "Beta Engineer": 7})
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_cross_month_late_page_and_two_knowledge_cutoffs(self):
        path = manifest(self.work.path, pages=[
            [record()],
            [record(record_id="role-2", title="Research Scientist", published_at="2026-09-20")]])
        late_page = path.parent / "page-2.json"
        body = late_page.read_bytes()
        late_page.unlink()
        first = self.work.harvester.ingest(path)
        self.assertEqual([page["state"] for page in first["pages"]], ["complete", "failed"])
        knowledge_at_close = self.latest_arrival()
        late_page.write_bytes(body)
        resumed = self.work.harvester.resume(path)
        self.assertEqual(resumed["state"], "complete")
        self.assertEqual(self.work.ledger.page("company", "batch-1", 2)["attempts"], 2)
        self.ingest_correction("batch-2", "2026-09-15", [{"record_id": "role-1", "version": 1}])
        at_close = self.delivery.replay(knowledge=knowledge_at_close)
        self.assertEqual(at_close["counts"]["accepted"], 1)
        self.assertEqual(at_close["accepted"][0]["record"]["quantity"], 4)
        self.assertEqual(at_close["accepted"][0]["selected"]["version"], 1)
        late_reasons = {item["source_record_id"]: item["excluded_reason"]
                        for item in at_close["excluded"]}
        self.assertEqual(late_reasons, {"role-2": "not_known_at_cutoff",
                                        "role-1": "not_known_at_cutoff"})
        self.assertIn("knowledge_cutoff", {note["type"] for note in at_close["explanations"]})
        today = self.delivery.replay()
        self.assertEqual(today["counts"]["accepted"], 2)
        quantities = {entity["record"]["title"]: entity["record"]["quantity"]
                      for entity in today["accepted"]}
        self.assertEqual(quantities, {"AI Engineer": 6, "Research Scientist": 4})
        excluded = {item["source_record_id"]: item["excluded_reason"] for item in today["excluded"]}
        self.assertEqual(excluded, {"role-1": "superseded_by_correction"})
        self.assertEqual(today["excluded"][0]["detail"]["correction"],
                         {"source": "company", "batch": "batch-2", "ordinal": 1})
        summary_close = self.delivery.summary(knowledge=knowledge_at_close)
        summary_today = self.delivery.summary()
        self.assertEqual(summary_close["by_kind"]["position"]["quantity"], 4)
        self.assertEqual(summary_today["by_kind"]["position"]["quantity"], 10)
        self.assertEqual(summary_today["by_region"]["Shanghai"]["entities"], 2)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_business_date_replay_applies_retroactive_correction(self):
        self.work.ingest()
        self.ingest_correction("batch-2", "2026-09-15", [{"record_id": "role-1", "version": 1}])
        month_end = self.delivery.replay(business="2026-09-30")
        self.assertEqual(month_end["counts"]["accepted"], 1)
        self.assertEqual(month_end["accepted"][0]["record"]["quantity"], 6)
        self.assertEqual(month_end["accepted"][0]["selected"]["version"], 2)
        mid_month = self.delivery.replay(business="2026-09-10")
        self.assertEqual(mid_month["accepted"][0]["record"]["quantity"], 4)
        self.assertEqual(mid_month["accepted"][0]["selected"]["version"], 1)
        reasons = {item["version"]: item["excluded_reason"] for item in mid_month["excluded"]}
        self.assertEqual(reasons, {2: "not_yet_effective"})
        before_effective = self.delivery.replay(business="2026-08-31")
        self.assertEqual(before_effective["counts"]["accepted"], 0)
        self.assertEqual({item["excluded_reason"] for item in before_effective["excluded"]},
                         {"not_yet_effective"})
        summary = self.delivery.summary(business="2026-09-30")
        self.assertEqual(summary["by_kind"]["position"]["quantity"], 6)

    def test_opposing_sources_conflict_resolution_across_cutoffs(self):
        self.work.register(key="campus", trust=90)
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-1", quantity=11)]],
                         captured_at="2026-09-30T09:00:00+00:00")
        knowledge_before_correction = self.latest_arrival()
        self.ingest_correction("batch-2", "2026-09-15",
                               [{"record_id": "vacancy-1", "version": 1}],
                               key="campus", pages=[[record(record_id="vacancy-1", quantity=4)]])
        before = self.delivery.replay(knowledge=knowledge_before_correction)
        self.assertEqual(before["counts"]["accepted"], 0)
        self.assertEqual(before["counts"]["conflicts"], 1)
        self.assertEqual(before["conflicts"][0]["reason"], "quantity_mismatch")
        after = self.delivery.replay()
        self.assertEqual(after["counts"]["conflicts"], 0)
        self.assertEqual(after["counts"]["accepted"], 1)
        entity = after["accepted"][0]
        self.assertEqual(entity["record"]["quantity"], 4)
        self.assertEqual(entity["selected"]["source"], "campus")
        self.assertEqual(len(entity["evidence"]), 2)
        excluded = {item["source_record_id"]: item["excluded_reason"] for item in after["excluded"]}
        self.assertEqual(excluded, {"vacancy-1": "superseded_by_correction"})
        early_business = self.delivery.replay(business="2026-09-10")
        self.assertEqual(early_business["counts"]["conflicts"], 1)
        late_business = self.delivery.replay(business="2026-09-30")
        self.assertEqual(late_business["counts"]["accepted"], 1)
        self.assertEqual(late_business["accepted"][0]["record"]["quantity"], 4)

    def test_correction_withdrawal_restores_versions_and_explains(self):
        self.work.ingest()
        self.ingest_correction("batch-2", "2026-09-15", [{"record_id": "role-1", "version": 1}])
        self.assertEqual(self.delivery.current()["accepted"][0]["record"]["quantity"], 6)
        knowledge_with_correction = self.latest_arrival()
        snapshot_path = self.work.path / "delivery.json"
        snapshot = self.delivery.write_snapshot(snapshot_path)
        withdrawn = self.work.harvester.withdraw_correction("company", "batch-2", 1,
                                                            "source revoked the correction")
        self.assertFalse(withdrawn["unchanged"])
        self.assertEqual(self.delivery.current()["accepted"][0]["record"]["quantity"], 4)
        today = self.delivery.replay()
        self.assertEqual(today["accepted"][0]["selected"]["version"], 1)
        excluded = {item["version"]: item["excluded_reason"] for item in today["excluded"]}
        self.assertEqual(excluded, {2: "correction_withdrawn"})
        notes = {note["type"]: note for note in today["explanations"]}
        self.assertIn("correction_withdrawn", notes)
        self.assertEqual(notes["correction_withdrawn"]["restored"],
                         [{"record_id": "role-1", "version": 1}])
        replayed = self.delivery.replay(knowledge=knowledge_with_correction)
        self.assertEqual(replayed["accepted"][0]["record"]["quantity"], 6)
        self.assertEqual({note["type"] for note in replayed["explanations"]},
                         {"correction_applied"})
        verified = verify_snapshot(snapshot_path, snapshot["sha256"])
        self.assertEqual(verified["counts"]["accepted"], 1)
        chain = Lineage(self.work.ledger).record("company", "role-1")
        self.assertTrue(chain["versions"][1]["correction"]["withdrawn_at"])
        listed = Lineage(self.work.ledger).corrections()["corrections"]
        self.assertEqual(listed[0]["state"], "withdrawn")
        repeat = self.work.harvester.withdraw_correction("company", "batch-2", 1, "again")
        self.assertTrue(repeat["unchanged"])
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_correction_page_without_records_retracts_target(self):
        self.work.ingest()
        _, result = self.ingest_correction("batch-2", "2026-10-01",
                                           [{"record_id": "role-1", "version": 1}], pages=[[]])
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["pages"][0]["records"], 0)
        self.assertEqual(self.delivery.current()["counts"]["accepted"], 0)
        replay = self.delivery.replay()
        self.assertEqual(replay["excluded"][0]["excluded_reason"], "superseded_by_correction")
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_replay_respects_source_withdrawal_at_cutoff(self):
        self.work.register(key="campus", trust=90)
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-1", quantity=11)]])
        knowledge_both_sources = self.latest_arrival()
        self.work.harvester.withdraw("campus", "mirror retired")
        today = self.delivery.replay()
        self.assertEqual(today["counts"]["accepted"], 1)
        self.assertEqual(today["accepted"][0]["selected"]["source"], "company")
        before = self.delivery.replay(knowledge=knowledge_both_sources)
        self.assertEqual(before["counts"]["conflicts"], 1)

    def test_cli_replay_corrections_and_withdraw_correction(self):
        declaration = self.work.path / "source.json"
        declaration.write_text(json.dumps(source(key="campus")), encoding="utf-8")
        database = self.work.path / "cli.sqlite3"
        self.assertEqual(main(["--db", str(database), "register", str(declaration)]), 0)
        path = manifest(self.work.path, key="campus")
        self.assertEqual(main(["--db", str(database), "ingest", str(path)]), 0)
        path = manifest(self.work.path, key="campus", batch="batch-2",
                        pages=[[record(quantity=6)]],
                        corrections={1: correction("2026-09-15", {"record_id": "role-1", "version": 1})})
        self.assertEqual(main(["--db", str(database), "ingest", str(path)]), 0)
        self.assertEqual(main(["--db", str(database), "replay",
                               "--knowledge", "2026-10-01T00:00:00+00:00"]), 0)
        self.assertEqual(main(["--db", str(database), "replay", "--business", "2026-09-30"]), 0)
        self.assertEqual(main(["--db", str(database), "summary", "--business", "2026-09-30"]), 0)
        self.assertEqual(main(["--db", str(database), "corrections"]), 0)
        self.assertEqual(main(["--db", str(database), "withdraw-correction",
                               "campus", "batch-2", "1", "--reason", "revoked"]), 0)
        self.assertEqual(main(["--db", str(database), "corrections", "--source", "campus"]), 0)
        self.assertEqual(main(["--db", str(database), "audit"]), 0)

    def test_cli_parser_supports_bitemporal_commands(self):
        parsed = parser().parse_args(["--db", "x.sqlite3", "replay",
                                      "--knowledge", "2026-10-01T00:00:00+00:00",
                                      "--business", "2026-09-30"])
        self.assertEqual(parsed.action, "replay")
        parsed = parser().parse_args(["--db", "x.sqlite3", "withdraw-correction",
                                      "company", "batch-2", "1", "--reason", "revoked"])
        self.assertEqual(parsed.ordinal, 1)
        parsed = parser().parse_args(["--db", "x.sqlite3", "summary",
                                      "--knowledge", "2026-10-01T00:00:00+00:00"])
        self.assertEqual(parsed.action, "summary")
        parsed = parser().parse_args(["--db", "x.sqlite3", "corrections", "--source", "company"])
        self.assertEqual(parsed.action, "corrections")


if __name__ == "__main__":
    unittest.main()
