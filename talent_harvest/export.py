from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from pathlib import Path
from typing import Any
import json

from .contracts import HarvestError, business_date, canonical, utc_instant
from .ledger import Ledger, utc_now
from .reconcile import reconcile_group


def _latest_per_source(rows: list[dict]) -> list[dict]:
    latest: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = row["source"], row["record_id"]
        previous = latest.get(key)
        if previous is None or row["id"] > previous["id"]:
            latest[key] = row
    return list(latest.values())


def _project(row: dict) -> dict:
    return {"source": row["source"], "source_record_id": row["record_id"],
            "batch": row["batch"], "page": row["ordinal"], "version": row["version"],
            "captured_at": row["captured_at"], "observed_at": row["observed_at"],
            "effective_at": row["effective_at"],
            "status": row["status"], "reasons": row["reasons"], "trust": row["trust"],
            "record": row["record"], "raw_digest": row["raw_digest"]}


def _correction_ref(correction: dict) -> dict:
    return {"source": correction["source"], "batch": correction["batch"],
            "ordinal": correction["ordinal"]}


class Delivery:
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    def _select(self, *, knowledge: str, business: str | None) -> tuple[list[dict], list[tuple[dict, str, dict]], list[dict]]:
        rows = self.ledger.observations()
        corrections = self.ledger.corrections()
        active = [item for item in corrections if item["registered_at"] <= knowledge
                  and not (item["withdrawn_at"] and item["withdrawn_at"] <= knowledge)]
        superseded: dict[tuple[str, str, int], dict] = {}
        for item in active:
            if business is not None and item["effective_at"] > business:
                continue
            key = (item["source"], item["target_record_id"], item["target_version"])
            previous = superseded.get(key)
            if previous is None or item["id"] > previous["id"]:
                superseded[key] = item
        page_correction = {}
        for item in corrections:
            page_correction.setdefault((item["source"], item["batch"], item["ordinal"]), item)
        pool: list[dict] = []
        excluded: list[tuple[dict, str, dict]] = []
        for row in rows:
            if row["observed_at"] > knowledge:
                excluded.append((row, "not_known_at_cutoff",
                                 {"observed_at": row["observed_at"], "knowledge": knowledge}))
                continue
            if row["withdrawn_at"] and row["withdrawn_at"] <= knowledge:
                excluded.append((row, "source_withdrawn", {"withdrawn_at": row["withdrawn_at"]}))
                continue
            correction = page_correction.get((row["source"], row["batch"], row["ordinal"]))
            if correction and correction["withdrawn_at"] and correction["withdrawn_at"] <= knowledge:
                excluded.append((row, "correction_withdrawn",
                                 {"withdrawn_at": correction["withdrawn_at"],
                                  "correction": _correction_ref(correction)}))
                continue
            replaced = superseded.get((row["source"], row["record_id"], row["version"]))
            if replaced is not None:
                excluded.append((row, "superseded_by_correction",
                                 {"correction": _correction_ref(replaced),
                                  "effective_at": replaced["effective_at"]}))
                continue
            pool.append(row)
        chosen: list[dict] = []
        by_record: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for row in pool:
            by_record[(row["source"], row["record_id"])].append(row)
        for key in sorted(by_record):
            versions = by_record[key]
            if business is None:
                versions.sort(key=lambda item: item["id"])
                keep = versions[-1]
                chosen.append(keep)
                for old in versions[:-1]:
                    excluded.append((old, "superseded_by_version",
                                     {"selected_version": keep["version"]}))
            else:
                eligible = sorted((item for item in versions if item["effective_at"] <= business),
                                  key=lambda item: (item["version"], item["id"]))
                for item in versions:
                    if item["effective_at"] > business:
                        excluded.append((item, "not_yet_effective",
                                         {"effective_at": item["effective_at"], "business_date": business}))
                if eligible:
                    keep = eligible[-1]
                    chosen.append(keep)
                    for old in eligible[:-1]:
                        excluded.append((old, "superseded_by_version",
                                         {"selected_version": keep["version"]}))
        return chosen, excluded, corrections

    def _render(self, chosen: list[dict], include_quarantine: bool) -> dict:
        groups: dict[str, list[dict]] = defaultdict(list)
        for row in chosen:
            groups[row["identity"]].append(row)
        accepted: list[dict] = []
        conflicts: list[dict] = []
        quarantined: list[dict] = []
        for identity in sorted(groups):
            group = groups[identity]
            rejected = [row for row in group if row["status"] != "accepted"]
            if include_quarantine:
                quarantined.extend(_project(row) for row in rejected)
            status, reason, winner = reconcile_group(group)
            if status == "accepted" and winner:
                accepted.append({"identity": identity, "record": winner["record"],
                                 "selected": _project(winner),
                                 "evidence": [_project(item) for item in sorted(group, key=lambda x: (x["source"], x["record_id"]))
                                              if item["status"] == "accepted"]})
            elif status == "conflict":
                conflicts.append({"identity": identity, "reason": reason,
                                  "evidence": [_project(item) for item in group if item["status"] == "accepted"]})
        return {"accepted": accepted, "conflicts": conflicts,
                "quarantined": quarantined if include_quarantine else [],
                "counts": {"accepted": len(accepted), "conflicts": len(conflicts),
                           "quarantined": len(quarantined) if include_quarantine else 0}}

    def current(self, *, include_quarantine: bool = False) -> dict:
        chosen, _, _ = self._select(knowledge=utc_now(), business=None)
        return {"schema_version": 1, **self._render(chosen, include_quarantine)}

    def replay(self, *, knowledge: str | None = None, business: str | None = None,
               include_quarantine: bool = False) -> dict:
        cutoff = utc_instant(knowledge, "knowledge") if knowledge is not None else utc_now()
        day = business_date(business, "business") if business is not None else None
        chosen, excluded, corrections = self._select(knowledge=cutoff, business=day)
        rendered = self._render(chosen, include_quarantine)
        rendered["counts"]["excluded"] = len(excluded)
        return {"schema_version": 1, "knowledge": cutoff, "business_date": day, **rendered,
                "excluded": [{**_project(row), "identity": row["identity"],
                              "excluded_reason": reason, "detail": detail}
                             for row, reason, detail in excluded],
                "explanations": self._explanations(corrections, cutoff, day, excluded)}

    @staticmethod
    def _explanations(corrections: list[dict], knowledge: str, business: str | None,
                      excluded: list[tuple[dict, str, dict]]) -> list[dict]:
        notes: list[dict] = []
        by_page: dict[tuple[str, str, int], list[dict]] = {}
        for item in corrections:
            if item["registered_at"] <= knowledge:
                by_page.setdefault((item["source"], item["batch"], item["ordinal"]), []).append(item)
        for key in sorted(by_page, key=lambda item: min(row["id"] for row in by_page[item])):
            rows = by_page[key]
            first = rows[0]
            targets = sorted({(row["target_record_id"], row["target_version"]) for row in rows})
            supersedes = [{"record_id": record_id, "version": version}
                          for record_id, version in targets]
            base = {"source": first["source"], "batch": first["batch"], "ordinal": first["ordinal"],
                    "effective_at": first["effective_at"], "registered_at": first["registered_at"],
                    "supersedes": supersedes}
            if first["withdrawn_at"] and first["withdrawn_at"] <= knowledge:
                notes.append({**base, "type": "correction_withdrawn",
                              "withdrawn_at": first["withdrawn_at"], "restored": supersedes,
                              "note": "correction withdrawn before the knowledge cutoff; "
                                      "superseded versions are restored and its own observations are excluded"})
            else:
                applies = business is None or first["effective_at"] <= business
                notes.append({**base, "type": "correction_applied", "affects_view": applies,
                              "note": "correction registered before the knowledge cutoff; "
                                      "it supersedes the listed versions"})
        late = sum(1 for _, reason, _ in excluded if reason == "not_known_at_cutoff")
        if late:
            notes.append({"type": "knowledge_cutoff", "knowledge": knowledge,
                          "excluded_observations": late,
                          "note": "observations arriving after the knowledge cutoff are excluded"})
        return notes

    def history(self, *, source: str | None = None, identity: str | None = None) -> dict:
        rows = self.ledger.observations(source=source, identity=identity)
        return {"schema_version": 1, "observations":
                [{**_project(row), "identity": row["identity"], "raw": row["raw"],
                  "withdrawn_at": row["withdrawn_at"]} for row in rows]}

    def trace(self, identity: str) -> dict:
        if len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
            raise HarvestError("identity", "identity must be sha256 hex")
        history = self.history(identity=identity)["observations"]
        if not history:
            raise HarvestError("identity_unknown", identity)
        sources = sorted({row["source"] for row in history})
        events = [event for event in self.ledger.events() if event["source"] in sources]
        current = self.current(include_quarantine=True)
        matching = [item for section in ("accepted", "conflicts") for item in current[section]
                    if item["identity"] == identity]
        return {"identity": identity, "history": history, "events": events,
                "current": matching[0] if matching else None}

    @staticmethod
    def _aggregate(accepted: list[dict]) -> dict:
        by_kind: dict[str, dict[str, int]] = defaultdict(lambda: {"entities": 0, "quantity": 0})
        by_region: dict[str, dict[str, int]] = defaultdict(lambda: {"entities": 0, "quantity": 0})
        for entity in accepted:
            record = entity["record"]
            by_kind[record["kind"]]["entities"] += 1
            by_kind[record["kind"]]["quantity"] += record["quantity"]
            by_region[record["region"]]["entities"] += 1
            by_region[record["region"]]["quantity"] += record["quantity"]
        return {"by_kind": dict(sorted(by_kind.items())),
                "by_region": dict(sorted(by_region.items()))}

    def summary(self, *, knowledge: str | None = None, business: str | None = None) -> dict:
        if knowledge is None and business is None:
            current = self.current(include_quarantine=True)
            return {"counts": current["counts"], **self._aggregate(current["accepted"])}
        view = self.replay(knowledge=knowledge, business=business, include_quarantine=True)
        return {"knowledge": view["knowledge"], "business_date": view["business_date"],
                "counts": view["counts"], **self._aggregate(view["accepted"])}

    def write_snapshot(self, destination: str | Path, *, include_quarantine: bool = False) -> dict:
        path = Path(destination)
        payload = self.current(include_quarantine=include_quarantine)
        data = canonical(payload).encode("utf-8")
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_bytes(data)
        temp.replace(path)
        return {"path": str(path), "sha256": sha256(data).hexdigest(),
                "bytes": len(data), "counts": payload["counts"]}


def verify_snapshot(path: str | Path, expected_sha256: str) -> dict:
    payload = Path(path).read_bytes()
    actual = sha256(payload).hexdigest()
    if actual != expected_sha256:
        raise HarvestError("snapshot_digest", f"expected {expected_sha256}, got {actual}")
    try:
        data = json.loads(payload)
    except (ValueError, UnicodeError):
        raise HarvestError("snapshot_json", "snapshot is not valid UTF-8 JSON") from None
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise HarvestError("snapshot_schema", "unexpected delivery schema")
    return {"sha256": actual, "counts": data.get("counts", {})}
