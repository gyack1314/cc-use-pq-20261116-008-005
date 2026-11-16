from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Iterator
import json
import sqlite3

from .contracts import Correction, HarvestError, Manifest, Record, Source, canonical
from .intake import manifest_digest


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS sources (
 key TEXT PRIMARY KEY, name TEXT NOT NULL, format TEXT NOT NULL,
 kind TEXT NOT NULL, field_map TEXT NOT NULL, trust INTEGER NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1, withdrawn_at TEXT
);
CREATE TABLE IF NOT EXISTS batches (
 source TEXT NOT NULL REFERENCES sources(key), batch TEXT NOT NULL,
 captured_at TEXT NOT NULL, manifest_hash TEXT NOT NULL,
 manifest_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 started_at TEXT NOT NULL, completed_at TEXT,
 PRIMARY KEY(source,batch)
);
CREATE TABLE IF NOT EXISTS pages (
 source TEXT NOT NULL, batch TEXT NOT NULL, ordinal INTEGER NOT NULL,
 path TEXT NOT NULL, digest TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0, error_code TEXT, error_detail TEXT,
 processed_at TEXT, record_count INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(source,batch,ordinal),
 FOREIGN KEY(source,batch) REFERENCES batches(source,batch)
);
CREATE TABLE IF NOT EXISTS observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, batch TEXT NOT NULL,
 ordinal INTEGER NOT NULL, record_id TEXT NOT NULL, identity TEXT NOT NULL,
 record_json TEXT NOT NULL, raw_json TEXT NOT NULL, raw_digest TEXT NOT NULL,
 status TEXT NOT NULL, reasons TEXT NOT NULL, version INTEGER NOT NULL,
 captured_at TEXT NOT NULL, observed_at TEXT NOT NULL, effective_at TEXT,
 UNIQUE(source,batch,ordinal,record_id),
 FOREIGN KEY(source,batch,ordinal) REFERENCES pages(source,batch,ordinal)
);
CREATE INDEX IF NOT EXISTS observations_identity ON observations(identity);
CREATE INDEX IF NOT EXISTS observations_source_record ON observations(source,record_id,version);
CREATE TABLE IF NOT EXISTS source_records (
 source TEXT NOT NULL REFERENCES sources(key), record_id TEXT NOT NULL,
 version INTEGER NOT NULL, latest_observation INTEGER NOT NULL REFERENCES observations(id),
 PRIMARY KEY(source,record_id)
);
CREATE TABLE IF NOT EXISTS corrections (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source TEXT NOT NULL, batch TEXT NOT NULL, ordinal INTEGER NOT NULL,
 record_id TEXT NOT NULL, target_record_id TEXT NOT NULL, target_version INTEGER NOT NULL,
 effective_at TEXT NOT NULL, registered_at TEXT NOT NULL, withdrawn_at TEXT,
 FOREIGN KEY(source,batch,ordinal) REFERENCES pages(source,batch,ordinal)
);
CREATE INDEX IF NOT EXISTS corrections_page ON corrections(source,batch,ordinal);
CREATE INDEX IF NOT EXISTS corrections_target ON corrections(source,target_record_id,target_version);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
 source TEXT, batch TEXT, ordinal INTEGER, payload TEXT NOT NULL,
 occurred_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Ledger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = Row
        self.db.executescript(SCHEMA)
        self.db.execute("PRAGMA busy_timeout=30000")
        self._migrate()

    def _migrate(self) -> None:
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(observations)")}
        if "effective_at" not in columns:
            self.db.execute("ALTER TABLE observations ADD COLUMN effective_at TEXT")
            for row in self.db.execute("SELECT id,record_json FROM observations").fetchall():
                published = json.loads(row["record_json"])["published_at"]
                self.db.execute("UPDATE observations SET effective_at=? WHERE id=?",
                                (published, row["id"]))
            self.db.commit()

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def event(self, kind: str, source: str | None, batch: str | None,
              ordinal: int | None, payload: dict) -> None:
        self.db.execute("INSERT INTO events(event_type,source,batch,ordinal,payload,occurred_at) VALUES(?,?,?,?,?,?)",
                        (kind, source, batch, ordinal, canonical(payload), utc_now()))

    def register(self, source: Source) -> dict:
        with self.transaction():
            old = self.db.execute("SELECT * FROM sources WHERE key=?", (source.key,)).fetchone()
            if old:
                if old["withdrawn_at"]:
                    raise HarvestError("source_withdrawn", source.key)
                old_document = {"key": old["key"], "name": old["name"], "format": old["format"],
                                "kind": old["kind"], "field_map": json.loads(old["field_map"]),
                                "trust": old["trust"]}
                if old_document == source.document():
                    return {"source": source.key, "revision": old["revision"], "unchanged": True}
                count = self.db.execute("SELECT count(*) FROM batches WHERE source=?", (source.key,)).fetchone()[0]
                if count and (old["format"] != source.format or old["kind"] != source.kind
                              or json.loads(old["field_map"]) != source.field_map):
                    raise HarvestError("source_contract_frozen", "format, kind, and field map cannot change after intake")
                revision = old["revision"] + 1
                self.db.execute("UPDATE sources SET name=?,trust=?,revision=? WHERE key=?",
                                (source.name, source.trust, revision, source.key))
                self.event("source_revised", source.key, None, None, {"revision": revision})
                return {"source": source.key, "revision": revision, "unchanged": False}
            self.db.execute("INSERT INTO sources(key,name,format,kind,field_map,trust) VALUES(?,?,?,?,?,?)",
                            (source.key, source.name, source.format, source.kind,
                             canonical(source.field_map), source.trust))
            self.event("source_registered", source.key, None, None, source.document())
            return {"source": source.key, "revision": 1, "unchanged": False}

    def source(self, key: str) -> Source:
        row = self.db.execute("SELECT * FROM sources WHERE key=?", (key,)).fetchone()
        if row is None:
            raise HarvestError("source_unknown", key)
        if row["withdrawn_at"]:
            raise HarvestError("source_withdrawn", key)
        return Source(row["key"], row["name"], row["format"], row["kind"],
                      json.loads(row["field_map"]), row["trust"])

    def ensure_batch(self, manifest: Manifest) -> dict:
        self.source(manifest.source)
        digest = manifest_digest(manifest)
        with self.transaction():
            existing = self.db.execute("SELECT * FROM batches WHERE source=? AND batch=?",
                                       (manifest.source, manifest.batch)).fetchone()
            if existing:
                if existing["manifest_hash"] != digest:
                    raise HarvestError("batch_immutable", "batch identifier already has different manifest")
                return self.batch(manifest.source, manifest.batch)
            self.db.execute("INSERT INTO batches(source,batch,captured_at,manifest_hash,manifest_json,started_at) VALUES(?,?,?,?,?,?)",
                            (manifest.source, manifest.batch, manifest.captured_at, digest,
                             canonical(manifest.document()), utc_now()))
            self.db.executemany("INSERT INTO pages(source,batch,ordinal,path,digest) VALUES(?,?,?,?,?)",
                                [(manifest.source, manifest.batch, p.ordinal, p.path, p.sha256)
                                 for p in manifest.pages])
            self.event("batch_registered", manifest.source, manifest.batch, None,
                       {"manifest_hash": digest, "page_count": len(manifest.pages)})
        return self.batch(manifest.source, manifest.batch)

    def batch(self, source: str, batch: str) -> dict:
        row = self.db.execute("SELECT * FROM batches WHERE source=? AND batch=?", (source, batch)).fetchone()
        if row is None:
            raise HarvestError("batch_unknown", f"{source}/{batch}")
        pages = [dict(p) for p in self.db.execute("SELECT * FROM pages WHERE source=? AND batch=? ORDER BY ordinal",
                                                 (source, batch))]
        return {"source": source, "batch": batch, "captured_at": row["captured_at"],
                "manifest_hash": row["manifest_hash"], "state": row["state"],
                "pages": pages, "completed_at": row["completed_at"]}

    def page(self, source: str, batch: str, ordinal: int) -> dict:
        row = self.db.execute("SELECT * FROM pages WHERE source=? AND batch=? AND ordinal=?",
                              (source, batch, ordinal)).fetchone()
        if row is None:
            raise HarvestError("page_unknown", f"{source}/{batch}/{ordinal}")
        return dict(row)

    def fail_page(self, source: str, batch: str, ordinal: int, error: HarvestError) -> None:
        with self.transaction():
            row = self.page(source, batch, ordinal)
            if row["state"] == "complete":
                return
            self.db.execute("UPDATE pages SET state='failed',attempts=attempts+1,error_code=?,error_detail=? WHERE source=? AND batch=? AND ordinal=?",
                            (error.code, error.detail, source, batch, ordinal))
            self.db.execute("UPDATE batches SET state='failed' WHERE source=? AND batch=?", (source, batch))
            self.event("page_failed", source, batch, ordinal, {"code": error.code, "detail": error.detail})

    def commit_page(self, source: str, batch: str, ordinal: int,
                    items: list[tuple[Record, dict, str, tuple[str, ...]]],
                    correction: Correction | None = None) -> dict:
        with self.transaction():
            row = self.page(source, batch, ordinal)
            if row["state"] == "complete":
                return {"ordinal": ordinal, "unchanged": True, "records": row["record_count"]}
            if correction is not None:
                self._validate_correction(source, items, correction)
            captured = self.db.execute("SELECT captured_at FROM batches WHERE source=? AND batch=?",
                                       (source, batch)).fetchone()[0]
            arrived = utc_now()
            for record, raw, status, reasons in items:
                previous = self.db.execute("SELECT version FROM source_records WHERE source=? AND record_id=?",
                                           (source, record.source_record_id)).fetchone()
                version = previous[0] + 1 if previous else 1
                effective = correction.effective_at if correction is not None else record.published_at
                cursor = self.db.execute("INSERT INTO observations(source,batch,ordinal,record_id,identity,record_json,raw_json,raw_digest,status,reasons,version,captured_at,observed_at,effective_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                         (source, batch, ordinal, record.source_record_id, record.identity,
                                          canonical(record.document()), canonical(raw), record.raw_digest,
                                          status, canonical(reasons), version, captured, arrived, effective))
                self.db.execute("INSERT INTO source_records(source,record_id,version,latest_observation) VALUES(?,?,?,?) ON CONFLICT(source,record_id) DO UPDATE SET version=excluded.version,latest_observation=excluded.latest_observation",
                                (source, record.source_record_id, version, cursor.lastrowid))
            if correction is not None:
                page_records = [record.source_record_id for record, _, _, _ in items] or [""]
                for record_id in page_records:
                    for target in correction.supersedes:
                        self.db.execute("INSERT INTO corrections(source,batch,ordinal,record_id,target_record_id,target_version,effective_at,registered_at) VALUES(?,?,?,?,?,?,?,?)",
                                        (source, batch, ordinal, record_id, target.record_id,
                                         target.version, correction.effective_at, arrived))
                self.event("correction_registered", source, batch, ordinal,
                           {"effective_at": correction.effective_at,
                            "records": [item for item in page_records if item],
                            "supersedes": [target.document() for target in correction.supersedes]})
            self.db.execute("UPDATE pages SET state='complete',attempts=attempts+1,error_code=NULL,error_detail=NULL,processed_at=?,record_count=? WHERE source=? AND batch=? AND ordinal=?",
                            (arrived, len(items), source, batch, ordinal))
            self.event("page_committed", source, batch, ordinal, {"records": len(items)})
            incomplete = self.db.execute("SELECT count(*) FROM pages WHERE source=? AND batch=? AND state!='complete'",
                                         (source, batch)).fetchone()[0]
            if incomplete == 0:
                self.db.execute("UPDATE batches SET state='complete',completed_at=? WHERE source=? AND batch=?",
                                (arrived, source, batch))
                self.event("batch_completed", source, batch, None, {"records_last_page": len(items)})
            else:
                failed = self.db.execute("SELECT count(*) FROM pages WHERE source=? AND batch=? AND state='failed'",
                                         (source, batch)).fetchone()[0]
                self.db.execute("UPDATE batches SET state=? WHERE source=? AND batch=?",
                                ("failed" if failed else "pending", source, batch))
            return {"ordinal": ordinal, "unchanged": False, "records": len(items)}

    def _validate_correction(self, source: str,
                             items: list[tuple[Record, dict, str, tuple[str, ...]]],
                             correction: Correction) -> None:
        for target in correction.supersedes:
            if target.source is not None and target.source != source:
                raise HarvestError("correction_cross_source",
                                   f"superseded record {target.source}:{target.record_id} is not from source {source}")
            known = self.db.execute("SELECT 1 FROM observations WHERE source=? AND record_id=? AND version=? LIMIT 1",
                                    (source, target.record_id, target.version)).fetchone()
            if known is None:
                raise HarvestError("correction_target_unknown",
                                   f"{source}:{target.record_id} version {target.version} is not registered")
        new_edges = [(record.source_record_id, target.record_id)
                     for record, _, _, _ in items for target in correction.supersedes]
        if self._correction_cycle(source, new_edges):
            raise HarvestError("correction_cycle",
                               "correction would create a cycle in the registered replacement chain")

    def _correction_cycle(self, source: str, new_edges: list[tuple[str, str]]) -> bool:
        graph: dict[str, set[str]] = {}
        rows = self.db.execute("SELECT record_id,target_record_id FROM corrections WHERE source=? AND withdrawn_at IS NULL",
                               (source,)).fetchall()
        for start, end in list(rows) + new_edges:
            if start and start != end:
                graph.setdefault(start, set()).add(end)
        state: dict[str, int] = {}
        for root in graph:
            if state.get(root, 0) != 0:
                continue
            state[root] = 1
            stack = [(root, iter(sorted(graph.get(root, ()))))]
            while stack:
                node, edges = stack[-1]
                descended = False
                for nxt in edges:
                    mark = state.get(nxt, 0)
                    if mark == 1:
                        return True
                    if mark == 0:
                        state[nxt] = 1
                        stack.append((nxt, iter(sorted(graph.get(nxt, ())))))
                        descended = True
                        break
                if not descended:
                    state[node] = 2
                    stack.pop()
        return False

    def corrections(self, *, source: str | None = None) -> list[dict]:
        if source is None:
            rows = self.db.execute("SELECT * FROM corrections ORDER BY id").fetchall()
        else:
            rows = self.db.execute("SELECT * FROM corrections WHERE source=? ORDER BY id", (source,)).fetchall()
        return [dict(row) for row in rows]

    def withdraw_correction(self, source: str, batch: str, ordinal: int, reason: str) -> dict:
        with self.transaction():
            rows = self.db.execute("SELECT * FROM corrections WHERE source=? AND batch=? AND ordinal=? ORDER BY id",
                                   (source, batch, ordinal)).fetchall()
            if not rows:
                raise HarvestError("correction_unknown", f"{source}/{batch}/{ordinal} has no registered correction")
            first = rows[0]
            if first["withdrawn_at"]:
                return {"source": source, "batch": batch, "ordinal": ordinal,
                        "withdrawn_at": first["withdrawn_at"], "unchanged": True}
            at = utc_now()
            self.db.execute("UPDATE corrections SET withdrawn_at=? WHERE source=? AND batch=? AND ordinal=?",
                            (at, source, batch, ordinal))
            targets = sorted({(row["target_record_id"], row["target_version"]) for row in rows})
            self.event("correction_withdrawn", source, batch, ordinal,
                       {"reason": reason, "effective_at": first["effective_at"],
                        "supersedes": [{"record_id": record_id, "version": version}
                                       for record_id, version in targets]})
            return {"source": source, "batch": batch, "ordinal": ordinal,
                    "withdrawn_at": at, "unchanged": False}

    def withdraw(self, source: str, reason: str) -> dict:
        with self.transaction():
            row = self.db.execute("SELECT withdrawn_at FROM sources WHERE key=?", (source,)).fetchone()
            if row is None:
                raise HarvestError("source_unknown", source)
            if row[0]:
                return {"source": source, "withdrawn_at": row[0], "unchanged": True}
            at = utc_now()
            self.db.execute("UPDATE sources SET withdrawn_at=? WHERE key=?", (at, source))
            self.event("source_withdrawn", source, None, None, {"reason": reason})
            return {"source": source, "withdrawn_at": at, "unchanged": False}

    def observations(self, *, source: str | None = None, identity: str | None = None) -> list[dict]:
        conditions = []
        params = []
        if source is not None:
            conditions.append("o.source=?")
            params.append(source)
        if identity is not None:
            conditions.append("o.identity=?")
            params.append(identity)
        clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = self.db.execute("SELECT o.*,s.trust,s.withdrawn_at FROM observations o JOIN sources s ON s.key=o.source"
                               + clause + " ORDER BY o.id", params)
        return [{**dict(row), "record": json.loads(row["record_json"]),
                 "raw": json.loads(row["raw_json"]), "reasons": json.loads(row["reasons"])} for row in rows]

    def events(self, *, source: str | None = None) -> list[dict]:
        if source is None:
            rows = self.db.execute("SELECT * FROM events ORDER BY id")
        else:
            rows = self.db.execute("SELECT * FROM events WHERE source=? ORDER BY id", (source,))
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
