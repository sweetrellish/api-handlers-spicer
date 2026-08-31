"""SQLite persistence for the local customer portal sandbox."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class PortalStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS portal_accounts (
                    contact_id TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    name TEXT NOT NULL,
                    account_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    address TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    project_manager TEXT NOT NULL,
                    project_type TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    estimated_completion TEXT NOT NULL,
                    sale_date TEXT NOT NULL,
                    total_value REAL NOT NULL DEFAULT 0,
                    documents_visible INTEGER NOT NULL DEFAULT 1,
                    payments_visible INTEGER NOT NULL DEFAULT 1,
                    password_salt TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    password_preview TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    synced_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS portal_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def list_accounts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM portal_accounts
                ORDER BY sale_date DESC, name COLLATE NOCASE ASC
                """
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_account_by_email(self, email: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM portal_accounts WHERE lower(email) = lower(?) LIMIT 1",
                (email.strip(),),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_account_by_contact_id(self, contact_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM portal_accounts WHERE contact_id = ? LIMIT 1",
                (contact_id,),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def upsert_account(self, record: dict[str, Any]) -> None:
        payload_json = json.dumps(record["payload"], ensure_ascii=True)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO portal_accounts (
                    contact_id, email, name, account_name, phone, address, account_id,
                    role, job_id, job_name, project_manager, project_type,
                    start_date, estimated_completion, sale_date, total_value,
                    documents_visible, payments_visible,
                    password_salt, password_hash, password_preview,
                    payload_json, synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(contact_id) DO UPDATE SET
                    email = excluded.email,
                    name = excluded.name,
                    account_name = excluded.account_name,
                    phone = excluded.phone,
                    address = excluded.address,
                    account_id = excluded.account_id,
                    role = excluded.role,
                    job_id = excluded.job_id,
                    job_name = excluded.job_name,
                    project_manager = excluded.project_manager,
                    project_type = excluded.project_type,
                    start_date = excluded.start_date,
                    estimated_completion = excluded.estimated_completion,
                    sale_date = excluded.sale_date,
                    total_value = excluded.total_value,
                    documents_visible = excluded.documents_visible,
                    payments_visible = excluded.payments_visible,
                    password_salt = excluded.password_salt,
                    password_hash = excluded.password_hash,
                    password_preview = excluded.password_preview,
                    payload_json = excluded.payload_json,
                    synced_at = excluded.synced_at
                """,
                (
                    record["contact_id"],
                    record["email"],
                    record["name"],
                    record["account_name"],
                    record["phone"],
                    record["address"],
                    record["account_id"],
                    record["role"],
                    record["job_id"],
                    record["job_name"],
                    record["project_manager"],
                    record["project_type"],
                    record["start_date"],
                    record["estimated_completion"],
                    record["sale_date"],
                    float(record.get("total_value") or 0),
                    int(bool(record.get("documents_visible", True))),
                    int(bool(record.get("payments_visible", True))),
                    record["password_salt"],
                    record["password_hash"],
                    record["password_preview"],
                    payload_json,
                    record["synced_at"],
                ),
            )

    def prune_accounts(self, allowed_contact_ids: set[str]) -> int:
        with self.connect() as conn:
            existing = {
                str(row["contact_id"])
                for row in conn.execute("SELECT contact_id FROM portal_accounts").fetchall()
            }
            stale = sorted(existing - allowed_contact_ids)
            if not stale:
                return 0
            conn.executemany(
                "DELETE FROM portal_accounts WHERE contact_id = ?",
                [(contact_id,) for contact_id in stale],
            )
            return len(stale)

    def update_password(self, contact_id: str, salt: str, password_hash: str, password_preview: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE portal_accounts
                SET password_salt = ?, password_hash = ?, password_preview = ?
                WHERE contact_id = ?
                """,
                (salt, password_hash, password_preview, contact_id),
            )

    def update_visibility(self, contact_id: str, *, documents_visible: bool | None = None,
                          payments_visible: bool | None = None) -> None:
        updates: list[str] = []
        params: list[Any] = []
        if documents_visible is not None:
            updates.append("documents_visible = ?")
            params.append(int(bool(documents_visible)))
        if payments_visible is not None:
            updates.append("payments_visible = ?")
            params.append(int(bool(payments_visible)))
        if not updates:
            return
        params.append(contact_id)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE portal_accounts SET {', '.join(updates)} WHERE contact_id = ?",
                tuple(params),
            )

    def get_meta(self, key: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM portal_meta WHERE key = ? LIMIT 1",
                (key,),
            ).fetchone()
        return str(row["value"]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO portal_meta(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        payload = json.loads(str(row["payload_json"]))
        return {
            "contact_id": str(row["contact_id"]),
            "email": str(row["email"]),
            "name": str(row["name"]),
            "account_name": str(row["account_name"]),
            "phone": str(row["phone"]),
            "address": str(row["address"]),
            "account_id": str(row["account_id"]),
            "role": str(row["role"]),
            "job_id": str(row["job_id"]),
            "job_name": str(row["job_name"]),
            "project_manager": str(row["project_manager"]),
            "project_type": str(row["project_type"]),
            "start_date": str(row["start_date"]),
            "estimated_completion": str(row["estimated_completion"]),
            "sale_date": str(row["sale_date"]),
            "total_value": float(row["total_value"]),
            "documents_visible": bool(row["documents_visible"]),
            "payments_visible": bool(row["payments_visible"]),
            "password_salt": str(row["password_salt"]),
            "password_hash": str(row["password_hash"]),
            "password_preview": str(row["password_preview"]),
            "payload": payload,
            "synced_at": str(row["synced_at"]),
        }