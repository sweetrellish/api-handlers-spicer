"""Local-only customer portal sandbox API."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from flask import Blueprint, jsonify, request

from config import Config
from gclid.gclid_sync import ReportBuilder
from marketsharp_service import MarketSharpService
from portal_store import PortalStore


log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
PORTAL_DB_PATH = Path(
    os.getenv("CUSTOMER_PORTAL_DB_PATH", str(ROOT / "data" / "customer_portal.db"))
)
PORTAL_SYNC_WINDOW_DAYS = max(30, int(os.getenv("CUSTOMER_PORTAL_SYNC_WINDOW_DAYS", "90")))
PORTAL_SYNC_MAX_AGE_MINUTES = max(5, int(os.getenv("CUSTOMER_PORTAL_SYNC_MAX_AGE_MINUTES", "60")))
PORTAL_JOB_FETCH_LIMIT = max(50, int(os.getenv("CUSTOMER_PORTAL_JOB_FETCH_LIMIT", "250")))

portal_bp = Blueprint("portal_api", __name__, url_prefix="/customer-portal/api")


def _ok(message: str, data: Any | None = None, status: int = 200):
    return jsonify({"success": True, "message": message, "data": data}), status


def _err(message: str, status: int = 400, data: Any | None = None):
    payload: dict[str, Any] = {"success": False, "message": message}
    if data is not None:
        payload["data"] = data
    return jsonify(payload), status


class PortalSandboxService:
    def __init__(self):
        self.store = PortalStore(PORTAL_DB_PATH)
        self.ms_service = MarketSharpService()
        self.report_builder = ReportBuilder(self.ms_service)

    def ensure_recent_snapshot(self) -> dict[str, Any]:
        meta = self.snapshot_meta()
        accounts = self.store.list_accounts()
        if not accounts:
            return self.sync_accounts(reason="initial_load")

        last_synced = meta.get("lastSyncedAt")
        if not last_synced:
            return self.sync_accounts(reason="missing_meta")

        try:
            synced_at = datetime.fromisoformat(last_synced.replace("Z", "+00:00"))
        except ValueError:
            return self.sync_accounts(reason="bad_meta")

        if datetime.now(UTC) - synced_at > timedelta(minutes=PORTAL_SYNC_MAX_AGE_MINUTES):
            return self.sync_accounts(reason="stale_snapshot")

        return {
            "synced": False,
            "meta": meta,
            "accounts": accounts,
        }

    def sync_accounts(self, reason: str = "manual") -> dict[str, Any]:
        synced_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        cutoff = datetime.now(UTC) - timedelta(days=PORTAL_SYNC_WINDOW_DAYS)
        jobs = self._fetch_recent_jobs()
        kept_accounts: dict[str, dict[str, Any]] = {}

        for job in jobs:
            sale_dt = self._parse_job_date(job)
            if sale_dt is None or sale_dt < cutoff:
                continue

            contact_id = str(
                job.get("contactId")
                or job.get("ContactId")
                or job.get("contact_id")
                or ""
            ).strip()
            if not contact_id:
                continue

            existing = kept_accounts.get(contact_id)
            if existing and existing.get("_sale_dt") and existing["_sale_dt"] >= sale_dt:
                continue

            contact_record = self.report_builder._fetch_contact_record(contact_id) or {}
            account = self._build_account(job, contact_record, sale_dt, synced_at)
            if not account:
                continue
            account["_sale_dt"] = sale_dt
            kept_accounts[contact_id] = account

        persisted_ids = set()
        for account in kept_accounts.values():
            account.pop("_sale_dt", None)
            self.store.upsert_account(account)
            persisted_ids.add(account["contact_id"])

        pruned = self.store.prune_accounts(persisted_ids)
        self.store.set_meta("last_synced_at", synced_at)
        self.store.set_meta("last_sync_reason", reason)
        self.store.set_meta("last_sync_account_count", str(len(persisted_ids)))
        self.store.set_meta("last_sync_pruned_count", str(pruned))

        return {
            "synced": True,
            "meta": self.snapshot_meta(),
            "accounts": self.store.list_accounts(),
        }

    def snapshot_meta(self) -> dict[str, Any]:
        return {
            "lastSyncedAt": self.store.get_meta("last_synced_at") or "",
            "lastSyncReason": self.store.get_meta("last_sync_reason") or "",
            "accountCount": int(self.store.get_meta("last_sync_account_count") or "0"),
            "prunedCount": int(self.store.get_meta("last_sync_pruned_count") or "0"),
            "windowDays": PORTAL_SYNC_WINDOW_DAYS,
            "localOnly": True,
        }

    def login(self, email: str, password: str) -> dict[str, Any] | None:
        account = self.store.get_account_by_email(email)
        if not account:
            return None
        if not self._verify_password(password, account["password_salt"], account["password_hash"]):
            return None
        return self._session_payload(account)

    def reset_password(self, contact_id: str) -> dict[str, Any]:
        account = self.store.get_account_by_contact_id(contact_id)
        if not account:
            raise ValueError("Account not found.")
        password = self._generate_password()
        salt, password_hash = self._hash_password(password)
        self.store.update_password(contact_id, salt, password_hash, password)
        refreshed = self.store.get_account_by_contact_id(contact_id)
        if not refreshed:
            raise ValueError("Account refresh failed after password reset.")
        return self._account_preview(refreshed)

    def update_visibility(self, contact_id: str, *, documents_visible: bool | None = None,
                          payments_visible: bool | None = None) -> dict[str, Any]:
        self.store.update_visibility(
            contact_id,
            documents_visible=documents_visible,
            payments_visible=payments_visible,
        )
        account = self.store.get_account_by_contact_id(contact_id)
        if not account:
            raise ValueError("Account not found.")
        payload = account["payload"]
        payload.setdefault("settings", {})
        payload["settings"]["documentsVisible"] = account["documents_visible"]
        payload["settings"]["paymentsVisible"] = account["payments_visible"]
        account["payload"] = payload
        self.store.upsert_account(account)
        return self._session_payload(account)

    def impersonate(self, contact_id: str) -> dict[str, Any]:
        account = self.store.get_account_by_contact_id(contact_id)
        if not account:
            raise ValueError("Account not found.")
        return self._session_payload(account)

    def public_preview(self) -> dict[str, Any]:
        snapshot = self.ensure_recent_snapshot()
        accounts = [self._account_preview(account) for account in snapshot["accounts"]]
        return {
            "meta": snapshot["meta"],
            "accounts": accounts,
            "warning": "Local sandbox only. Passwords are intentionally visible for localhost testing.",
        }

    def admin_snapshot(self) -> dict[str, Any]:
        snapshot = self.ensure_recent_snapshot()
        accounts = [
            {
                **self._account_preview(account),
                "documentsVisible": account["documents_visible"],
                "paymentsVisible": account["payments_visible"],
                "jobId": account["job_id"],
                "projectManager": account["project_manager"],
                "projectType": account["project_type"],
                "saleDate": account["sale_date"],
                "syncedAt": account["synced_at"],
            }
            for account in snapshot["accounts"]
        ]
        return {"meta": snapshot["meta"], "accounts": accounts}

    def _fetch_recent_jobs(self) -> list[dict[str, Any]]:
        base_url = self.ms_service.odata_url.rstrip("/")
        headers = self.ms_service._odata_headers()
        entities = ("Jobs", "Job", "SoldJobs")
        param_sets = (
            {"$expand": "Contract", "$top": str(PORTAL_JOB_FETCH_LIMIT)},
            {"$top": str(PORTAL_JOB_FETCH_LIMIT)},
        )

        for entity in entities:
            for params in param_sets:
                try:
                    response = requests.get(
                        f"{base_url}/{entity}",
                        headers=headers,
                        params=params,
                        timeout=20,
                    )
                    if response.status_code == 404:
                        break
                    if response.status_code != 200:
                        continue
                    raw = response.json().get("d", {})
                    jobs = raw.get("results", raw) if isinstance(raw, dict) else raw
                    if isinstance(jobs, list):
                        return jobs
                except Exception as exc:
                    log.warning("Portal sandbox job fetch failed for %s: %s", entity, exc)
        return []

    def _build_account(self, job: dict[str, Any], contact_record: dict[str, Any],
                       sale_dt: datetime, synced_at: str) -> dict[str, Any] | None:
        contact_id = str(
            job.get("contactId") or job.get("ContactId") or contact_record.get("id") or ""
        ).strip()
        if not contact_id:
            return None

        email, phone = self.report_builder._contact_email_phone(contact_record)
        if not email:
            return None

        name = self._contact_name(contact_record)
        account_name = str(contact_record.get("businessName") or name).strip() or email
        address = self._contact_address(contact_record)
        account_id = self._account_id(contact_record, contact_id)
        job_id = str(job.get("id") or job.get("Id") or job.get("jobId") or contact_id).strip()
        job_name = self._job_name(job, account_name)
        project_manager = self._project_manager(job)
        project_type = self._project_type(job)
        total_value = self._job_amount(job)
        appointments = self.report_builder.appointments_for_contact(contact_id)
        start_date = self._start_date(job, sale_dt, appointments)
        estimated_completion = self._estimated_completion(job, sale_dt, appointments)
        existing = self.store.get_account_by_contact_id(contact_id)

        if existing:
            password_salt = existing["password_salt"]
            password_hash = existing["password_hash"]
            password_preview = existing["password_preview"]
            documents_visible = existing["documents_visible"]
            payments_visible = existing["payments_visible"]
        else:
            password_preview = self._generate_password()
            password_salt, password_hash = self._hash_password(password_preview)
            documents_visible = True
            payments_visible = True

        payload = {
            "user": {
                "id": contact_id,
                "name": name,
                "email": email,
                "role": "owner",
                "accountId": account_id,
                "accountName": account_name,
                "phone": phone,
                "address": address,
            },
            "job": {
                "id": job_id,
                "name": job_name,
                "projectManager": project_manager,
                "startDate": start_date,
                "estimatedCompletion": estimated_completion,
                "type": project_type,
                "totalValue": total_value,
                "milestones": self._milestones(sale_dt, appointments, project_manager, estimated_completion),
                "documents": [] if documents_visible else [],
                "payments": [] if payments_visible else [],
            },
            "settings": {
                "documentsVisible": documents_visible,
                "paymentsVisible": payments_visible,
            },
            "sandbox": {
                "saleDate": sale_dt.date().isoformat(),
                "passwordPreview": password_preview,
                "localOnly": True,
            },
        }

        return {
            "contact_id": contact_id,
            "email": email,
            "name": name,
            "account_name": account_name,
            "phone": phone,
            "address": address,
            "account_id": account_id,
            "role": "owner",
            "job_id": job_id,
            "job_name": job_name,
            "project_manager": project_manager,
            "project_type": project_type,
            "start_date": start_date,
            "estimated_completion": estimated_completion,
            "sale_date": sale_dt.date().isoformat(),
            "total_value": total_value,
            "documents_visible": documents_visible,
            "payments_visible": payments_visible,
            "password_salt": password_salt,
            "password_hash": password_hash,
            "password_preview": password_preview,
            "payload": payload,
            "synced_at": synced_at,
        }

    def _session_payload(self, account: dict[str, Any]) -> dict[str, Any]:
        payload = dict(account["payload"])
        payload["sandbox"] = {
            **payload.get("sandbox", {}),
            "passwordPreview": account["password_preview"],
            "localOnly": True,
        }
        payload["settings"] = {
            "documentsVisible": account["documents_visible"],
            "paymentsVisible": account["payments_visible"],
        }
        return payload

    @staticmethod
    def _account_preview(account: dict[str, Any]) -> dict[str, Any]:
        return {
            "contactId": account["contact_id"],
            "email": account["email"],
            "name": account["name"],
            "accountName": account["account_name"],
            "accountId": account["account_id"],
            "password": account["password_preview"],
            "jobName": account["job_name"],
            "saleDate": account["sale_date"],
        }

    @staticmethod
    def _parse_job_date(job: dict[str, Any]) -> datetime | None:
        for key in ("saleDate", "SaleDate", "contractDate", "ContractDate"):
            value = job.get(key)
            parsed = PortalSandboxService._parse_datetime(value)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        for candidate in (text, text.replace("Z", "+00:00"), text.split("T", 1)[0]):
            try:
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=UTC)
                return parsed.astimezone(UTC)
            except ValueError:
                continue
        return None

    @staticmethod
    def _contact_name(contact_record: dict[str, Any]) -> str:
        first = str(contact_record.get("firstName") or contact_record.get("FirstName") or "").strip()
        last = str(contact_record.get("lastName") or contact_record.get("LastName") or "").strip()
        business = str(contact_record.get("businessName") or contact_record.get("BusinessName") or "").strip()
        full = f"{first} {last}".strip()
        return full or business or "Unknown Contact"

    @staticmethod
    def _contact_address(contact_record: dict[str, Any]) -> str:
        parts = [
            str(contact_record.get("address1") or contact_record.get("Address1") or "").strip(),
            str(contact_record.get("city") or contact_record.get("City") or "").strip(),
            str(contact_record.get("state") or contact_record.get("State") or "").strip(),
            str(
                contact_record.get("postalCode")
                or contact_record.get("zipCode")
                or contact_record.get("PostalCode")
                or contact_record.get("ZipCode")
                or ""
            ).strip(),
        ]
        return ", ".join([part for part in parts if part]) or "Address unavailable"

    @staticmethod
    def _account_id(contact_record: dict[str, Any], contact_id: str) -> str:
        explicit = str(
            contact_record.get("displayId")
            or contact_record.get("DisplayId")
            or contact_record.get("customerNumber")
            or contact_record.get("CustomerNumber")
            or ""
        ).strip()
        if explicit:
            return explicit
        suffix = contact_id.replace("-", "")[-6:].upper()
        return f"MS-{suffix}"

    @staticmethod
    def _job_name(job: dict[str, Any], fallback: str) -> str:
        for key in ("name", "Name", "jobName", "JobName", "description", "Description"):
            value = str(job.get(key) or "").strip()
            if value:
                return value
        return f"{fallback} Project"

    @staticmethod
    def _project_manager(job: dict[str, Any]) -> str:
        for key in (
            "projectManager", "ProjectManager", "projectManagerName", "ProjectManagerName",
            "assignedToName", "AssignedToName", "salesRepName", "SalesRepName",
        ):
            value = str(job.get(key) or "").strip()
            if value:
                return value
        return "Spicer Bros. Team"

    @staticmethod
    def _project_type(job: dict[str, Any]) -> str:
        for key in ("jobType", "JobType", "type", "Type", "category", "Category"):
            value = str(job.get(key) or "").strip()
            if value:
                return value
        return "In Progress"

    def _job_amount(self, job: dict[str, Any]) -> float:
        contract = job.get("Contract")
        if isinstance(contract, dict):
            contract_obj = contract.get("results", contract)
            if isinstance(contract_obj, list) and contract_obj:
                raw = self.report_builder._pick_amount(contract_obj[0])
            else:
                raw = self.report_builder._pick_amount(contract_obj)
            parsed = self._to_float(raw)
            if parsed is not None:
                return parsed

        raw = self.report_builder._pick_amount(job)
        parsed = self._to_float(raw)
        return parsed if parsed is not None else 0.0

    @staticmethod
    def _to_float(value: Any) -> float | None:
        text = str(value or "").replace(",", "").replace("$", "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _start_date(self, job: dict[str, Any], sale_dt: datetime, appointments: list[dict[str, Any]]) -> str:
        for key in ("startDate", "StartDate"):
            parsed = self._parse_datetime(job.get(key))
            if parsed is not None:
                return parsed.date().isoformat()
        appt_dates = [self._parse_appointment_date(item) for item in appointments]
        appt_dates = [item for item in appt_dates if item is not None]
        if appt_dates:
            return min(appt_dates).date().isoformat()
        return sale_dt.date().isoformat()

    def _estimated_completion(self, job: dict[str, Any], sale_dt: datetime,
                              appointments: list[dict[str, Any]]) -> str:
        for key in ("completionDate", "CompletionDate", "endDate", "EndDate"):
            parsed = self._parse_datetime(job.get(key))
            if parsed is not None:
                return parsed.date().isoformat()
        appt_dates = [self._parse_appointment_date(item) for item in appointments]
        appt_dates = [item for item in appt_dates if item is not None]
        if appt_dates:
            return (max(appt_dates) + timedelta(days=30)).date().isoformat()
        return (sale_dt + timedelta(days=45)).date().isoformat()

    def _parse_appointment_date(self, appointment: dict[str, Any]) -> datetime | None:
        for key in ("setDate", "SetDate", "appointmentDate", "AppointmentDate", "dateTime", "startDate"):
            parsed = self._parse_datetime(appointment.get(key))
            if parsed is not None:
                return parsed
        return None

    def _milestones(self, sale_dt: datetime, appointments: list[dict[str, Any]],
                    project_manager: str, estimated_completion: str) -> list[dict[str, Any]]:
        milestones = [
            {
                "id": "sale-recorded",
                "title": "Contract Sale Recorded",
                "description": "MarketSharp contract sale date is on file for this account.",
                "status": "completed",
                "dueDate": sale_dt.date().isoformat(),
                "completedDate": sale_dt.date().isoformat(),
                "assignee": project_manager,
                "phase": "Sales",
            }
        ]
        parsed_appointments = [self._parse_appointment_date(item) for item in appointments]
        parsed_appointments = [item for item in parsed_appointments if item is not None]
        if parsed_appointments:
            first_appt = min(parsed_appointments)
            latest_appt = max(parsed_appointments)
            milestones.append(
                {
                    "id": "first-appointment",
                    "title": "Customer Appointment Logged",
                    "description": "An appointment linked to this customer is present in MarketSharp.",
                    "status": "completed",
                    "dueDate": first_appt.date().isoformat(),
                    "completedDate": first_appt.date().isoformat(),
                    "assignee": project_manager,
                    "phase": "Planning",
                }
            )
            milestones.append(
                {
                    "id": "active-production",
                    "title": "Production Window Active",
                    "description": "This customer remains inside the 3-month sandbox window from contract sale date.",
                    "status": "in_progress",
                    "dueDate": estimated_completion,
                    "assignee": project_manager,
                    "phase": "Production",
                    "notes": f"Latest MarketSharp appointment on {latest_appt.date().isoformat()}.",
                }
            )
        else:
            milestones.append(
                {
                    "id": "active-production",
                    "title": "Production Window Active",
                    "description": "This customer remains inside the 3-month sandbox window from contract sale date.",
                    "status": "in_progress",
                    "dueDate": estimated_completion,
                    "assignee": project_manager,
                    "phase": "Production",
                    "notes": "No MarketSharp appointment records were surfaced for this customer in the sandbox sync.",
                }
            )
        milestones.append(
            {
                "id": "documents-pending",
                "title": "Documents Ready For Read-Only Linking",
                "description": "Document and payment tables are staged for read-only integration in the next portal pass.",
                "status": "pending",
                "dueDate": estimated_completion,
                "assignee": project_manager,
                "phase": "Portal Data",
            }
        )
        return milestones

    @staticmethod
    def _generate_password() -> str:
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%"
        return "".join(secrets.choice(alphabet) for _ in range(12))

    @staticmethod
    def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
        raw_salt = secrets.token_bytes(16) if salt is None else base64.b64decode(salt)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), raw_salt, 100_000)
        return base64.b64encode(raw_salt).decode("ascii"), base64.b64encode(digest).decode("ascii")

    @staticmethod
    def _verify_password(password: str, salt: str, expected_hash: str) -> bool:
        _, actual_hash = PortalSandboxService._hash_password(password, salt)
        return hmac.compare_digest(actual_hash, expected_hash)


_service = PortalSandboxService()


@portal_bp.get("/health")
def portal_health():
    return _ok("Portal API healthy.", _service.snapshot_meta())


@portal_bp.get("/sandbox/preview")
def portal_preview():
    return _ok("Portal sandbox preview loaded.", _service.public_preview())


@portal_bp.post("/sandbox/sync")
def portal_sync():
    return _ok("Portal sandbox synced.", _service.sync_accounts(reason="manual_sync"))


@portal_bp.post("/login")
def portal_login():
    body = request.get_json(silent=True) or {}
    email = str(body.get("email") or "").strip()
    password = str(body.get("password") or "")
    if not email or not password:
        return _err("Email and password are required.")

    _service.ensure_recent_snapshot()
    session = _service.login(email, password)
    if not session:
        return _err("Invalid credentials. Please try again.", 401)
    return _ok("Portal login successful.", session)


@portal_bp.get("/admin/accounts")
def admin_accounts():
    return _ok("Portal admin snapshot loaded.", _service.admin_snapshot())


@portal_bp.post("/admin/reset-password")
def admin_reset_password():
    body = request.get_json(silent=True) or {}
    contact_id = str(body.get("contactId") or "").strip()
    if not contact_id:
        return _err("contactId is required.")
    try:
        return _ok("Sandbox password reset.", _service.reset_password(contact_id))
    except ValueError as exc:
        return _err(str(exc), 404)


@portal_bp.post("/admin/impersonate")
def admin_impersonate():
    body = request.get_json(silent=True) or {}
    contact_id = str(body.get("contactId") or "").strip()
    if not contact_id:
        return _err("contactId is required.")
    try:
        return _ok("Customer session loaded.", _service.impersonate(contact_id))
    except ValueError as exc:
        return _err(str(exc), 404)


@portal_bp.post("/admin/visibility")
def admin_visibility():
    body = request.get_json(silent=True) or {}
    contact_id = str(body.get("contactId") or "").strip()
    if not contact_id:
        return _err("contactId is required.")

    docs = body.get("documentsVisible")
    pays = body.get("paymentsVisible")
    try:
        data = _service.update_visibility(
            contact_id,
            documents_visible=bool(docs) if docs is not None else None,
            payments_visible=bool(pays) if pays is not None else None,
        )
        return _ok("Portal visibility updated.", data)
    except ValueError as exc:
        return _err(str(exc), 404)