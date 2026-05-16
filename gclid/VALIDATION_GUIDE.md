# GCLID Validation Scripts Reference

## Quick Start

After generating a report on the server, download the CSV and run:
```bash
python validate_all.py
```

## Individual Scripts

### `validate_csv_schema.py`
**Purpose:** Verify CSV format and structure  
**Checks:**
- Parameters:TimeZone= line is present
- Column headers are correct (no Email/Phone)
- All rows have GCLID

**Run locally after:** `scp` CSV from server

---

### `validate_lifecycle.py`
**Purpose:** Ensure Sold Jobs always occur after their paired Qualified Leads  
**Checks:**
- Maps GCLID → first Qualified Lead timestamp
- Verifies Sold Job timestamps are later
- Reports violations with specific GCLIDs

**Expected:** Zero violations or you'll need to adjust 90-day lookback window

---

### `validate_times.py`
**Purpose:** Check time diversity and catch suspicious patterns  
**Checks:**
- Counts unique timestamps
- Shows frequency distribution
- Flags low diversity (acceptable for date-only sources with fallback time)

**Expected:** Either all identical (if source is date-only) or good spread

---

### `validate_all.py`
**Purpose:** Run all validators in sequence  
**Use when:** You want a quick pass/fail on entire export

---

### `run_full_test.py`
**Purpose:** Full end-to-end: generate report + validate  
**Run on server:** `cd /home/rellis/spicer/gclid && python run_full_test.py`

**Output:** Either "ready for upload" or details on what failed

---

### `verify_deployment.py`
**Purpose:** Check that deployed code matches local files  
**Run locally:** `python verify_deployment.py`

**Output:** Shows SHA256 hashes and flags mismatches

---

## Workflow

### Initial Deployment
1. Edit code locally
2. Run `python verify_deployment.py` (shows what needs deployment)
3. SCP files to server: `scp gclid/*.py rellis@host:/home/rellis/spicer/gclid/`
4. Verify: `python verify_deployment.py` again

### Report Generation
1. On server: `cd /home/rellis/spicer/gclid && python run_full_test.py`
   - Generates Feb 2025 report + validates in one go
   
   OR two steps:
   - Generate: `python gclid_worker.py --month 2025-02 --contacts-csv ms-report-hasGCLID.csv --contacts-mode assist`
   - Then: `python validate_all.py`

2. Locally: Download CSV and `python validate_all.py` for full audit

---

## Troubleshooting

**"IndentationError" or "SyntaxError"**
- Use these Python files, not inline SSH heredocs (they're fragile)

**Lifecycle violations**
- Qualified Lead and Sold Job from same contact have wrong ordering
- Check GCLID timestamp extraction in source data
- May need to adjust 90-day lookback or appointment/job query

**Low time diversity**
- Expected if most source records are date-only
- Falls back to 12:00:00 (configurable in gclid_sync.py)

**File mismatch in verify_deployment**
- Local files are newer than server versions
- Need to SCP them to update

