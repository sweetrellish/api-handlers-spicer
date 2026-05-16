# Production Diagnostic Toolkit

Three new tools for debugging why export coverage is low and understanding data flow:

## 1. `debug_contact_rows.py` — Per-Contact Row Generation

**Purpose:** See exactly what rows are generated for specific contacts and why.

**Run locally:**
```bash
python debug_contact_rows.py <contact_id> [contact_id2 ...]
```

**Example:**
```bash
python debug_contact_rows.py 767aa2a7-ec31-4c08-8876-3a4c8911319e 88fbbf27-40e7-4b85-bb46-2d7b741f65b6
```

**Output shows:**
- Contact details (GCLID, UTM fields)
- Raw appointments found (dates, all fields)
- Raw jobs found (dates, all fields)
- Actual rows that would be written to CSV
- Why rows were skipped (date range mismatch, etc.)

**Use case:** You have a contact you know should be in the export but isn't. This shows exactly where the data gets lost (query fails, dates don't match, etc.).

---

## 2. `check_raw_queries.py` — Raw MarketSharp Query Results

**Purpose:** See what the MarketSharp API actually returns for appointments and jobs.

**Run on server (or locally if MarketSharp creds configured):**
```bash
cd /home/rellis/spicer/gclid
python check_raw_queries.py <contact_id> [contact_id2 ...]
```

**Output shows:**
- All appointment records returned (dates, all fields)
- All job records returned (dates, all fields)
- Contact record (name, email, phone)
- Query errors (if any)

**Use case:** Verify that MarketSharp queries are working and returning data. If queries return empty, appointments/jobs don't exist. If queries return data with odd dates, you've found the source of the problem.

---

## 3. `compare_months.py` — Coverage Across Multiple Months

**Purpose:** Run the full worker across several months and see if coverage is consistently low or varies.

**Run on server:**
```bash
cd /home/rellis/spicer/gclid
python compare_months.py
```

**Output shows:**
- Row count and unique contact count for Feb, Mar, Apr 2025
- Coverage % per month
- Warnings if coverage is suspiciously low

**Use case:** Determine if the problem is:
- Data genuinely sparse for Feb (only 0.7% coverage)
- Seasonal (more data in Jan or March)
- Systemic (all months are low)

---

## Workflow: Debug Low Export Coverage

If you find that exports have very low coverage (like the current 0.7%), use these in order:

### Step 1: Check if data exists
```bash
python compare_months.py
```
→ If Feb is 0.7% but Jan is 60%, data genuinely sparse for Feb  
→ If all months are <5%, systemic issue with queries or filtering

### Step 2: Verify MarketSharp queries work
```bash
# Pick a contact from the reference list (Contacts (1).csv)
python check_raw_queries.py 767aa2a7-ec31-4c08-8876-3a4c8911319e
```
→ If queries return empty, MarketSharp has no events for that contact  
→ If queries return data with dates, dates are the issue

### Step 3: Trace row generation
```bash
# Use contacts from check_raw_queries output (ones with data)
python debug_contact_rows.py 767aa2a7-ec31-4c08-8876-3a4c8911319e
```
→ If rows are built, export logic works  
→ If no rows despite data existing, date filter or _build_rows_for_contact is the issue

### Step 4: Verify final export
```bash
cd /home/rellis/spicer/gclid
python run_full_test.py  # or just: python gclid_worker.py --month 2025-02 ...
python validate_all.py
python diagnose_export.py
```

---

## Example: Full Debug Session

```bash
# On server:

# 1. Check coverage across months
rellis@server:~/spicer/gclid$ python compare_months.py
Reference contacts: 304
  Generating 2025-02...OK
  Generating 2025-03...OK
  Generating 2025-04...OK

Results:
Month          Rows      Contacts     Coverage
2025-02        9         2            0.7%
2025-03        45        18           5.9%
2025-04        120       45           14.8%

Analysis:
✓ Reasonable coverage (14.8% in 2025-04)
→ Feb is genuinely sparse, April has more data

# 2. Check queries for a Feb contact with actual data
rellis@server:~/spicer/gclid$ python check_raw_queries.py Cj0KCQiA4L67BhDUARIsADWrl7ExQ51yuINJRkKe-IrJgS9hwVkP79tmcDUX1MwlXe1LeUBmMH0UzEEaAlAYEALw_wcB

--- Contact: <gclid> ---

APPOINTMENTS:
  Found: 2
  [1] setDate: 2025-02-27T00:00:00
  [2] setDate: 2025-02-23T11:11:00

JOBS:
  Found: 1
  [1] saleDate: 2025-02-24

# 3. Debug why this contact only has X rows instead of Y expected
rellis@server:~/spicer/gclid$ python debug_contact_rows.py Cj0KCQiA4L67BhDUARIsADWrl7ExQ51yuINJRkKe-IrJgS9hwVkP79tmcDUX1MwlXe1LeUBmMH0UzEEaAlAYEALw_wcB

--- Contact: <gclid> ---

CONTACT INFO:
  contact_id: <uuid>
  contact_name: John Smith

FIELDS:
  GCLID: Cj0KCQiA4L67BhDUARIsADWrl7ExQ51yuINJRkKe-IrJgS9hwVkP79tmcDUX1MwlXe1LeUBmMH0UzEEaAlAYEALw_wcB
  utm_source: google

APPOINTMENTS:
  Total found: 2
    [1] date=2025-02-27T00:00:00
    [2] date=2025-02-23T11:11:00

JOBS:
  Total found: 1
    [1] saleDate=2025-02-24

GENERATED ROWS:
  Total: 3
    [1] Qualified Lead        2/27/2025 12:00:00 AM America/New_York value=200
    [2] Sold Job             2/24/2025 12:00:00 AM America/New_York value=
    [3] Qualified Lead       2/23/2025 11:11:00 AM America/New_York value=200

→ 3 rows generated (matches 9 total / 3 unique contacts)
```

---

## Key Files

- [diagnose_export.py](diagnose_export.py) — Overall export analysis
- [compare_months.py](compare_months.py) — Multi-month coverage comparison
- [check_raw_queries.py](check_raw_queries.py) — Raw MarketSharp API results
- [debug_contact_rows.py](debug_contact_rows.py) — Per-contact row generation trace
- [validate_all.py](validate_all.py) — CSV format and logic validation

