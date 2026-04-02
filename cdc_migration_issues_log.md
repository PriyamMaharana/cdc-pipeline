# Migration Issues Log
## Project: CDC Pipeline — Change Data Capture Incremental Load Engine
**Tech Stack:** Python 3.11 | SQL Server 2025 Developer Edition | pyodbc | Pandas
**Date:** April 2026
**Status:** All issues resolved ✓

---

## How to Read This Log

Each issue follows this structure:
- **What happened** — the exact error or symptom
- **Root cause** — why it actually happened
- **Fix applied** — exactly what was changed
- **Lesson learned** — what this teaches about production CDC pipelines

---

## Issue #1 — CDC Not Available on SQL Server Express Edition

**Severity:** Critical (entire project blocked)
**Phase:** Initial setup — EXEC sys.sp_cdc_enable_db
**Error message:**
```
This instance of SQL Server is the Express Edition (64-bit).
CDC is only available in the Standard, Enterprise, and Developer editions.
```

**What happened:**
The CDC setup SQL ran without error until sp_cdc_enable_db — at which
point SQL Server rejected the command entirely. The project could not
proceed on the existing Express instance.

**Root cause:**
SQL Server Express Edition does not include CDC functionality. CDC
requires access to the SQL Server Agent (which runs the capture and
cleanup jobs) — and SQL Server Agent is not included in Express Edition.
This is a licensing restriction, not a configuration issue.

**Fix applied:**
Downloaded and installed SQL Server 2025 Enterprise Developer Edition
(free for development use) alongside the existing Express instance.
Developer Edition has all Enterprise features with no licensing cost
for non-production use.

Connection string updated from:
```python
# OLD — Express instance
'SERVER=localhost\\SQLExpress;'

# NEW — Developer default instance
'SERVER=localhost;'
```

**Lesson learned:**
Always verify the SQL Server edition supports required features before
starting development. Key features NOT available in Express Edition:
CDC, SQL Server Agent, SSRS, Always On, partitioning. For any
serious ETL or data engineering project, Developer Edition should
be the minimum — it is free and feature-complete.

---

## Issue #2 — SQL Server 2025 Mandatory Encryption Blocking Connection

**Severity:** Critical (could not connect from Python or SSMS)
**Phase:** Database connection — pyodbc.connect()
**Error message:**
```
pyodbc.OperationalError: ('08001', '[08001] [Microsoft][ODBC SQL Server Driver]
[DBNETLIB]SQL Server does not exist or access denied. (17) (SQLDriverConnect);
Invalid connection string attribute (0)')
```

**What happened:**
After installing SQL Server 2025, neither SSMS nor the Python pipeline
could connect to the new instance. The error message was misleading —
it said "SQL Server does not exist" but the server was clearly running.

**Root cause:**
SQL Server 2025 enforces encrypted connections by default. The old
`{SQL Server}` ODBC driver does not support the newer TLS encryption
parameters that SQL Server 2025 requires. The connection was being
rejected at the encryption handshake level before authentication
even occurred.

Two compounding problems:
1. Wrong ODBC driver (`{SQL Server}` instead of `{ODBC Driver 17 for SQL Server}`)
2. Missing `TrustServerCertificate=yes` parameter for local development

**Fix applied:**
Updated connection string to use the modern ODBC driver with
encryption parameters:

```python
# BROKEN — old driver, no encryption params
def get_conn():
    return pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost'           # also missing semicolon
        'DATABASE=SCD_Project;'
        'Trusted_Connection=yes;'
    )

# FIXED — modern driver with encryption handling
def get_conn():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=SCD_Project;'
        'Trusted_Connection=yes;'
        'TrustServerCertificate=yes;'
    )
```

Also fixed a missing semicolon after `localhost` — Python was
concatenating `SERVER=localhost` and `DATABASE=SCD_Project;` into
`SERVER=localhostDATABASE=SCD_Project;` which further broke the
connection string.

**Lesson learned:**
When upgrading SQL Server versions, always check encryption defaults.
SQL Server 2022+ and 2025 enforce `Encrypt=Strict` by default.
For local development connections:
- Use `ODBC Driver 17` or `ODBC Driver 18` — not the legacy `{SQL Server}` driver
- Add `TrustServerCertificate=yes` to bypass certificate validation locally
- In production, use proper SSL certificates instead of bypassing

---

## Issue #3 — CDC Capture Instance Already Exists Error

**Severity:** High (CDC setup could not complete)
**Phase:** EXEC sys.sp_cdc_enable_table
**Error message:**
```
Msg 22926, Level 16, State 1, Procedure sys.sp_cdc_verify_capture_instance
Could not create a capture instance because the capture instance name
'dbo_orders' already exists in the current database.
Specify an explicit unique name for the parameter @capture_instance.
```

**What happened:**
Running sp_cdc_enable_table failed because a capture instance named
`dbo_orders` already existed from a previous setup attempt. The error
prevented CDC from being reconfigured on the orders table.

**Root cause:**
During an earlier failed setup attempt, sp_cdc_enable_table had
partially succeeded — it created the capture instance metadata but
the overall setup did not complete cleanly. When the setup script
was run again, SQL Server found the existing `dbo_orders` instance
and refused to create a duplicate.

SQL Server allows a maximum of 2 capture instances per table. If
one already exists with the same name, you must either drop it
first or specify a different capture instance name.

**Fix applied:**
Dropped the existing capture instance first, then recreated cleanly:

```sql
-- Step 1: Drop existing capture instance
EXEC sys.sp_cdc_disable_table
    @source_schema    = N'dbo',
    @source_name      = N'orders',
    @capture_instance = N'dbo_orders';

-- Verify it's gone
SELECT capture_instance FROM cdc.change_tables;
-- Returns 0 rows

-- Step 2: Recreate fresh
EXEC sys.sp_cdc_enable_table
    @source_schema        = N'dbo',
    @source_name          = N'orders',
    @role_name            = NULL,
    @supports_net_changes = 1;

-- Verify
SELECT capture_instance FROM cdc.change_tables;
-- Returns: dbo_orders
```

**Lesson learned:**
When re-running CDC setup scripts in development, always check for
existing capture instances first:
```sql
SELECT capture_instance, source_schema, source_name
FROM cdc.change_tables;
```
If any exist from a previous attempt, drop them before re-enabling.
In production, document all active capture instances and treat them
as infrastructure — do not drop without a maintenance window.

---

## Issue #4 — NULL LSN Error on First Pipeline Run

**Severity:** Critical (pipeline crashed on first run)
**Phase:** get_lsn_range() — sys.fn_cdc_get_min_lsn()
**Error message:**
```
2026-04-01 20:27:46 | INFO  | First run - starting from minimum LSN
2026-04-01 20:27:46 | ERROR | Pipeline FAILED: cannot convert 'NoneType' object to bytes
```

**What happened:**
The pipeline crashed immediately on the first run when trying to
get the LSN range. The error occurred inside get_lsn_range() when
attempting to call `.hex()` on the LSN value returned by
`sys.fn_cdc_get_min_lsn('dbo_orders')`.

**Root cause:**
`sys.fn_cdc_get_min_lsn()` returns NULL when the CDC change table
is empty — meaning no changes have been captured yet. This happens
when CDC is enabled on a table but no DML operations (INSERT/UPDATE/
DELETE) have occurred since CDC was enabled.

The pipeline was run immediately after CDC setup, before any data
was inserted into the orders table. With no captured changes, SQL
Server had no minimum LSN to return — it returned NULL instead.

The original code had no NULL check:
```python
# BROKEN — no NULL guard
cursor.execute("SELECT sys.fn_cdc_get_min_lsn('dbo_orders')")
min_lsn = cursor.fetchone()[0]   # returns None
lsn_from = min_lsn               # None
logger.info(f"LSN: {lsn_from.hex()}")  # crash: NoneType has no .hex()
```

**Fix applied:**
Two-part fix:

Part 1 — Insert data before running pipeline:
```sql
INSERT INTO orders (customer_id, product_name, quantity,
                    unit_price, order_status, order_date)
VALUES (1, 'Laptop', 1, 45000.00, 'Pending', '2026-03-01');
-- Wait 5 seconds for CDC capture job
WAITFOR DELAY '00:00:05';
-- Verify CDC captured it
SELECT COUNT(*) FROM cdc.dbo_orders_CT;  -- must be > 0
```

Part 2 — Added NULL guard in Python:
```python
# FIXED — NULL guard before using LSN values
if min_lsn is None or max_lsn is None:
    logger.warning(
        "LSN is NULL — CDC change table is empty. "
        "Insert data into orders table first, "
        "wait 5 seconds for CDC job to capture, then retry."
    )
    raise ValueError(
        "CDC has no captured changes yet. "
        "Run INSERT INTO orders in SSMS first."
    )
```

**Lesson learned:**
CDC capture is asynchronous — SQL Server Agent runs the capture
job on a schedule (default: every 5 seconds). There is always a
small delay between a DML operation and when it appears in the
change table. Always verify `SELECT COUNT(*) FROM cdc.dbo_orders_CT`
returns rows before running the pipeline for the first time.

In production, add a pre-flight check at the start of every CDC
pipeline run that validates the LSN range is non-null before
proceeding. Fail fast with a clear message rather than crashing
mid-pipeline.

---

## Issue #5 — CDC Not Capturing Changes Despite Being Enabled

**Severity:** Critical (silent failure — no error, just no data)
**Phase:** Post-setup verification — cdc.dbo_orders_CT
**Symptom:**
```sql
-- CDC was enabled, orders table had data, but:
SELECT COUNT(*) FROM cdc.dbo_orders_CT;
-- Always returned 0 no matter how many inserts were done

-- Tried everything:
-- ✗ Dropped and recreated capture instance
-- ✗ Disabled and re-enabled CDC on database
-- ✗ Re-ran entire cdc_setup.sql from scratch
-- ✗ Restarted SSMS
-- Still 0 rows in change table
```

**What happened:**
CDC was correctly enabled at both the database level and table level.
Inserts into the orders table were succeeding. But the CDC change
table `cdc.dbo_orders_CT` remained permanently empty regardless of
how many rows were inserted or how long we waited.

No error message was shown anywhere — the setup appeared successful,
the pipeline appeared configured, but changes were silently not
being captured.

**Root cause:**
SQL Server CDC relies on two background jobs that run automatically
via **SQL Server Agent**:

```
CDC Capture Job  → reads transaction log → writes to change table
CDC Cleanup Job  → removes old change records past retention period
```

Both jobs are created automatically when CDC is enabled — BUT they
only run if **SQL Server Agent is running**. On a fresh Developer
Edition installation, SQL Server Agent is disabled by default.

With Agent stopped:
- CDC is "enabled" in metadata (is_cdc_enabled = 1)
- Capture instances exist (cdc.change_tables has rows)
- But NO actual capturing happens
- Change table stays empty forever
- No error is raised anywhere

This is one of the most confusing CDC issues because everything
looks correctly configured but nothing works.

**How it was diagnosed:**
After exhausting all CDC configuration options (drop/recreate instance,
disable/re-enable database CDC, full reset), checked SQL Server Agent
status in SSMS Object Explorer:

```
SSMS → Object Explorer → SQL Server Agent
→ Right-click → Status showed: Stopped
```

**Fix applied:**
Started SQL Server Agent via SSMS:
```
SSMS → Object Explorer → SQL Server Agent
→ Right-click → Start
→ Status changed to: Running (green arrow)
```

Then verified CDC capture jobs exist and are running:
```sql
-- Check CDC jobs were created
SELECT job.name, job.enabled, activity.run_requested_date
FROM msdb.dbo.sysjobs job
LEFT JOIN msdb.dbo.sysjobactivity activity
    ON job.job_id = activity.job_id
WHERE job.name LIKE '%cdc%';

-- Should show two jobs:
-- cdc.SCD_Project_capture  → enabled = 1
-- cdc.SCD_Project_cleanup  → enabled = 1
```

After starting Agent — inserted one row into orders, waited 5 seconds:
```sql
INSERT INTO orders (customer_id, product_name, quantity,
                    unit_price, order_status, order_date)
VALUES (99, 'Test Item', 1, 100.00, 'Pending', '2026-04-01');

WAITFOR DELAY '00:00:05';

SELECT COUNT(*) FROM cdc.dbo_orders_CT;
-- Now returned: 1  ← CDC working
```

**To make SQL Server Agent start automatically on Windows startup:**
```
Windows → Services → SQL Server Agent (MSSQLSERVER)
→ Right-click → Properties
→ Startup type: Automatic
→ Click OK
```

**Lesson learned:**
CDC without SQL Server Agent is like having a security camera with
no recording device — the hardware is installed but nothing is
captured. SQL Server Agent is the engine that powers CDC.

**Always verify Agent is running before debugging CDC:**
```sql
-- Quick Agent status check
EXEC xp_servicecontrol 'QUERYSTATE', 'SQLServerAGENT';
-- Should return: Running
```

Add this as the FIRST step in any CDC troubleshooting checklist:
1. Which version of SQL Server (Express or Developer Edition)? Recomended - Developer Edition
1. Is SQL Server Agent running?
2. Is CDC enabled on database? (is_cdc_enabled = 1)
3. Is CDC enabled on table? (is_tracked_by_cdc = 1)
4. Do capture jobs exist? (msdb.dbo.sysjobs LIKE '%cdc%')
5. Does change table have rows? (SELECT COUNT(*) FROM cdc.dbo_orders_CT)

In production environments, SQL Server Agent is always running —
but on local development machines after a fresh install it must
be started and set to automatic startup manually.

---

## Summary Table

| # | Issue | Phase | Severity | Type |
|---|---|---|---|---|
| 1 | CDC not available in Express Edition | Setup | Critical | Edition limitation |
| 2 | SQL Server 2025 encryption blocking connection | Connection | Critical | Driver + config |
| 3 | CDC capture instance already exists | CDC setup | High | Duplicate setup |
| 4 | NULL LSN on first pipeline run | get_lsn_range() | Critical | Empty change table |
| 5 | CDC not capturing — SQL Server Agent disabled | Capture | Critical | Silent failure |

---

## What This Project Taught Me

1. **Edition matters for enterprise features** — CDC, Agent, partitioning
   all require Standard/Enterprise/Developer. Verify edition before
   starting any feature-dependent project.

2. **SQL Server 2025 changed encryption defaults** — legacy ODBC drivers
   and connection strings that worked on 2019 will fail silently on 2025.
   Always use ODBC Driver 17 or 18 with TrustServerCertificate for
   local development.

3. **CDC setup is not idempotent** — running it twice creates duplicates.
   Always check existing capture instances before enabling. Treat CDC
   configuration as infrastructure, not a rerunnable script.

4. **CDC capture is asynchronous** — data must exist AND be captured
   before the first pipeline run. Build pre-flight LSN validation
   into every CDC pipeline — fail fast with a meaningful error message
   rather than crashing with a NoneType exception.

5. **NULL guards are mandatory for LSN operations** — any CDC function
   that returns an LSN can return NULL in edge cases. Always validate
   before using LSN values in string or binary operations.

6. **CDC without SQL Server Agent is silent and useless** — Agent
   powers the capture and cleanup jobs. On fresh Developer Edition
   installs, Agent is disabled by default. It is the first thing to
   check when CDC appears configured but no changes are captured.
   Set Agent to Automatic startup so it survives machine restarts.
