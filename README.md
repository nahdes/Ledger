# Apex Financial Services — Agentic Audit Ledger
### TRP1 Week 5 · Local Replication Guide

---

## Folder Structure

```
apex-ledger/
│
├── docker-compose.yml          ← Two Postgres containers: dev (5432) + test (5433)
├── pyproject.toml              ← All deps, pytest config, coverage, ruff
├── .env.example                ← Copy to .env — credentials & paths
├── .gitignore
│
├── infra/
│   └── db/
│       ├── init/
│       │   └── 01_schema.sql   ← Runs automatically on first container boot
│       └── pgadmin_servers.json
│
├── scripts/
│   └── load_seed_data.py       ← Idempotent loader: seed_events.jsonl → Postgres
│
├── data/
│   └── .gitkeep                ← Put your seed_events.jsonl here
│
├── src/
│   ├── models/
│   │   └── events.py           ← All 34 event types + exceptions + StoredEvent
│   ├── event_store.py          ← Async EventStore (OCC, outbox, upcasting)
│   ├── aggregates/
│   │   ├── loan_application.py ← 8-state machine + 6 business rules
│   │   └── agent_session.py    ← Gas Town pattern + model version locking
│   ├── commands/
│   │   └── handlers.py         ← load → validate → append command handlers
│   ├── upcasting/
│   │   ├── registry.py         ← Immutable upcast chain
│   │   └── upcasters.py        ← CreditAnalysisCompleted v1→v2
│   ├── projections/            ← Phase 3 (empty stubs, ready for implementation)
│   ├── integrity/              ← Phase 4 audit chain (empty stubs)
│   └── mcp/                    ← Phase 5 MCP server (empty stubs)
│
└── tests/
    ├── conftest.py             ← Root: session-scoped DB pool
    ├── unit/
    │   ├── conftest.py         ← FakeEventStore + pre-built event sequences
    │   ├── test_loan_application.py   ← 30 tests, all 6 business rules
    │   ├── test_agent_session.py      ← 19 tests, Gas Town + model locking
    │   └── test_upcasters.py          ← 11 tests, THE IMMUTABILITY TEST (graded)
    ├── integration/
    │   ├── conftest.py         ← Real DB + autouse TRUNCATE before each test
    │   ├── test_event_store.py ← THE DOUBLE-DECISION TEST (graded)
    │   └── test_seed_data.py   ← Validates loaded seed data
    └── e2e/
        └── test_gas_town.py    ← THE CRASH RECOVERY TEST (graded)
```

---

## Prerequisites

Install these before starting:

| Tool | Version | Check |
|---|---|---|
| **Docker Desktop** | 4.x+ | `docker --version` |
| **Python** | 3.11+ | `python3 --version` |
| **uv** (recommended) | any | `pip install uv` |
| **git** | any | `git --version` |

---

## Step-by-Step Setup

### Step 1 — Get the files

If you have this as a zip, extract it. If you're cloning from git:

```bash
git clone <your-repo-url> apex-ledger
cd apex-ledger
```

If you're setting up from scratch, create the folder and copy all files in exactly
the structure shown above. Every `__init__.py` must exist (they can be empty).

---

### Step 2 — Create your environment file

```bash
cp .env.example .env
```

Open `.env` — the defaults work without any changes if you use this docker-compose.yml.
Only edit if you have port conflicts on 5432 or 5433.

```
DATABASE_URL=postgresql://ledger:ledger_dev_secret@localhost:5432/ledger
TEST_DATABASE_URL=postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test
SEED_DATA_PATH=data/seed_events.jsonl
```

---

### Step 3 — Install Python dependencies

**With uv (recommended — faster, creates a lockfile):**

```bash
uv venv
source .venv/bin/activate        # Mac / Linux
# .venv\Scripts\activate         # Windows

uv sync --extra test
```

**With pip:**

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -e ".[test]"
```

Verify it worked:

```bash
python -c "import asyncpg, pydantic; print('OK')"
```

---

### Step 4 — Start the databases

```bash
docker compose up -d
```

This starts two containers:

| Container | Host Port | Database | Purpose |
|---|---|---|---|
| `ledger-db` | 5432 | `ledger` | Development |
| `ledger-test-db` | 5433 | `ledger_test` | Tests (auto-truncated) |

The file `infra/db/init/01_schema.sql` runs **automatically** on first boot
and creates all tables. You do not need to run it manually.

Wait for both containers to be healthy:

```bash
docker compose ps
```

Both should show `(healthy)` in the STATUS column.
If a container is stuck starting, check its logs:

```bash
docker compose logs ledger-db
docker compose logs ledger-test-db
```

---

### Step 5 — Confirm the schema was applied

```bash
docker exec ledger-db psql -U ledger -d ledger -c "\dt"
```

Expected output — four tables:

```
          List of relations
 Schema |          Name           | Type  | Owner
--------+-------------------------+-------+--------
 public | event_streams           | table | ledger
 public | events                  | table | ledger
 public | outbox                  | table | ledger
 public | projection_checkpoints  | table | ledger
```

Run the same for the test database:

```bash
docker exec ledger-test-db psql -U ledger -d ledger_test -c "\dt"
```

---

### Step 6 — Run the unit tests (no database needed)

```bash
pytest tests/unit/ -v
```

Expected: **60 passed in under 0.2 seconds**. These use an in-memory
`FakeEventStore` — no Docker required.

```
tests/unit/test_agent_session.py ................... [ 31%]
tests/unit/test_loan_application.py ..............................  [ 81%]
tests/unit/test_upcasters.py ...........                           [100%]
60 passed in 0.14s
```

---

### Step 7 — Run the integration tests (requires Docker)

```bash
pytest tests/integration/test_event_store.py -v
```

Expected: **~20 tests passing**. The `clean_db` fixture truncates all tables
before each test automatically.

> **If you see `connection refused on 5433`**: the test database is not ready.
> Run `docker compose ps` and wait for `(healthy)`.

---

### Step 8 — Load your synthetic seed data

Place your `seed_events.jsonl` in the `data/` folder, then:

```bash
# Validate first — reads the file, prints stats, no DB writes
python scripts/load_seed_data.py data/seed_events.jsonl --dry-run

# Load into the dev database
python scripts/load_seed_data.py data/seed_events.jsonl
```

The loader is **idempotent** — safe to run multiple times. Duplicate
`(stream_id, stream_position)` pairs are silently skipped.

Verify the load:

```bash
docker exec ledger-db psql -U ledger -d ledger \
  -c "SELECT COUNT(*) FROM events;"
```

Expected: at least 1198 rows (the number in the reference seed file).

---

### Step 9 — Validate the seed data

```bash
pytest tests/integration/test_seed_data.py -v
```

These tests run against the **dev database** (not the test DB) and check:
- All 6 stream type prefixes are present
- `CreditAnalysisCompleted` is stored at `event_version=2`
- APEX-0021 has `ApplicationApproved`
- APEX-0023 is declined with OFAC block (REG-002)
- `event_streams.current_version` matches the max `stream_position` for every stream
- Outbox coverage ≥ 95%

---

### Step 10 — Run the three graded tests

Run all three with one command:

```bash
pytest -m graded -v
```

Or individually:

```bash
# THE IMMUTABILITY TEST
pytest tests/unit/test_upcasters.py::TestImmutabilityGuarantee -v

# THE DOUBLE-DECISION TEST (concurrency)
pytest tests/integration/test_event_store.py::TestOptimisticConcurrencyControl::test_concurrent_appends_exactly_one_wins -v

# THE CRASH RECOVERY TEST (Gas Town)
pytest tests/e2e/test_gas_town.py::test_agent_reconstructs_after_crash -v
```

---

### Step 11 — Full suite with coverage

```bash
pytest --cov=src --cov-report=term-missing
```

---

### Step 12 — Optional: pgAdmin visual UI

```bash
docker compose --profile tools up -d
```

Open **http://localhost:5050** in your browser.

- Email: `dev@apex.local`
- Password: `pgadmin_dev`

Both databases (`Ledger Dev` and `Ledger Test`) are pre-registered —
no configuration needed.

---

## Useful Commands

```bash
# Start databases
docker compose up -d

# Stop (keeps data)
docker compose down

# Stop and DELETE all data (fresh start)
docker compose down -v

# Unit tests only (no Docker)
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# E2E tests
pytest tests/e2e/ -v

# All three graded tests
pytest -m graded -v

# Full suite with HTML coverage report
pytest --cov=src --cov-report=html
open htmlcov/index.html

# Load seed data (dry run)
python scripts/load_seed_data.py data/seed_events.jsonl --dry-run

# Load seed data into dev DB
python scripts/load_seed_data.py data/seed_events.jsonl

# Load seed data into test DB
python scripts/load_seed_data.py data/seed_events.jsonl \
  --db-url postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test

# Connect directly to dev DB
docker exec -it ledger-db psql -U ledger -d ledger

# Connect directly to test DB
docker exec -it ledger-test-db psql -U ledger -d ledger_test

# Inspect the event store live
docker exec ledger-db psql -U ledger -d ledger \
  -c "SELECT stream_id, event_type, stream_position FROM events ORDER BY global_position LIMIT 20;"
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `connection refused` on 5432 or 5433 | `docker compose ps` — wait for `(healthy)`. If stuck: `docker compose down -v && docker compose up -d` |
| `ModuleNotFoundError: asyncpg` | Activate your venv first: `source .venv/bin/activate` |
| `asyncio_mode` error in pytest | Ensure `pyproject.toml` has `asyncio_mode = "auto"` and `pytest-asyncio >= 0.23` is installed |
| `recorded_at` parse error in loader | Your generator must produce ISO-8601 without a Z suffix: `2026-03-15T14:30:00.123456` |
| `test_seed_data.py` skipped entirely | `data/seed_events.jsonl` not found — run your generator first and place the file there |
| Tables missing after `docker compose up` | The init script only runs on **first** boot. If you already had a volume: `docker compose down -v && docker compose up -d` |
| pgAdmin "Unable to connect to server" | Use host `ledger-db` (the container name), not `localhost`. The `pgadmin_servers.json` is pre-configured. |
| Port 5432 already in use | Edit `docker-compose.yml` and change `"5432:5432"` to e.g. `"5555:5432"`, then update `.env` accordingly |

---

## What Each File Does

### Infrastructure

| File | Purpose |
|---|---|
| `docker-compose.yml` | Defines `ledger-db` (dev, port 5432), `ledger-test-db` (test, port 5433), and optional `pgadmin` (port 5050, profile `tools`) |
| `infra/db/init/01_schema.sql` | Creates `event_streams`, `events`, `outbox`, `projection_checkpoints` tables. Runs automatically on first container start. Also creates the `pg_notify` trigger and `archive_stream()` function. |
| `infra/db/pgadmin_servers.json` | Auto-registers both databases in pgAdmin so you don't have to configure them manually |
| `pyproject.toml` | Declares all Python dependencies, pytest settings (`asyncio_mode = "auto"`, test markers), coverage config, ruff and mypy settings |
| `.env.example` | Template for `.env`. Copy it — never commit your actual `.env` |

### Source

| File | Purpose |
|---|---|
| `src/models/events.py` | All 34 event types derived from `seed_events.jsonl`. Also contains `StoredEvent`, `StreamMetadata`, `ApplicationState`, `VALID_TRANSITIONS`, and the exception hierarchy (`DomainError`, `OptimisticConcurrencyError`, `StreamNotFoundError`, `PreconditionFailedError`) |
| `src/event_store.py` | `EventStore` class. Key methods: `append()` (OCC via `SELECT FOR UPDATE`, writes outbox in same transaction), `load_stream()`, `load_all()` (async generator for projections), `stream_version()`, `get_stream_metadata()`, `archive_stream()` |
| `src/upcasting/registry.py` | `UpcasterRegistry`. The `upcast()` method **always returns a new `StoredEvent`** — the original is never mutated. This is the immutability guarantee tested by the graded test. |
| `src/upcasting/upcasters.py` | Concrete upcasters. `CreditAnalysisCompleted` v1→v2 sets `model_version = "legacy-pre-2026"` and `confidence = None` — never fabricates a value. |
| `src/aggregates/loan_application.py` | `LoanApplicationAggregate`. Replays events to rebuild state. Contains 6 `assert_*` methods: `assert_awaiting_analysis`, `assert_credit_limit_within_assessed_max`, `assert_confidence_floor`, `assert_all_compliance_checks_passed`, `assert_no_prior_credit_analysis`, `assert_valid_contributing_sessions` |
| `src/aggregates/agent_session.py` | `AgentSessionAggregate`. Gas Town: `assert_context_loaded()` raises `GAS_TOWN` if no `AgentSessionStarted` event. `assert_model_version_current()` raises `MODEL_VERSION_LOCK` if model version drifts. |
| `src/commands/handlers.py` | 5 command handlers. Each follows: load aggregate → check business rules → append events. Never touches the DB directly — depends only on the `EventStore` interface. |
| `scripts/load_seed_data.py` | Reads `seed_events.jsonl`, assigns `stream_position` by counting per-stream, upserts `event_streams`, inserts events + outbox rows in batches. Idempotent via `ON CONFLICT DO NOTHING`. |

### Tests

| File | Purpose |
|---|---|
| `tests/conftest.py` | Session-scoped `db_pool` fixture (connects to test DB on port 5433). Shared helpers: `utcnow()`, `make_app_id()`, `make_session_id()` |
| `tests/unit/conftest.py` | `FakeEventStore` — in-memory drop-in for `EventStore`. `stored()` helper converts any `BaseEvent` to a `StoredEvent`. Pre-built fixtures: `submitted_application`, `started_agent_session` |
| `tests/unit/test_loan_application.py` | 30 tests across 6 rule classes. Tests all state machine transitions, boundary cases (confidence = 0.60 passes, 0.599 fails), and edge cases (human override unlocks re-analysis) |
| `tests/unit/test_agent_session.py` | 19 tests. Verifies Gas Town enforcement, model version locking with the exact seed model string `claude-sonnet-4-20250514`, application tracking |
| `tests/unit/test_upcasters.py` | **GRADED.** `TestImmutabilityGuarantee` has 3 assertions: `v1.event_version` unchanged, `v1.payload` dict unchanged, new fields absent from `v1.payload` after `upcast()` |
| `tests/integration/conftest.py` | `store` fixture (real `EventStore`), `raw_conn` fixture (direct asyncpg for DB assertions), `autouse clean_db` that truncates all tables before every test |
| `tests/integration/test_event_store.py` | **GRADED** (`test_concurrent_appends_exactly_one_wins`). Also tests: basic append/load, payload roundtrip, metadata, position filtering, outbox atomicity, archive |
| `tests/integration/test_seed_data.py` | Validates loaded seed data against expected values from the reference `seed_events.jsonl`. Skipped automatically if `data/seed_events.jsonl` doesn't exist |
| `tests/e2e/test_gas_town.py` | **GRADED** (`test_agent_reconstructs_after_crash`). Appends 5 events, deletes in-memory agent, reconstructs from DB, asserts `version=5`, `model_version`, `nodes_executed`, `tools_called` |

---

## The Three Graded Tests — What They Check

### 1. Immutability Test
`tests/unit/test_upcasters.py::TestImmutabilityGuarantee::test_upcast_does_not_mutate_original_payload`

```
BEFORE upcast:  v1.event_version = 1,  v1.payload = {"application_id": "APEX-0001", ...}
AFTER  upcast:  v1.event_version = 1   ← UNCHANGED
                v1.payload has NO new keys  ← UNCHANGED
                v2.event_version = 2   ← new object
                v2.payload has model_version, confidence  ← new fields on NEW object
```

### 2. Double-Decision Test
`tests/integration/test_event_store.py::TestOptimisticConcurrencyControl::test_concurrent_appends_exactly_one_wins`

```
Stream seeded at version 3.
Task A and Task B both call append(..., expected_version=3) simultaneously.

Result:
  Exactly 1 winner  → returns version 4
  Exactly 1 loser   → raises OptimisticConcurrencyError(expected=3, actual=4)
  Stream has 4 events total  ← not 5 (no split-brain)
```

### 3. Crash Recovery Test
`tests/e2e/test_gas_town.py::test_agent_reconstructs_after_crash`

```
1. Append 5 events to agent stream
2. del agent_type, session_id  ← simulates pod eviction
3. ReconstructedAgentContext.load(store, recovered_type, recovered_session)
4. Assert: version=5, model_version="claude-sonnet-4-20250514",
           nodes_executed has 3 entries, tools_called has 1 entry
5. assert_context_loaded()          ← no exception
6. assert_model_version_current()   ← no exception
```
