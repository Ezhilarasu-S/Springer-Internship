# DHAP-34: Email Communications CSV → PostgreSQL via Airflow

**Project**: Glynac-AI Data Platform  
**Epic**: DHAP-34  
**Status**: In Progress  
**Dataset**: `email_communications`  
**Author**: Data Platform Team  
**Created**: July 15, 2026

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Architecture](#architecture)
6. [Configuration](#configuration)
7. [Running the Pipeline](#running-the-pipeline)
8. [Verification](#verification)
9. [Troubleshooting](#troubleshooting)
10. [Maintenance](#maintenance)

---

## Overview

This project implements an end-to-end data ingestion pipeline that:

- **Ingests** email communication data from a local CSV file (sourced from SharePoint)
- **Validates** data against a formal schema (schema.yaml)
- **Transforms** and cleans the data (timestamps, normalization, etc.)
- **Loads** cleaned data into a PostgreSQL table

The entire stack runs locally via Docker Compose: Apache Airflow (orchestrator) + PostgreSQL (data warehouse).

### Key Features

✅ **Schema-driven ingestion** — data is validated against formal schema  
✅ **Idempotent loads** — re-running the DAG is safe (truncate-then-load strategy)  
✅ **Secure credential handling** — secrets never hardcoded, via .env file  
✅ **Reproducible environment** — single `docker compose up` command  
✅ **Comprehensive logging** — full audit trail in Airflow UI  

---

## Project Structure

```
.
├── manifest.yaml                           # Dataset metadata
├── schema.yaml                             # Column definitions & validation rules
├── ddl.sql                                 # PostgreSQL table DDL (auto-run on startup)
├── docker-compose.yaml                     # Airflow + PostgreSQL stack definition
├── .env.example                            # Environment template (copy to .env)
├── dags/
│   └── load_email_communications_to_postgres.py   # Main Airflow DAG
├── data/
│   └── dataset.csv                         # Source CSV (copied from uploads)
├── logs/                                   # Airflow task logs (auto-created)
├── config/                                 # Airflow config overrides (auto-created)
└── README.md                               # This file
```

---

## Prerequisites

### System Requirements

- **Docker** (v20.10+) and **Docker Compose** (v1.29+)
- **Git** (for version control)
- **8GB RAM** recommended (Docker desktop)
- **10GB disk space** for images and volumes

### Software Versions

| Tool | Version | Notes |
|------|---------|-------|
| Apache Airflow | 2.9.1 | Python 3.11-based image |
| PostgreSQL | 15 | Alpine-based, lightweight |
| Python | 3.11 | Via Airflow image |
| pandas | latest | Included in Airflow image |
| PyYAML | latest | Included in Airflow image |

### Verify Installation

```bash
# Check Docker
docker --version
# Expected: Docker version 20.10.x or higher

docker compose --version
# Expected: Docker Compose version 1.29.x or higher

# Check Git
git --version
# Expected: git version 2.x or higher
```

---

## Quick Start

### 1. Clone Repository & Setup

```bash
# Clone the Glynac airflow-dag-configs repo
git clone https://github.com/Glynac-AI/airflow-dag-configs.git
cd airflow-dag-configs

# Navigate to your project path
cd intern-project/<your-name>/project-DHAP-34/email_communications

# Copy this README and all files into this directory
# (Files should already be present if following the task)
```

### 2. Create Environment File

```bash
# Copy .env.example to .env
cp .env.example .env

# Edit .env with your values (in development, defaults are usually fine):
# POSTGRES_PASSWORD=your_secure_password
# FERNET_KEY=your_generated_key  # Generate if needed

# DON'T commit .env to git
# Add .env to .gitignore if not already present
echo ".env" >> .gitignore
```

### 3. Copy Dataset

```bash
# Create data directory if it doesn't exist
mkdir -p data

# Copy the CSV from uploads
cp /mnt/user-data/uploads/dataset.csv ./data/

# Verify
ls -lh data/dataset.csv
```

### 4. Start the Stack

```bash
# Build and start all services (Airflow, PostgreSQL, Redis)
docker compose up -d

# This will:
# - Pull images
# - Create containers
# - Initialize PostgreSQL (run ddl.sql)
# - Initialize Airflow (create admin user: admin/admin)
# - Start webserver on http://localhost:8080

# Wait 30-40 seconds for services to fully start
sleep 40

# Check status
docker compose ps
```

### 5. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

You should see the DAG `load_email_communications_to_postgres` in the list.

### 6. Trigger the DAG

1. Click on the DAG name in the Airflow UI
2. Click **"Trigger DAG"** (play button)
3. Click **"Trigger"** in the modal
4. Watch the DAG run in real-time (refresh browser)

Expected runtime: **2–5 minutes** depending on system.

### 7. Verify Load Success

```bash
# Once DAG completes, query PostgreSQL
docker compose exec postgres psql -U airflow -d airflow_db

# In PostgreSQL CLI:
SELECT COUNT(*) FROM email_communications;
# Expected: 2259 rows

SELECT * FROM email_communications LIMIT 5;

\q  # Exit psql
```

---

## Architecture

### Data Flow Diagram

```
┌──────────────────────┐
│   Local CSV File     │
│   (dataset.csv)      │
└──────────┬───────────┘
           │
           v
┌──────────────────────────────────────────────┐
│         Apache Airflow (Docker)              │
│  ┌────────────────────────────────────────┐  │
│  │  DAG: load_email_communications_to_postgres
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ Task 1: read_csv                 │  │  │
│  │  │ - Load CSV → pandas DataFrame    │  │  │
│  │  └────────────────┬─────────────────┘  │  │
│  │                   v                    │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ Task 2: validate_schema          │  │  │
│  │  │ - Check columns exist            │  │  │
│  │  │ - Validate enum values           │  │  │
│  │  │ - Check required fields not null │  │  │
│  │  │ - Fail fast on errors            │  │  │
│  │  └────────────────┬─────────────────┘  │  │
│  │                   v                    │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ Task 3: transform                │  │  │
│  │  │ - Strip whitespace               │  │  │
│  │  │ - Parse timestamps → UTC TZ      │  │  │
│  │  │ - Normalize email_status field   │  │  │
│  │  │ - Round customer_satisfaction    │  │  │
│  │  └────────────────┬─────────────────┘  │  │
│  │                   v                    │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ Task 4: load_to_postgres         │  │  │
│  │  │ - Truncate table                 │  │  │
│  │  │ - COPY/INSERT data               │  │  │
│  │  │ - Verify row count               │  │  │
│  │  └────────────────┬─────────────────┘  │  │
│  │                   v                    │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │ Task 5: summary                  │  │  │
│  │  │ - Log execution summary          │  │  │
│  │  └──────────────────────────────────┘  │  │
│  └────────────────────────────────────────┘  │
└────────┬──────────────────────────────────────┘
         │
         v
┌──────────────────────────┐
│  PostgreSQL (Docker)     │
│  email_communications    │
│  Table (2259 rows)       │
└──────────────────────────┘
```

### Components

| Component | Role | Docker Container |
|-----------|------|------------------|
| **Apache Airflow Webserver** | DAG authoring, scheduling, monitoring | `email-comm-airflow-webserver` |
| **Apache Airflow Scheduler** | Triggers DAG runs on schedule (or manual) | `email-comm-airflow-scheduler` |
| **PostgreSQL** | Metadata DB (Airflow) + Target warehouse | `email-comm-postgres` |
| **Redis** | Message broker for distributed execution (optional) | `email-comm-redis` |

---

## Configuration

### Environment Variables (.env)

**Never commit real secrets to .env.** Below are key variables:

```bash
# Airflow Metadata Database
POSTGRES_DB=airflow_db              # Airflow's internal DB
POSTGRES_USER=airflow               # Default user
POSTGRES_PASSWORD=airflow_password  # CHANGE ME

# Fernet Key (for encrypting connections/variables)
FERNET_KEY=<base64_string>          # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Target Database (where data is loaded)
TARGET_POSTGRES_HOST=postgres       # Docker service name
TARGET_POSTGRES_PORT=5432
TARGET_POSTGRES_DB=airflow_db
TARGET_POSTGRES_USER=airflow
TARGET_POSTGRES_PASSWORD=airflow_password

# CSV location (inside container)
CSV_FILE_PATH=/opt/airflow/data/dataset.csv
```

### Schema Files

#### manifest.yaml
- **Dataset metadata**: name, source, owner, status
- **Purpose**: Document the dataset at the enterprise level

#### schema.yaml
- **Column definitions**: name, type, nullable, description
- **Validation rules**: enum constraints, range checks
- **Primary key definition**

#### ddl.sql
- **PostgreSQL DDL**: CREATE TABLE statement
- **Indexes**: On common query columns
- **Constraints**: NOT NULL, CHECK, UNIQUE

---

## Running the Pipeline

### Manual Trigger (Recommended for Testing)

```bash
# Via Airflow UI:
1. Navigate to http://localhost:8080
2. Find DAG: load_email_communications_to_postgres
3. Click DAG name → "Trigger DAG" → "Trigger"
4. Watch the run in the Graph view

# Via CLI (alternative):
docker compose exec airflow-webserver \
  airflow dags test load_email_communications_to_postgres 2026-07-15
```

### Scheduled Runs

Currently disabled (`schedule_interval: None`). To enable:

```yaml
# In load_email_communications_to_postgres.py, change:
schedule_interval=None,  # To:
schedule_interval="0 2 * * *",  # Run daily at 2 AM UTC
```

### Monitor Execution

```bash
# Tail Airflow logs
docker compose logs -f airflow-webserver

# Tail Scheduler logs
docker compose logs -f airflow-scheduler

# View DAG logs in UI
# http://localhost:8080 → DAG → Tree View → Click task → Logs tab
```

---

## Verification

### 1. Check Airflow UI

**URL**: http://localhost:8080

- ✅ DAG appears in DAG list
- ✅ DAG runs to completion (green checkmark in Tree/Graph view)
- ✅ All 5 tasks complete successfully
- ✅ No errors in Logs tab

### 2. Query PostgreSQL

```bash
# Connect to the container's PostgreSQL
docker compose exec postgres psql -U airflow -d airflow_db

# Verify table exists
\dt email_communications;

# Verify row count
SELECT COUNT(*) AS total_rows FROM email_communications;
# Expected: 2259

# Sample data
SELECT 
  thread_id, 
  sender, 
  email_status, 
  email_criticality, 
  customer_satisfaction
FROM email_communications
LIMIT 10;

# Check for duplicates (should be 0)
SELECT 
  thread_id, sender, receiver, timestamp, 
  COUNT(*) AS duplicates
FROM email_communications
GROUP BY thread_id, sender, receiver, timestamp
HAVING COUNT(*) > 1;

# Exit
\q
```

### 3. Test Idempotency

Re-run the DAG:

```bash
# Trigger again in UI or:
docker compose exec airflow-webserver \
  airflow dags test load_email_communications_to_postgres 2026-07-16

# Verify row count is still 2259 (not doubled)
docker compose exec postgres psql -U airflow -d airflow_db -c \
  "SELECT COUNT(*) FROM email_communications;"
```

### 4. Test Schema Validation

Intentionally trigger a validation error:

```bash
# Edit data/dataset.csv: change one email_status value to invalid_status

# Re-run DAG

# Check logs — should fail at validate_schema task with clear error
docker compose logs airflow-scheduler | grep -A 5 "validation"
```

---

## Troubleshooting

### Issue: "docker: command not found"

**Solution**: Install Docker Desktop or Docker Engine.

```bash
# macOS with Homebrew
brew install docker docker-compose

# Ubuntu/Debian
sudo apt-get install docker.io docker-compose-plugin
```

### Issue: "Port 8080 already in use"

**Solution**: Use a different port or kill existing container.

```bash
# Option 1: Kill existing container
docker compose down

# Option 2: Use different port (edit docker-compose.yaml)
# Change "8080:8080" to "8888:8080"
```

### Issue: Airflow UI takes 2–3 minutes to load

**Solution**: Normal behavior. Airflow initializes the metadata database on first run.

```bash
# Monitor initialization
docker compose logs -f airflow-webserver | grep "ready to handle"

# Wait until you see this message, then refresh browser
```

### Issue: DAG doesn't appear in Airflow UI

**Solution**: DAG file must be in the `dags/` folder and syntactically valid.

```bash
# Check DAG syntax
python3 -m py_compile dags/load_email_communications_to_postgres.py

# If errors, fix them. Airflow auto-reloads DAGs every 30 seconds.

# Force refresh in UI: click "Refresh" in top-right corner
```

### Issue: "No module named 'psycopg2'"

**Solution**: Airflow image includes psycopg2. If missing, rebuild:

```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Issue: DAG task fails with "CSV file not found"

**Solution**: Ensure CSV is in the `data/` folder.

```bash
# Check from host
ls -lh data/dataset.csv

# Check inside container
docker compose exec airflow-webserver ls -lh /opt/airflow/data/
```

### Issue: PostgreSQL connection refused

**Solution**: PostgreSQL may not be ready. Restart:

```bash
docker compose restart postgres

# Wait 10 seconds, then retry DAG
sleep 10
```

### Issue: "UNIQUE constraint violated" during load

**Solution**: This means a duplicate row was loaded. Check:

```bash
# Find duplicates
docker compose exec postgres psql -U airflow -d airflow_db -c \
  "SELECT thread_id, sender, receiver, timestamp, COUNT(*) 
   FROM email_communications 
   GROUP BY thread_id, sender, receiver, timestamp 
   HAVING COUNT(*) > 1;"
```

---

## Maintenance

### Regular Tasks

#### Daily
- ✅ Monitor Airflow UI for failed DAG runs
- ✅ Check logs for warnings or errors

#### Weekly
- ✅ Verify PostgreSQL table row count (should match expectations)
- ✅ Review Airflow logs for performance issues

#### Monthly
- ✅ Backup PostgreSQL data
- ✅ Review and archive old Airflow logs

### Backup & Recovery

```bash
# Backup PostgreSQL
docker compose exec postgres pg_dump -U airflow airflow_db > backup.sql

# Restore from backup
docker compose exec -T postgres psql -U airflow airflow_db < backup.sql
```

### Clean Up

```bash
# Stop all services
docker compose down

# Remove volumes (WARNING: deletes all data)
docker compose down -v

# Remove images (WARNING: will need to re-download)
docker compose down --rmi all
```

### Update Configuration

```bash
# Edit environment variables
nano .env

# Restart services to apply changes
docker compose down
docker compose up -d
```

---

## Common Questions

### Q: Can I run this DAG multiple times without duplicating data?

**A**: Yes! The DAG truncates the table before loading (`TRUNCATE TABLE ... RESTART IDENTITY`). This is idempotent.

### Q: How do I add new columns to the table?

**A**: 
1. Update `schema.yaml` with new column definition
2. Update `ddl.sql` with new column (ALTER TABLE or recreate)
3. Update the DAG to handle the new column
4. Re-run

### Q: Can I schedule this DAG to run daily?

**A**: Yes, change `schedule_interval=None` to `schedule_interval="0 2 * * *"` (daily at 2 AM UTC).

### Q: How do I change the PostgreSQL password?

**A**: 
1. Edit `.env` file
2. Run `docker compose down`
3. Delete the `postgres_data` volume: `docker volume rm <project>_postgres_data`
4. Run `docker compose up -d` (fresh DB with new password)

### Q: Where are the Airflow logs stored?

**A**: Inside the `logs/` folder on your host machine (mounted from container).

### Q: Can I connect to PostgreSQL from my local machine?

**A**: Yes! PostgreSQL runs on `localhost:5432`. 

```bash
# Using psql
psql -h localhost -U airflow -d airflow_db

# Using any SQL client (pgAdmin, DBeaver, etc.)
# Host: localhost
# Port: 5432
# Username: airflow
# Password: (from .env POSTGRES_PASSWORD)
# Database: airflow_db
```

---

## Support & Contact

**Jira**: [DHAP-34](https://jira.glynac-ai.com/browse/DHAP-34)  
**Repository**: [glynac-AI/airflow-dag-configs](https://github.com/Glynac-AI/airflow-dag-configs)  
**Slack**: #data-platform-team  
**Email**: data-platform@glynac-ai.com

---

## License

Proprietary — Glynac-AI  
See LICENSE file in repository root.

---

## Changelog

### v1.0 (July 15, 2026)
- Initial release
- CSV ingestion with schema validation
- PostgreSQL load with idempotent truncate-then-insert
- Comprehensive error handling and logging
- Full Docker Compose stack

---

**Last Updated**: July 15, 2026  
**Next Review**: August 15, 2026
