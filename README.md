# STEDI Lakehouse Data Pipeline on AWS

An end-to-end ETL pipeline that turns raw, semi-structured JSON from three separate STEDI systems into a single consented, privacy-compliant training table for the data science team — built on S3, AWS Glue, the Glue Data Catalog, and Athena.

---

## Business Context

STEDI sells a balance-training device (the Step Trainer). The data science team wants to train a model that detects steps from device and phone sensor readings, but the raw data cannot be used as-is:

1. **Consent** — only a subset of customers agreed to have their data used for research. Everyone else must be excluded before any analysis touches their records.
2. **A broken identity key** — a defect in the customer fulfillment system produced unreliable and duplicated `serialNumber` values in the customer records, so devices could not be reliably attributed to their owners.
3. **Three disconnected sources** — customer records, phone accelerometer readings, and device sensor readings arrive independently with no shared model.

This pipeline resolves all three before any data reaches the modeling layer.

---

## Consumers of This Pipeline

| Consumer | Table they read | What they need from it |
| --- | --- | --- |
| Data science team | `machine_learning_curated` | Paired device + phone sensor readings, consented only |
| Privacy / compliance | `customer_trusted` | Evidence that non-consenting customers never propagate downstream |
| Product analytics | `customers_curated` | The consenting, *active* customer base |
| Fulfillment engineering | `step_trainer_trusted` | Visibility into how many devices fail owner attribution |

---

## Data Sources

All three land as line-delimited JSON in `s3://stedi-lakehouse-yazeedalmuhlaki/`.

| Source | Landing prefix | Grain | Key fields |
| --- | --- | --- | --- |
| Customer registration (website) | `customer/landing/` | One row per customer | `email`, `serialNumber`, `customerName`, `birthDay`, `registrationDate`, `shareWithResearchAsOfDate`, `shareWithPublicAsOfDate`, `shareWithFriendsAsOfDate` |
| Accelerometer (mobile app) | `accelerometer/landing/` | One row per reading | `user` (email), `timeStamp`, `x`, `y`, `z` |
| Step Trainer (IoT device) | `step_trainer/landing/` | One row per reading | `serialNumber`, `sensorReadingTime`, `distanceFromObject` |

The only link between the app data and the device data is the customer: accelerometer rows carry an email, step trainer rows carry a serial number, and the customer record is what maps one to the other.

---

## Lakehouse Architecture

Data moves through three zones in S3, each registered as a database in the Glue Data Catalog (`stedi_db`) so every stage is queryable in Athena.

- **Landing** — raw and immutable, exactly as received. Never modified, so any downstream logic can be corrected and replayed without re-requesting from source systems.
- **Trusted** — filtered and validated. Consent is enforced here, and the `serialNumber` defect is resolved here.
- **Curated** — modeled for a specific consumer. `machine_learning_curated` exists to serve the data science team; a different consumer would build its own curated table from the trusted zone rather than repeat the cleaning.

---

## Jobs

Run in this order — later jobs depend on the output of earlier ones.

### 1. `customer_landing_to_trusted.py`
**Purpose:** enforce research consent at the entry point, so no non-consenting customer exists anywhere downstream.
**In:** `customer/landing/` → **Out:** `customer/trusted/data/` (`customer_trusted`)
**Logic:** Spark SQL filter on `shareWithResearchAsOfDate IS NOT NULL`.

### 2. `accelerometer_landing_to_trusted.py`
**Purpose:** restrict phone sensor readings to consenting customers only.
**In:** `accelerometer/landing/`, `customer_trusted` → **Out:** `accelerometer/trusted/data/` (`accelerometer_trusted`)
**Logic:** join `accelerometer.user = customer.email`, then keep only `user`, `timeStamp`, `x`, `y`, `z` — the join filters, it does not enrich, so customer attributes are dropped after it.

### 3. `customer_trusted_to_curated.py`
**Purpose:** produce the definitive customer list — consenting **and** active. A consenting customer who never generated a reading is not useful as a modeling reference.
**In:** `customer_trusted`, `accelerometer_trusted` → **Out:** `customer/curated/data/` (`customers_curated`)
**Logic:** join on `email = user`, drop the accelerometer columns, then drop duplicates back to one row per customer.

### 4. `step_trainer_landing_to_trusted.py`
**Purpose:** resolve the `serialNumber` defect. The trustworthy serial number is the one emitted by the device itself, not the one stored on the customer record, so the curated customer list is used as the reference set.
**In:** `step_trainer/landing/`, `customers_curated` → **Out:** `step_trainer/trusted/data/` (`step_trainer_trusted`)
**Logic:** `INNER JOIN` on `serialNumber` — any device reading without a known, consenting owner is dropped.

### 5. `machine_learning_curated.py`
**Purpose:** produce the training table by pairing each device reading with the phone reading captured at the same moment.
**In:** `step_trainer_trusted`, `accelerometer_trusted` → **Out:** `machine_learning/curated/data/` (`machine_learning_curated`)
**Logic:** `INNER JOIN` on `sensorReadingTime = timeStamp`.

---

## Row Counts by Stage

| Zone | Table | Rows |
| --- | --- | --- |
| Landing | `customer_landing` | 956 |
| Landing | `accelerometer_landing` | 81,273 |
| Landing | `step_trainer_landing` | 28,680 |
| Trusted | `customer_trusted` | 482 |
| Trusted | `accelerometer_trusted` | 40,981 |
| Trusted | `step_trainer_trusted` | 14,460 |
| Curated | `customers_curated` | 482 |
| Curated | `machine_learning_curated` | 43,681 |

Consent removes roughly half of every source, which is expected. `customers_curated` matching `customer_trusted` exactly means every consenting customer had at least one accelerometer reading.

---


## Stack

Amazon S3 · AWS Glue (Studio, Spark jobs, Data Catalog) · Apache Spark / PySpark · Amazon Athena · Python

---

## Repository Layout

```
.
├── python_scripts/   # The five Glue ETL jobs
├── SQL/              # DDL for the Glue Catalog tables
├── screenshots/      # Athena verification queries and row counts
└── stedi_lakehouse_data_pipeline.png
```
