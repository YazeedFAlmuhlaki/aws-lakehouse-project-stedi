# STEDI Lakehouse Data Pipeline on AWS

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline on AWS to build a lakehouse solution for STEDI. The pipeline processes raw, semi-structured JSON data from various sources (customer website, mobile app, and IoT devices) and transforms it into a clean, structured, and curated dataset ready for machine learning analysis.

---

## The Challenge

The STEDI data science team needed to analyze sensor data from their Step Trainer devices to build predictive models. However, they faced several critical data challenges:

1.  **Data Privacy**: The raw data included customers who had not consented to having their data used for research. This data needed to be filtered out to respect user privacy.
2.  **Data Quality**: A bug in the customer fulfillment system resulted in unreliable and duplicated `serialNumber` data in the customer records, making it impossible to link customers to their devices reliably.
3.  **Data Silos**: The data was spread across three different sources, making it difficult to perform combined analysis.

This project solves these challenges by building a robust data pipeline that cleans, joins, and restructures the data into a single, reliable, and analysis-ready table.

---

## Lakehouse Architecture

The pipeline follows a multi-layered lakehouse architecture, progressively refining data as it moves through different zones in Amazon S3.

* **Landing Zone (Bronze)**: Stores the raw, untouched JSON data as it arrives from the source systems.
* **Trusted Zone (Silver)**: Contains data that has been cleaned, filtered, and validated. In this zone, we filter for customer consent and solve the `serialNumber` data quality issue.
* **Curated Zone (Gold)**: Holds the final, aggregated, and business-ready dataset, specifically modeled for the data science team's machine learning use case.

---

## ⚙️ Technology Stack

* **Data Lake Storage**: Amazon S3
* **ETL Service**: AWS Glue & Glue Studio
* **Data Catalog**: AWS Glue Data Catalog
* **Interactive Querying**: Amazon Athena
* **Core Engine**: Apache Spark
* **Languages**: Python (PySpark) & SQL

---

## ETL Workflow

The entire pipeline is orchestrated by a series of AWS Glue jobs. Each job performs a specific transformation, moving data from one zone to the next.

### 1. Landing Zone to Trusted Zone

* **`customer_landing_to_trusted.py`**:
    * **Purpose**: Filters raw customer data to keep only records of users who have consented to share their data for research.
    * **Input**: `customer_landing`
    * **Output**: `customer_trusted`
    * **Transformation**: A SQL `WHERE` clause filters for records where `shareWithResearchAsOfDate` is not null.

* **`accelerometer_landing_to_trusted.py`**:
    * **Purpose**: Sanitizes accelerometer data to include only readings from trusted customers.
    * **Inputs**: `accelerometer_landing`, `customer_trusted`
    * **Output**: `accelerometer_trusted`
    * **Transformation**: An `INNER JOIN` on the user's email.

### 2. Trusted Zone to Curated Zone

* **`customer_trusted_to_curated.py`**:
    * **Purpose**: Creates a final, definitive master list of unique customers who are both consenting and active (have submitted sensor data).
    * **Inputs**: `customer_trusted`, `accelerometer_trusted`
    * **Output**: `customers_curated`
    * **Transformation**: An `INNER JOIN` followed by a `Drop Duplicates` transform to ensure a unique customer list.

* **`step_trainer_landing_to_trusted.py`**:
    * **Purpose**: Solves the `serialNumber` bug by joining the raw step trainer data (which has the correct serial numbers) with the curated customer list.
    * **Inputs**: `step_trainer_landing`, `customers_curated`
    * **Output**: `step_trainer_trusted`
    * **Transformation**: A robust SQL `INNER JOIN` on `serialNumber`.

* **`machine_learning_curated.py`**:
    * **Purpose**: Creates the final table for the data science team by combining trusted step trainer and accelerometer readings that occurred at the same time.
    * **Inputs**: `step_trainer_trusted`, `accelerometer_trusted`
    * **Output**: `machine_learning_curated`
    * **Transformation**: A SQL `INNER JOIN` on the event timestamps (`sensorReadingTime` and `timeStamp`).

---

## ✅ Final Table Counts

The following table summarizes the row counts at each stage of the pipeline, confirming the successful execution of all transformations.

| Zone      | Table Name                 | Row Count |
| :-------- | :------------------------- | :-------- |
| Landing   | `customer_landing`         | 956       |
| Landing   | `accelerometer_landing`    | 81,273    |
| Landing   | `step_trainer_landing`     | 28,680    |
| **Trusted** | **`customer_trusted`** | **482** |
| **Trusted** | **`accelerometer_trusted`** | **40,981** |
| **Trusted** | **`step_trainer_trusted`** | **14,460** |
| **Curated** | **`customers_curated`** | **482** |
| **Curated** | **`machine_learning_curated`** | **43,681** |