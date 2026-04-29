# Urban Mobility Intelligence Platform  
### End-to-End Azure + Databricks Data Engineering Project

## Project Overview
This project is a full-scale **Urban Mobility Reservation & Analytics Platform** designed to simulate a real-world transportation ecosystem where booking data flows through both **real-time streaming** and **batch ingestion** pipelines.

The platform combines:
- **FastAPI Web Application** for ride booking simulation
- **Azure Event Hub** for real-time streaming ride events
- **Azure Data Factory (ADF)** for batch ingestion of static mapping + historical bulk ride datasets
- **Azure Data Lake Storage Gen2 (ADLS)** as cloud data lake
- **Azure Databricks (Delta Live Tables / Lakeflow Pipelines)** for Bronze → Silver → Gold transformations
- **SCD Type 2 + CDC** implementation for dimension history tracking
- **Star Schema** for analytics-ready warehousing

---

# Business Problem
Urban mobility systems generate:
### Real-Time:
- New ride bookings
- Live trip events

### Batch:
- Static lookup files (city, vehicle, payment, status)
- Historical bulk rides

This project unifies both sources into a scalable modern data platform.


Batch Flow:
GitHub JSON Files → Azure Data Factory → ADLS Gen2 → Databricks Bronze Batch
Transformation Flow:
Bronze → Silver (OBT) → Gold (Star Schema)
Tech Stack
Frontend / API:
FastAPI
Python
Azure:
Azure Event Hub
Azure Data Factory
Azure Data Lake Storage Gen2
Data Engineering:
Azure Databricks
PySpark
Delta Lake
Delta Live Tables / Lakeflow Pipelines
Version Control:
GitHub
Data Layers
Bronze Layer
Sources:
Streaming:
rides_raw from Event Hub Kafka stream
Batch:
bulk_rides
map_cities
map_vehicle_types
map_vehicle_makes
map_payment_methods
map_ride_statuses
map_cancellation_reasons
Purpose:
Raw ingestion
Minimal transformation
Replay capability
Silver Layer
Output:
silver_obt (Operational Business Table)
Features:
Streaming + batch merge
Initial bulk load + live streaming append
Lookup enrichment
Deduplication
Standardization
Business Value:

Single unified ride transaction layer.

Gold Layer
Star Schema:
Dimensions:
dim_location (SCD Type 2)
dim_vehicle
dim_payment
dim_booking
dim_passenger
dim_driver
Fact:
fact
SCD Type 2 + CDC
Implemented In:
dim_location
Behavior:
Tracks city/state/region changes over time
Uses:
__START_AT
__END_AT
Example:

When New York → South East Coast changes to South West Coast:

Old row expires
New row inserted
Full historical lineage preserved
Key Features
Real-Time Streaming:
Kafka API with Azure Event Hub
Incremental ride ingestion
Batch Processing:
ADF → ADLS ingestion
Static mapping hydration
Data Governance:
GitHub integration
ARM templates
Infrastructure exports
Warehousing:
SCD2 Dimensions
CDC
Star Schema
Project Structure
Urban_Mobility_Intelligence/
│
├── WebApp/
├── ADF/
├── Databricks/
│   ├── bronze_ingestion
│   ├── silver_obt
│   └── gold_model
│
├── infrastructure/
│   ├── eventhub_template.json
│   ├── storage_template.json
│   └── datafactory_arm/
│
├── architecture/
├── screenshots/
└── README.md
Screenshots Included
Web App:
Booking UI
API trigger
Azure:
Event Hub Namespace
Event Hub Data Explorer
Storage Account
ADF Pipeline
Databricks:
Bronze ingestion
Silver OBT
Gold pipeline graph
SCD2 validation
Sample Validation Queries
Check SCD2:
SELECT *
FROM streaming.bronze.dim_location
WHERE pickup_city_id = 1
ORDER BY city_updated_at DESC;
Learning Outcomes
Through this project I practiced:
Streaming architecture
Batch + streaming unification
Medallion architecture
Delta Lake
CDC
SCD Type 2
Azure ecosystem integration
Data warehousing
Cloud deployment documentation
Why This Project Matters

This project demonstrates practical capabilities for:

Data Engineer
Analytics Engineer
Azure Data Engineer
Databricks Engineer

It reflects enterprise-grade design rather than isolated notebook experimentation.

Future Improvements
Power BI dashboard
CI/CD automation
Terraform deployment
Unity Catalog governance
Partition optimization
Monitoring & alerting
Author
Sourish Pal

Data Engineer | Azure | Databricks | PySpark | Streaming Architecture
