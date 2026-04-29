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

