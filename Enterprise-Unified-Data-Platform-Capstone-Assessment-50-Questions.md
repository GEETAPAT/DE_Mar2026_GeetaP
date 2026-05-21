# Enterprise Unified Data Platform — Student Assessment (80 Questions)

**Course / Project:** Azure Data Engineering Capstone — Retail & E-Commerce  
**Document Reference:** Enterprise-Unified-Data-Platform-Azure-Capstone.md (v3.0)  
**Instructions:** Answer all **80** questions in your own words. Short answers (2–5 sentences) are sufficient unless otherwise stated. Do not copy text verbatim from the document—demonstrate understanding.

- **Questions 1–50:** Include answer spaces below each question.  
- **Questions 51–80:** Full questions only (write answers on separate sheets or below each question as directed by your instructor).

**Student Name:** _______________________________  
**Date:** _______________________________  
**Team / Roll No.:** _______________________________

---

## Section A — Project Overview & Business Context (Questions 1–8)

**1.** What is the primary business problem this retail capstone project aims to solve?

**Your Answer:**



---

**2.** List any five business objectives of the unified data platform.

**Your Answer:**



---

**3.** What industry domain and project duration are specified for this capstone?

**Your Answer:**



---

**4.** Name the five heterogeneous data sources used in this architecture and their primary purpose (one line each).

**Your Answer:**



---

**5.** What is the difference between batch load and near real-time load in this project? Give one source example for each.

**Your Answer:**



---

**6.** Why does the organization need a unified customer and sales view? What layer primarily delivers it?

**Your Answer:**



---

**7.** What are three risks mentioned in the project documentation if the platform is not implemented?

**Your Answer:**



---

**8.** Who are the key stakeholders, and what does each group need from the platform?

**Your Answer:**



---

## Section B — Architecture & Medallion Design (Questions 9–16)

**9.** What is the architecture pattern used in this project? Briefly explain Bronze, Silver, and Gold layers.

**Your Answer:**



---

**10.** List eight core Azure/cloud components used in this platform and one responsibility of each.

**Your Answer:**



---

**11.** What is the role of Azure Data Factory versus Azure Databricks in the end-to-end flow?

**Your Answer:**



---

**12.** Why is Delta Lake chosen instead of plain Parquet files in Bronze/Silver/Gold?

**Your Answer:**



---

**13.** Describe the end-to-end data flow from source systems to Power BI in seven steps or fewer.

**Your Answer:**



---

**14.** What containers exist in ADLS Gen2 for this project, and what is stored in each?

**Your Answer:**



---

**15.** What is the folder path pattern for Raw layer files from SQL Server?

**Your Answer:**



---

**16.** What is Unity Catalog, and where is it applied in this architecture?

**Your Answer:**



---

## Section C — Ingestion, Metadata & ADF Pipelines (Questions 17–24)

**17.** What is metadata-driven ingestion, and which ADF pipeline implements it?

**Your Answer:**



---

**18.** List the columns of the `ingestion_config` metadata table and explain the purpose of `Watermark_Column` and `Last_Load_Time`.

**Your Answer:**



---

**19.** Name all four main ADF pipelines in this project and the purpose of each.

**Your Answer:**



---

**20.** Which ADF activities are used in `PL_Metadata_Driven_Ingestion`? (List at least five.)

**Your Answer:**



---

**21.** How does `PL_API_Incremental_Load` handle API pagination and failures?

**Your Answer:**



---

**22.** What linked services are required in ADF for this retail project? List all eight.

**Your Answer:**



---

**23.** What validations does ADF perform before triggering Databricks? Name at least four.

**Your Answer:**



---

**24.** What is the parent orchestration pipeline, and which pipelines does it call in sequence?

**Your Answer:**



---

## Section D — Databricks, Delta Lake & Transformations (Questions 25–32)

**25.** List the five Databricks notebooks and the layer each supports.

**Your Answer:**



---

**26.** What cluster configuration is recommended (node type, runtime, autoscaling)?

**Your Answer:**



---

**27.** What lineage/metadata columns are added in the Bronze layer? Why are they important?

**Your Answer:**



---

**28.** Explain SCD Type 1 and SCD Type 2. Which dimension uses SCD2 in this project?

**Your Answer:**



---

**29.** What transformations happen in the Silver layer? List at least five.

**Your Answer:**



---

**30.** Name the three fact tables and five dimension tables in the Gold star schema.

**Your Answer:**



---

**31.** What Delta Lake features are used in this project? Explain any two with a retail use case.

**Your Answer:**



---

**32.** What is the purpose of `MERGE INTO` in Silver/Gold processing? Give one example table.

**Your Answer:**



---

## Section E — Data Quality, Security & Monitoring (Questions 33–40)

**33.** What happens to records that fail critical data quality rules?

**Your Answer:**



---

**34.** Name four types of data quality validations applied in this framework.

**Your Answer:**



---

**35.** Which notebook runs data quality checks, and when does it block Gold processing?

**Your Answer:**



---

**36.** How are secrets managed in this project? Where should connection strings NOT be stored?

**Your Answer:**



---

**37.** What authentication methods are used between ADF, Databricks, and ADLS?

**Your Answer:**



---

**38.** List five security controls implemented in this platform.

**Your Answer:**



---

**39.** What tools are used for monitoring, and what are three monitored metrics?

**Your Answer:**



---

**40.** What should happen when an ADF pipeline fails in production (alerting and audit)?

**Your Answer:**



---

## Section F — Power BI, CI/CD & Operations (Questions 41–46)

**41.** Name the three Power BI dashboards and one key visual/metric on each.

**Your Answer:**



---

**42.** What is Row-Level Security (RLS), and give one example role filter for regional managers.

**Your Answer:**



---

**43.** What is incremental refresh in Power BI, and which tables typically use it?

**Your Answer:**



---

**44.** What are the four deployment environments in the CI/CD approach?

**Your Answer:**



---

**45.** What artifacts are deployed using Azure DevOps (list at least four)?

**Your Answer:**



---

**46.** What is the typical daily UTC timeline from ADF ingestion start to Power BI refresh?

**Your Answer:**



---

## Section G — Scenarios, Deliverables & Applied Understanding (Questions 47–50)

**47.** Daily SQL Server load fails because a new column was added at the source. What solution does the document recommend?

**Your Answer:**



---

**48.** Duplicate customer records appear in reporting. At which layer should deduplication occur, and how?

**Your Answer:**



---

**49.** Power BI dashboard refresh is slow. List three optimization strategies from the document.

**Your Answer:**



---

**50.** List nine project deliverables expected at project completion. Which deliverable proves end-to-end integration?

**Your Answer:**



---

## Section H — Granular Implementation & Schemas (Questions 51–58)

**51.** What is the recommended Git monorepo folder structure for the `adf`, `databricks`, `infra`, and `powerbi` components in this retail lakehouse project?

**52.** What is the complete ADLS path pattern for landing Raw Parquet files from SQL Server table `sales_orders` on 20 May 2026, including file naming convention?

**53.** What columns are defined in the `retail_prod.metadata.ingestion_config` control table, and what is the purpose of `last_watermark_value` versus `last_load_time`?

**54.** List all Bronze Delta tables documented for this project and specify the file format and partition column used for each source at the Bronze layer.

**55.** Describe the full column list and grain of the `retail_prod.gold.fact_sales` table, including which columns are measures versus foreign keys to dimensions.

**56.** What is the merge key and deduplication logic used when building `silver.sales_order_header` from `bronze.sqlserver_sales_orders`?

**57.** What seed configuration rows are defined for `ingestion_config` for SQL Server, Oracle, REST API, SFTP, and MongoDB sources (include load type and watermark column for each)?

**58.** What is the Unity Catalog naming pattern for schemas and an example fully qualified table name for a Silver customer entity?

---

## Section I — Pipelines, Jobs & Lineage (Questions 59–66)

**59.** What are the pipeline parameters and variables defined for `PL_Metadata_Driven_Ingestion`, and how is the dynamic sink path constructed for a Copy activity?

**60.** Describe the complete activity sequence inside `PL_API_Incremental_Load`, including OAuth token handling, pagination loop, and retry policy.

**61.** What is the Databricks job name `JOB_Retail_Daily_Medallion`, and what are the task keys and dependencies between Bronze, Silver, DQ, Gold, and Audit notebooks?

**62.** What parameters are passed from ADF to `NB_Bronze_Load`, and what PySpark transformations add lineage metadata before writing to Delta?

**63.** What is the column-level lineage path for the measure `net_sales_amount` from source OLTP through Bronze, Silver, Gold mart, and Power BI?

**64.** What is the documented daily UTC schedule from `PL_Master_Daily_Orchestration` start through Power BI semantic model refresh, including approximate times for Bronze, Silver, Gold, and audit?

**65.** Which ADF trigger types are used for daily batch ingestion versus near real-time API ingestion, and what are their schedule examples?

**66.** What activities run inside `PL_Master_Daily_Orchestration` when `PL_Metadata_Driven_Ingestion` or `PL_Databricks_Transformation` fails?

---

## Section J — Data Quality, Security & Operations (Questions 67–72)

**67.** What is the schema of the `retail_prod.dq.validation_results` table, and how does `NB_Data_Quality_Checks` decide whether to allow Gold processing?

**68.** List the six DQ rules in the catalog (rule_id, target_table, rule_type) and the severity level for each.

**69.** What is stored in the quarantine zone path pattern, and what columns are captured in the quarantine table when a record fails validation?

**70.** What RBAC roles are assigned to the ADF Managed Identity, Databricks Managed Identity, and BI Developer group on storage and workspace resources?

**71.** What are the four data classification levels used in this project, and give one example column or dataset for confidential data?

**72.** What SLO targets are defined for pipeline availability, Bronze-to-Gold latency, API ingest lag, and Power BI refresh completion?

---

## Section K — Power BI, CI/CD & Team Delivery (Questions 73–78)

**73.** Which Gold Delta tables are imported into the Power BI semantic model, and which of them use incremental refresh versus full refresh?

**74.** Write the DAX measure definitions (or formulas) documented for Total Revenue, Gross Margin %, and Sales Growth % in the retail semantic model.

**75.** What is the Git branch to Azure environment mapping for develop, QA, UAT, and production deployments, and which stages require manual approval?

**76.** What artifacts are listed in the CI/CD artifact inventory (ADF, Databricks, infrastructure, Power BI, DevOps), and what happens in the post-deployment validation stage?

**77.** What are the phase gate criteria for G0 through G4 in the 16-week timeline, and what must be complete before exiting the Gold/Medallion gate (G2)?

**78.** What is the RACI assignment for metadata framework development, Gold star schema design, and production deployment among PM, Architect, ADF, Databricks, BI, and DevOps roles?

---

## Section L — Applied & Troubleshooting (Questions 79–80)

**79.** REST API response latency increases and pages time out during `PL_API_Incremental_Load`. Describe the root cause, immediate fix, monitoring signal, and preventive control documented for this scenario.

**80.** A new supplier column is added to the SFTP vendor CSV file mid-project. Explain how the platform handles schema change from Raw through Bronze to Silver without breaking the daily pipeline, referencing file formats, Delta features, and metadata configuration.

---

## Submission Checklist

- [ ] All 80 questions answered
- [ ] Answers written in own words
- [ ] Name and date filled in
- [ ] File saved as: `YourName_Assessment_50Q.md` or PDF

---

## For Instructor Use Only (Do Not Distribute to Students Before Assessment)

| Section | Questions | Topics |
|---------|-----------|--------|
| A | 1–8 | Business problem, objectives, sources, stakeholders |
| B | 9–16 | Medallion, components, ADLS, Unity Catalog |
| C | 17–24 | Metadata, ADF pipelines, linked services, validation |
| D | 25–32 | Notebooks, Bronze/Silver/Gold, Delta, SCD, MERGE |
| E | 33–40 | DQ, quarantine, Key Vault, RBAC, monitoring |
| F | 41–46 | Power BI, RLS, CI/CD, timeline |
| G | 47–50 | Real-time scenarios, deliverables, applied troubleshooting |
| H | 51–58 | Repo structure, ADLS paths, schemas, Bronze/Gold tables |
| I | 59–66 | ADF/Databricks pipelines, jobs, lineage, schedules |
| J | 67–72 | DQ catalog, quarantine, RBAC, SLOs |
| K | 73–78 | Power BI model, DAX, CI/CD, gates, RACI |
| L | 79–80 | API latency and schema change scenarios |

**Suggested grading:** 2 marks per question = **160 marks** total (80 questions). Partial credit for incomplete but correct technical direction.

---

*Assessment version: 2.0 | 80 questions (50 with answer spaces + 30 full questions) | Based on: Enterprise-Unified-Data-Platform-Azure-Capstone.md v3.0*
