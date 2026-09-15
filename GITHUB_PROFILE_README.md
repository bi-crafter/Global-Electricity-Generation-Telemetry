<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=600&size=28&pause=1000&color=0EA5E9&center=true&vCenter=true&width=750&lines=Hi+%F0%9F%91%8B+I'm+Abhijit+Sarkar;Data+Engineer+%7C+Microsoft+Fabric+Architect;Building+Real-Time+Data+Pipelines;Medallion+Architecture+%7C+PySpark+%7C+Direct+Lake" alt="Typing SVG" />

<br/>

[![LinkedIn](https://img.shields.io/badge/LinkedIn-bi--crafter-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/bi-crafter)
[![GitHub](https://img.shields.io/badge/GitHub-bi--crafter-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/bi-crafter)
[![Open to Work](https://img.shields.io/badge/Open%20to%20Work-Data%20Engineer-22C55E?style=for-the-badge&logo=checkmarx&logoColor=white)](#)

</div>

---

## 🚀 About Me

I build **production-grade data infrastructure** that turns raw, messy operational data into reliable, real-time business intelligence.

Specialising in **Microsoft Fabric**, **PySpark**, **Direct Lake**, and **Power BI**, I architect end-to-end pipelines — from live multi-source API ingestion all the way to executive C-suite command centers — using the **Medallion (Bronze → Silver → Gold)** pattern to enforce data quality and sub-second query performance at every layer.

```python
abhijit = {
    "role"         : "Data Engineer / Microsoft Fabric Architect",
    "stack"        : ["Microsoft Fabric", "PySpark", "Python", "SQL", "Power BI", "Delta Lake", "Direct Lake"],
    "architecture" : ["Medallion (Bronze→Silver→Gold)", "Star Schema", "Direct Lake", "Data Quality Engine"],
    "domains"      : ["Renewable Energy & Sustainability", "Fintech", "Aviation", "Healthcare"],
    "open_to"      : ["Data Engineer", "Fabric Analytics Engineer", "Remote / Hybrid"],
}
```

---

## 🛠️ Tech Stack

<div align="center">

| Layer | Tools |
|---|---|
| **Lakehouse & Orchestration** | ![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=flat&logo=microsoft&logoColor=white) ![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=flat&logo=microsoft-azure&logoColor=white) |
| **Processing** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white) ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white) ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white) |
| **Storage & Delta Engine** | ![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=databricks&logoColor=white) ![Azure Data Lake](https://img.shields.io/badge/ADLS%20Gen2-0078D4?style=flat&logo=microsoft-azure&logoColor=white) |
| **Analytics & BI** | ![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black) ![Direct Lake](https://img.shields.io/badge/Direct%20Lake-0078D4?style=flat&logo=microsoft&logoColor=white) ![SQL](https://img.shields.io/badge/SQL-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white) ![DAX](https://img.shields.io/badge/DAX-F2C811?style=flat&logo=powerbi&logoColor=black) |
| **Governance & Control** | ![Git](https://img.shields.io/badge/Git-F05032?style=flat&logo=git&logoColor=white) ![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white) ![PySpark DQ](https://img.shields.io/badge/PySpark--DQ-22C55E?style=flat&logo=apachespark&logoColor=white) |

</div>

---

## 🏗️ Featured Projects

### ⚡ Global Renewable Energy & Grid Decarbonization Command Center
> *Enterprise end-to-end data engineering platform & Direct Lake Power BI command center on Microsoft Fabric*

- 🌐 Ingests **774.8 TWh global generation & real-time grid carbon intensity** from 5 live REST APIs (Electricity Maps, Ember Climate, NASA POWER, Open-Meteo, World Bank)
- 🏛️ Full **Medallion Architecture (Bronze → Silver → Gold)** with PySpark UTC normalization and window deduplication
- 🛡️ Automated **PySpark Data Quality Engine** verifying null constraints and range anomalies, logging results to `dq_results` (99.8% pass rate)
- ⚡ **Direct Lake Semantic Model** with 26 master DAX measures powering dynamic What-If scenario modeling (+5% to +30% target expansion)
- 📊 7-page dark glassmorphism C-suite Power BI Command Center report built with a custom JSON design system

[![Repo](https://img.shields.io/badge/View%20Project-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/bi-crafter/Global-Electricity-Generation-Telemetry)

---

### ✈️ CCU Airport Intelligence Platform
> *Real-time flight operations analytics using Microsoft Fabric*

- 🔄 Ingests **50K+ flight records/day** from 5 live APIs (OpenSky, Aviationstack, METAR, TomTom, FlightAware)
- ⚡ Reduced data latency from **4 hours → 8 minutes** using PySpark streaming on Fabric
- 🏛️ Full **Medallion Architecture** (Bronze → Silver → Gold) with Delta Lake
- 📊 3 real-time Power BI dashboards serving operations, safety, and congestion teams

[![Repo](https://img.shields.io/badge/View%20Project-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/bi-crafter/ccu-airport-intelligence-platform)

---

### 🏦 UNO Digital Bank — KYC Compliance Pipeline
> *End-to-end compliance data pipeline for a digital banking platform*

- ⚡ Reduced compliance reporting cycle from **4 hours → under 5 minutes**
- 🔍 Automated **data quality checks** catching 99.2% of malformed records before Silver layer
- 🔐 Integrated real-time video KYC API + JWT auth pipeline into unified audit trail
- 📦 Full Medallion pipeline with automated SLA alerting

[![Repo](https://img.shields.io/badge/View%20Project-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/bi-crafter/uno-bank-kyc-pipeline)

---

### 🏥 Healthcare Clinical Trial Analytics
> *Power BI + SQL analytics platform for clinical trial performance*

- 📊 Multi-page Power BI report with advanced DAX (time intelligence, RLS, iterators)
- 🗄️ Star schema data model with 5 fact tables and 8 dimension tables
- 🧬 Drug efficacy, patient survival rates, and country-wise trial distribution

[![Repo](https://img.shields.io/badge/View%20Project-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/bi-crafter)

---

## 📊 GitHub Stats

<div align="center">

<img src="https://github-readme-stats.vercel.app/api?username=bi-crafter&show_icons=true&theme=tokyonight&hide_border=true&count_private=true" height="165" />
<img src="https://github-readme-stats.vercel.app/api/top-langs/?username=bi-crafter&layout=compact&theme=tokyonight&hide_border=true" height="165" />

</div>

---

## 🏅 Certifications

| Certification | Issuer | Status |
|---|---|---|
| DP-600 Fabric Analytics Engineer | Microsoft | 🔄 In Progress |
| HackerRank SQL | HackerRank | ✅ Certified Apr 2026 |
| Data Fundamentals | IBM | ✅ Certified Apr 2026 |
| Delta Lake in Microsoft Fabric | Microsoft Learn | ✅ Completed |

---

## 📫 Let's Connect

<div align="center">

I'm actively looking for **Data Engineer / Analytics Engineer / Fabric Architect** roles — **Remote or Hybrid, India**.

If you're building a data platform and need someone who has shipped real pipelines in production, let's talk.

[![LinkedIn](https://img.shields.io/badge/Connect%20on%20LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/bi-crafter)

</div>

---

<div align="center">
<img src="https://komarev.com/ghpvc/?username=bi-crafter&color=0EA5E9&style=flat-square&label=Profile+Views" />
</div>
