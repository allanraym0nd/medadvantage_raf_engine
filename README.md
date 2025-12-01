# MedAdvantage RAF Engine
**Risk Adjustment Factor Calculation Pipeline for Medicare Advantage Plans**

![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)

## 📋 Project Overview

This project implements an end-to-end **Risk Adjustment Factor (RAF) calculation engine** for Medicare Advantage health plans. It transforms raw healthcare claims data into actionable risk scores that determine Medicare payments and identify high-risk members requiring care management.

### Business Problem

Medicare Advantage plans receive payments based on the expected healthcare costs of their enrolled members. Sicker members with chronic conditions cost more to treat, so Medicare adjusts payments using **Risk Adjustment Factors (RAF scores)**.

**This pipeline calculates those RAF scores by:**
- Mapping medical diagnoses (ICD-10 codes) to Hierarchical Condition Categories (HCCs)
- Mapping prescription drugs (NDC codes) to RxHCC categories
- Aggregating risk weights to produce member-level risk scores

### Key Metrics

- **100 members** enrolled
- **500 medical claims** processed
- **800 pharmacy claims** analyzed
- **20 HCC conditions** mapped
- **16 RxHCC medication categories** tracked

---

## 🏗️ Architecture

### Data Pipeline Flow
```
Raw Data (PostgreSQL)
    ↓
Staging Layer (dbt views)
  • Clean and standardize data
  • Remove nulls and invalid records
    ↓
Core Layer (dbt tables)
  • Join claims with HCC/RxHCC mappings
  • Add business logic and enrichments
    ↓
Marts Layer (dbt tables)
  • Aggregate to member-level risk scores
  • Calculate final RAF scores
  • Categorize risk levels
```

### Technology Stack

- **Database:** PostgreSQL 14+
- **Transformation:** dbt Core 1.10
- **Data Generation:** Mockaroo (synthetic data)
- **Version Control:** Git/GitHub

---

## 📊 Data Model

### Source Tables (Raw Schema)

| Table | Description | Rows |
|-------|-------------|------|
| `members` | Member demographics and enrollment | 100 |
| `medical_claims` | Healthcare visits with diagnoses | 500 |
| `pharmacy_claims` | Prescription fills | 800 |
| `icd_to_hcc` | ICD-10 to HCC mapping table | 20 |
| `ndc_to_rxhcc` | NDC to RxHCC mapping table | 16 |

### dbt Models

#### Staging Layer (`analytics.stg_*`)
- `stg_members` - Cleaned member data with calculated age
- `stg_medical_claims` - Standardized medical claims
- `stg_pharmacy_claims` - Standardized pharmacy claims

#### Core Layer (`analytics.core_*`)
- `core_members` - Enriched members with age bands and risk categories
- `core_medical_claims` - Claims joined with HCC weights
- `core_pharmacy_claims` - Claims joined with RxHCC weights

#### Marts Layer (`analytics.*`)
- `member_hccs` - Unique HCC conditions per member per year
- `member_rxhccs` - Unique RxHCC medications per member per year
- `member_risk_scores` - **Final output: Complete risk scores per member**

---

## 🚀 Getting Started

### Prerequisites
```bash
# Required software
- PostgreSQL 14+
- Python 3.8+
- dbt Core 1.8+
```

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/medadvantage-raf-engine.git
cd medadvantage-raf-engine
```

2. **Set up Python environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install dbt-postgres
```

3. **Configure dbt profile**

Edit `~/.dbt/profiles.yml`:
```yaml
medadvantage_raf_engine:
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: your_username
      password: your_password
      dbname: medadvantage_raf
      schema: analytics
      threads: 4
  target: dev
```

4. **Load sample data**

Run the SQL scripts in `data/` folder to populate raw tables:
```bash
psql -d medadvantage_raf -f data/load_raw_data.sql
```

5. **Run dbt pipeline**
```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

  
Feel free to reach out or open an issue if you have questions about this project!
