# ASEAN Food Price ETL & Dashboard

A lightweight ETL pipeline and interactive dashboard to analyze ASEAN food prices using **PySpark**, **DuckDB**, **Streamlit**, and **Hugging Face Datasets**.

---

## 🌍 Dataset

- **Source**: [WFP Global Food Prices](https://data.humdata.org/dataset/global-wfp-food-prices)  
- **Processed Data**: [ASEAN Food ETL Dataset on Hugging Face](https://huggingface.co/datasets/qindea/asean-food-etl-data)

---

## 🚀 Quick Start

### 1️⃣ Setup

```bash
git clone https://github.com/qiqinn/asean-food-etl.git
cd asean-food-etl
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2️⃣ Run ETL

```bash
streamlit run app/dashboard.py
```

### 🗃️ Features

* Currency conversion to USD
* Commodity name standardization
* Data partitioned by country
* Dashboard with filters & charts
* Reads data directly from Hugging Face


### 📦 Tech Stack

* PySpark
* DuckDB
* Streamlit
* Hugging Face Hub