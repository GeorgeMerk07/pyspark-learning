# PySpark Learning Journal ⚡

Hands-on practice with **Apache Spark (PySpark)**, built while preparing for my
MSc in Big Data Analytics. Each script takes the Titanic passenger dataset one
step further, from raw CSV to clean, model-ready data, using the same tools
used on real big-data pipelines.

## 🛠 Tech stack

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.2-E25A1C?logo=apachespark&logoColor=white)
![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)
![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1)

## 📊 Dataset

The classic **Titanic** dataset: 891 passengers, 12 columns, with the goal of
predicting `Survived` (0/1). It is small on purpose, so the focus stays on
Spark techniques that scale to data far larger than one machine's memory.

## 📁 Contents

| File | What it does |
|---|---|
| `01_load.py` | Loads the CSV, inspects the schema, row counts and summary statistics |
| `02_clean.py` | Counts missing values in every column in one pass, drops and imputes, engineers features |
| `ex2.py` | Independent exercise: mode and mean imputation, `IsAlone` feature, Parquet round-trip |

## 🧠 Skills practised

- Spark DataFrames: `select`, `filter`, `groupBy`, `withColumn`
- **Spark SQL** on temporary views
- Missing-value analysis and imputation with MLlib's `Imputer`
- Feature engineering (`FamilySize`, `IsAlone`)
- Columnar storage with **Parquet** (schema preserved on read-back)
- Spark's **lazy evaluation**: transformations vs actions

## 🔍 Key findings

- **177 of 891 ages (~20%) were missing.** Filled with the **median (28.0)**
  rather than the mean (29.7), because the median is less affected by outliers.
- `Cabin` was **77% empty**, so it was dropped rather than invented.
- The 2 missing `Embarked` values were filled with the most common port,
  **Southampton (S)**, identified with code rather than assumed.
- **537 passengers (60%) travelled alone**, a useful candidate feature for
  survival prediction.

## ▶️ How to run

```bash
pip install pyspark
python 01_load.py
```

Requires **Python 3.12** and **Java 21** (Spark runs on the JVM).

## 🗺 Roadmap

- [x] Loading and inspecting data
- [x] Cleaning and feature engineering
- [ ] Exploratory analysis with Spark SQL (joins, window functions)
- [ ] First ML model with Spark MLlib
- [ ] Model comparison and evaluation
