import pandas as pd
import numpy as np
import os

# === LOAD RAW CARDIO PARQUET ===
input_file = "../data/raw/apple_health_records.parquet"

# Check if file exists and has content
if not os.path.exists(input_file):
    print(f"❌ Error: Input file not found at '{input_file}'")
    raise FileNotFoundError(f"Input file not found: {input_file}")

if os.path.getsize(input_file) == 0:
    print(f"❌ Error: Input file '{input_file}' is empty")
    raise ValueError(f"Empty input file: {input_file}")

try:
    df = pd.read_parquet(input_file)
    if df.empty:
        print(f"❌ Error: No data found in '{input_file}'")
        raise ValueError(f"Empty DataFrame from {input_file}")
except Exception as e:
    print(f"❌ Error reading Parquet file: {str(e)}")
    print("Please ensure the file is a valid Parquet file and contains the required data")
    raise

# Clean column names
df.columns = [col.strip() for col in df.columns]
print("📄 Available columns:", df.columns.tolist())

# Validate required columns
required_cols = ["type", "value", "startDate"]
missing_cols = [col for col in required_cols if col not in df.columns]

if missing_cols:
    print(f"❌ Error: Missing required columns: {missing_cols}")
    print("Available columns:", df.columns.tolist())
    print("\nPlease ensure your input file contains the following columns:")
    print("- type: The type of health measurement")
    print("- value: The numerical value of the measurement")
    print("- startDate: The timestamp of the measurement")
    raise KeyError(f"Missing required columns: {missing_cols}")

# Data cleaning
df.dropna(subset=required_cols, inplace=True)
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df.dropna(subset=["value"], inplace=True)

# Handle dates
df["startDate"] = pd.to_datetime(df["startDate"], utc=True, errors="coerce")
if "endDate" in df.columns:
    df["endDate"] = pd.to_datetime(df["endDate"], utc=True, errors="coerce")
df.dropna(subset=["startDate"], inplace=True)

# Convert kJ to kcal if present
if "unit" in df.columns and "kJ" in df["unit"].unique():
    df.loc[df["unit"] == "kJ", "value"] = df["value"] / 4.184
    df.loc[df["unit"] == "kJ", "unit"] = "kcal"

# Create derived columns
df["date"] = df["startDate"].dt.date
df["metric"] = df["type"].str.replace("HKQuantityTypeIdentifier", "", regex=False)
if "sourceName" in df.columns:
    df["source"] = df["sourceName"].fillna("Unknown").str.strip().str.lower()

# Create summary statistics
summary = df.groupby(["date", "metric"]).agg(
    avg_value=("value", "mean"),
    min_value=("value", "min"),
    max_value=("value", "max"),
    count=("value", "count")
).reset_index()

# Create pivot table
pivot = summary.pivot(index="date", columns="metric", values="avg_value").reset_index()
pivot.columns = [str(c).strip().replace(" ", "").replace("\\", "") for c in pivot.columns]

# Save cleaned data
try:
    pivot.to_parquet("../data/clean/clean_cardio_data.parquet", index=False)
    print("✅ Cleaned data saved to 'data/clean/clean_cardio_data.parquet'")
except Exception as e:
    print(f"❌ Error saving cleaned data: {str(e)}")
    raise

# === Prepare ML-Ready Dataset ===
ml_required_cols = [
    "RestingHeartRate",
    "HeartRate",
    "WalkingHeartRate",
    "VO2Max",
    "DistanceWalkingRunning"
]

ml_optional_cols = [
    "ActiveEnergyBurned",
    "FlightsClimbed"
]

available_required_cols = [col for col in ml_required_cols if col in pivot.columns]
missing_required_cols = set(ml_required_cols) - set(pivot.columns)

if missing_required_cols:
    print(f"⚠️ Warning: Missing expected features: {missing_required_cols}")

if not available_required_cols:
    print("❌ Error: None of the required ML features are available in the data")
    raise KeyError("No required ML features found in the data")

ml_df = pivot.dropna(subset=available_required_cols)

# Handle optional columns
for col in ml_optional_cols:
    if col in ml_df.columns:
        ml_df[col].fillna(ml_df[col].mean(), inplace=True)

# Save ML-ready data
try:
    ml_df.to_parquet("../data/clean/ml_ready_cardio_data.parquet", index=False)
    print("✅ ML-ready data saved to 'data/clean/ml_ready_cardio_data.parquet'")
except Exception as e:
    print(f"❌ Error saving ML-ready data: {str(e)}")
    raise