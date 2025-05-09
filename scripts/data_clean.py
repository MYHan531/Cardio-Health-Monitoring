import pandas as pd
import numpy as np

# === LOAD RAW CARDIO CSV ===
df = pd.read_csv("../data/raw/synthetic_cardio_health_data.csv", encoding="cp1252")
# df = pd.read_csv("../data/raw/parsed_export.csv", encoding="cp1252")

df.columns = [col.strip() for col in df.columns]
print("📄 Available columns:", df.columns.tolist())

# required_cols = ["type", "value", "startDate"]
# available_cols = [col for col in required_cols if col in df.columns]

# if available_cols:
#     df.dropna(subset=available_cols, inplace=True)
# else:
#     raise KeyError(f"Missing all required columns: {required_cols}")


df.dropna(subset=["type", "value", "startDate"], inplace=True)
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df.dropna(subset=["value"], inplace=True)

df["startDate"] = pd.to_datetime(df["startDate"], utc=True, errors="coerce")
df["endDate"] = pd.to_datetime(df["endDate"], utc=True, errors="coerce")
df.dropna(subset=["startDate"], inplace=True)

if "kJ" in df["unit"].unique():
    df.loc[df["unit"] == "kJ", "value"] = df["value"] / 4.184
    df.loc[df["unit"] == "kJ", "unit"] = "kcal"

df["date"] = df["startDate"].dt.date
df["metric"] = df["type"].str.replace("HKQuantityTypeIdentifier", "", regex=False)
df["source"] = df["sourceName"].fillna("Unknown").str.strip().str.lower()

summary = df.groupby(["date", "metric"]).agg(
    avg_value=("value", "mean"),
    min_value=("value", "min"),
    max_value=("value", "max"),
    count=("value", "count")
).reset_index()

pivot = summary.pivot(index="date", columns="metric", values="avg_value").reset_index()
pivot.columns = [str(c).strip().replace(" ", "").replace("\\", "") for c in pivot.columns]

pivot.to_csv("../data/clean/clean_cardio_data.csv", index=False)
print("✅ Cleaned data saved to 'data/clean/clean_cardio_data.csv'")

# === Prepare ML-Ready Dataset ===

required_cols = [
    "RestingHeartRate",
    "HeartRate",
    "WalkingHeartRate",
    "VO2Max",
    "DistanceWalkingRunning"
]

optional_cols = [
    "ActiveEnergyBurned",
    "FlightsClimbed"
]

available_required_cols = [col for col in required_cols if col in pivot.columns]
missing_required_cols = set(required_cols) - set(pivot.columns)

if missing_required_cols:
    print(f"⚠️ Warning: Missing expected features: {missing_required_cols}")

ml_df = pivot.dropna(subset=available_required_cols)

for col in optional_cols:
    if col in ml_df.columns:
        ml_df[col].fillna(ml_df[col].mean(), inplace=True)

ml_df.to_csv("../data/clean/ml_ready_cardio_data.csv", index=False)
print("✅ ML-ready data saved to 'data/clean/ml_ready_cardio_data.csv'")
