import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_user_data(user_id, start_date, num_days):
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    data = []

    for date in dates:
        # Generate all health metrics with realistic overlapping ranges
        resting_hr = np.random.normal(70, 10)
        vo2_max = np.random.normal(35, 7)
        walk_km = np.random.normal(5, 3)
        kcal = walk_km * np.random.normal(40, 10)
        flights = max(0, np.random.normal(8, 5))
        heart_rate = resting_hr + np.random.normal(10, 6)
        walking_hr = resting_hr + np.random.normal(25, 10)
        hrv = np.random.normal(40, 15)

        
        if random.random() < 0.05:
            vo2_max = None
        if random.random() < 0.02:
            resting_hr += random.uniform(20, 40)

        
        if vo2_max is not None and (resting_hr > 78 or vo2_max < 30):
            label = "At Risk"
        elif vo2_max is not None and resting_hr < 62 and vo2_max > 42:
            label = "Fit"
        else:
            label = "Average"

        if random.random() < 0.1:
            label = random.choice(["Fit", "Average", "At Risk"])

        data.append({
            'user_id': user_id,
            'date': date.strftime('%Y-%m-%d'),
            'RestingHeartRate': round(resting_hr, 1),
            'HeartRate': round(heart_rate, 1),
            'WalkingHeartRate': round(walking_hr, 1),
            'HeartRateVariability': round(hrv, 1),
            'DistanceWalkingRunning': round(walk_km, 2),
            'ActiveEnergyBurned': round(kcal, 1),
            'VO2Max': round(vo2_max, 1) if vo2_max is not None else None,
            'FlightsClimbed': round(flights),
            'cardio_status': label
        })

    return data

# === Generate Data ===
all_data = []
for i in range(100):
    uid = f"u{i+1:03}"
    all_data += generate_user_data(uid, datetime(2018, 1, 1), 2555)

df = pd.DataFrame(all_data)

os.makedirs("../data/raw", exist_ok=True)
df.to_csv("../data/raw/synthetic_cardio_health_data.csv", index=False)

print("✅ Realistic synthetic cardio health data saved to '../data/raw/synthetic_cardio_health_data.csv'")
