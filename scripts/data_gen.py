import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_user_data(user_id, start_date, num_days):
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    data = []

    for date in dates:
        fitness = random.choice(['Fit', 'Average', 'At Risk'])
        
        if fitness == 'Fit':
            resting_hr = np.random.normal(58, 3)
            heart_rate = np.random.normal(65, 5)
            walking_hr = np.random.normal(90, 6)
            hrv = np.random.normal(60, 8)
            walk_km = np.random.normal(10, 2)
            kcal = np.random.normal(500, 60)
            vo2_max = np.random.normal(45, 5)
            flights = np.random.normal(12, 3)
        elif fitness == 'Average':
            resting_hr = np.random.normal(70, 5)
            heart_rate = np.random.normal(75, 6)
            walking_hr = np.random.normal(100, 7)
            hrv = np.random.normal(40, 6)
            walk_km = np.random.normal(6, 1.5)
            kcal = np.random.normal(350, 50)
            vo2_max = np.random.normal(35, 4)
            flights = np.random.normal(8, 2)
        else:
            resting_hr = np.random.normal(80, 6)
            heart_rate = np.random.normal(85, 7)
            walking_hr = np.random.normal(110, 8)
            hrv = np.random.normal(20, 5)
            walk_km = np.random.normal(2, 1)
            kcal = np.random.normal(200, 30)
            vo2_max = np.random.normal(28, 3)
            flights = np.random.normal(3, 1)

        data.append({
            'user_id': user_id,
            'date': date.strftime('%Y-%m-%d'),
            'RestingHeartRate': round(resting_hr, 1),
            'HeartRate': round(heart_rate, 1),
            'WalkingHeartRate': round(walking_hr, 1),
            'HeartRateVariability': round(hrv, 1),
            'DistanceWalkingRunning': round(walk_km, 2),
            'ActiveEnergyBurned': round(kcal, 1),
            'VO2Max': round(vo2_max, 1),
            'FlightsClimbed': round(flights, 0),
            'cardio_status': fitness
        })

    return data

# === Generate Data ===
all_data = []
for i in range(100):
    uid = f"u{i+1:03}"
    all_data += generate_user_data(uid, datetime(2024, 1, 1), 90)

df = pd.DataFrame(all_data)

os.makedirs("../data/raw", exist_ok=True)
df.to_csv("../data/raw/synthetic_cardio_health_data.csv", index=False)

print("✅ Synthetic cardio health data saved to '../data/raw/synthetic_cardio_health_data.csv'")