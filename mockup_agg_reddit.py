import pandas as pd
import numpy as np
import random
from datetime import datetime
np.random.seed(42)
dates = pd.date_range(start="2024-01-01", end="2025-04-11", freq="D")
california_locations = [("Los Angeles", 34.05, -118.25), ("San Diego", 32.72, -117.16), ("San Jose", 37.33, -121.89), ("San Francisco", 37.77, -122.42), ("Fresno", 36.74, -119.78), ("Sacramento", 38.58, -121.49)]
other_locations = [("New York", 40.67, -73.94), ("Chicago", 41.84, -87.68), ("Phoenix", 33.54, -112.07), ("Philadelphia", 40.01, -75.13), ("Austin", 30.51, -97.68), ("Seattle", 47.62, -122.35), (None, None, None)]
all_locations = california_locations + other_locations
cutoff = pd.Timestamp("2025-02-01")
start_date = pd.Timestamp("2024-01-01")
end_date = pd.Timestamp("2025-04-11")
total_days = (end_date - start_date).days
def get_count(date, sentiment):
    days_from_start = (date - start_date).days
    mid_point = (cutoff - start_date).days
    if sentiment == "positive":
        scale = 1 - (days_from_start / mid_point) if date < cutoff else (days_from_start - mid_point) / (total_days - mid_point)
    else:
        scale = (days_from_start / mid_point) if date < cutoff else 1 - ((days_from_start - mid_point) / (total_days - mid_point))
    base = 500 if sentiment == "positive" else 300
    max_variation = 3500
    return int(base + scale * max_variation)
rows = []
for date in dates:
    for city, lat, lon in all_locations:
        is_california = (city, lat, lon) in california_locations
        loan_type = random.choice(["auto", "home"])
        if date < cutoff:
            sentiment = np.random.choice(["positive", "negative"], p=[0.3, 0.7]) if is_california else np.random.choice(["positive", "negative"], p=[0.5, 0.5])
        else:
            sentiment = np.random.choice(["positive", "negative"], p=[0.7, 0.3]) if is_california else np.random.choice(["positive", "negative"], p=[0.6, 0.4])
        count = get_count(date, sentiment)
        rows.append({
            "substring(processing_time, 1, 10)": date.strftime("%Y-%m-%d"),
            "loan_type": loan_type,
            "sentiment": sentiment,
            "City_latitude": lat,
            "City_longitude": lon,
            "count(1)": count
        })
df = pd.DataFrame(rows)
df.to_csv("loan_sentiment_data.csv", index=False)
print("✅ Data written to 'loan_sentiment_data.csv' with trend-based counts.")
