import json
import pandas as pd
import re
from datetime import datetime

# Flattening function stays the same
def flatten_json(y):
    flat = {}
    if isinstance(y, dict):
        for key, val in y.items():
            if isinstance(val, (dict, list)):
                nested = flatten_json(val)
                for nested_key, nested_val in nested.items():
                    flat[f'{nested_key}'] = nested_val
            else:
                flat[key] = val
    elif isinstance(y, list):
        flat['List'] = [flatten_json(item) for item in y]
    else:
        flat = y
    return flat

# Updated load_and_flatten with timestamp as formatted string
def load_and_flatten(filepath):
    match = re.search(r'_(\d{8}_\d{6})\.json$', filepath)
    timestamp_str = match.group(1) if match else None

    # Format to readable string like "2025-04-05 01:50:06"
    formatted_timestamp = (
        datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        if timestamp_str else None
    )

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)
    vehicle_activities = data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"]["VehicleActivity"]

    flattened_data = []
    for activity in vehicle_activities:
        flat = flatten_json(activity)
        flat['FileTimestamp'] = formatted_timestamp  # ⬅️ now it's a string like '2025-04-05 01:50:06'
        flattened_data.append(flat)

    return pd.DataFrame(flattened_data)


# Load and flatten both JSON files
df1 = load_and_flatten('sample-raw-data/AMSY_20250410_100010.json')
df2 = load_and_flatten('sample-raw-data/SCCU_20250410_100010.json')

# Compare column sets
cols1 = set(df1.columns)
cols2 = set(df2.columns)

common_cols = cols1 & cols2
only_in_df1 = cols1 - cols2
only_in_df2 = cols2 - cols1

# Print comparison results
print(f"Total columns in AMSY: {len(cols1)}")
print(f"Total columns in SCCU: {len(cols2)}")
print(f"Common columns: {len(common_cols)}")
print(f"Columns only in AMSY:\n{only_in_df1}")
print(f"Columns only in SCCU:\n{only_in_df2}")


combined_df = pd.concat([df1, df2], ignore_index=True)

print(combined_df.head())
print(f"Columns only in SCCU:\n{combined_df.columns}")
print(f'Total rows: {len(combined_df)}')
print(f'Total columns after combining: {len(combined_df.columns)}')

combined_df.to_csv("sample-raw-data/bus_data_with_file_timestamp.csv", index=False)
