import json
from datetime import datetime

def extract_schema(data, prefix=''):
    schema = {}
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f'{prefix}.{key}' if prefix else key
            schema.update(extract_schema(value, full_key))
    elif isinstance(data, list):
        if data:
            # Assume first element represents the structure
            schema.update(extract_schema(data[0], prefix + '[]'))
        else:
            schema[prefix + '[]'] = 'EmptyList'
    else:
        schema[prefix] = type(data).__name__
    return schema

def load_and_inspect_schema(filepath):
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)
    
    # Directly extract schema from the full data structure, including top-level keys
    return extract_schema(data)  # Now this includes the full structure

# Extract schemas
schema_amsy = load_and_inspect_schema('sample-raw-data/AMSY_20250410_100010.json')
schema_sccu = load_and_inspect_schema('sample-raw-data/SCCU_20250410_100010.json')

# Print schemas as JSON
print("Unflattened schema for AMSY:")
print(json.dumps(schema_amsy, indent=4))

print("\nUnflattened schema for SCCU:")
print(json.dumps(schema_sccu, indent=4))
