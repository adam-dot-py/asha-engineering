import pandas as pd
import difflib
import json

# import server config file
provider_config = "/home/asha/airflow/support-providers-config.json"

with open(provider_config, "r") as fp:
  config = json.load(fp)
  
# Load the JSON data into a Python dictionary
data = json.loads(config)

# Extract the list of housing providers
provider_list = config['housing_providers']


def get_closest_match(value):
    # Find the closest match in the provider list
    matches = difflib.get_close_matches(value, provider_list, n=1, cutoff=0.95)  # Adjust cutoff as needed
    return matches[0] if matches else value  # Return the closest match or the original value if no match
    