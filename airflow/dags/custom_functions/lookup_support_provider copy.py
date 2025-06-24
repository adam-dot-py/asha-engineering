import json
from fuzzywuzzy import process, fuzz

def lookup_support_provider(value):
    """_docstring
    """
    
    # import server config file
    provider_config = "/home/asha/airflow/support-providers-config.json"

    with open(provider_config, "r") as fp:
      config = json.load(fp)
    
    # Extract the list of housing providers
    provider_list = config['housing_providers']
    
    matches = [(fuzz.token_sort_ratio(value, item), item) for item in provider_list]
    score, word = max(matches, key=lambda x: x[0])
    
    print(matches)
    
    # word, score = process.extractOne(value, provider_list)
    
    return word
    
    # if score >= 90:
    #   return word
    # else:
    #   return value
    
print(lookup_support_provider("SAIF SOCIAL AND HEALTHCARE HOMES"))