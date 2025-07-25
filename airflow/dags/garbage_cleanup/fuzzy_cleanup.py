# packages
import duckdb
import json
from fuzzywuzzy import fuzz
from functools import wraps

# dictionary containing keywords
categories = {
    'Local Councils': [
        'council', 'board', 'municipality', 'borough', 
        'district', 'authority', 'parish', 'county', 
        'local government', 'city council', 'township'
    ],
    'Hospitals': [
        'hospital', 'gp', 'general practitioner', 'health', 
        'nhs', 'clinic', 'medical', 'urgent care', 
        'surgery', 'care center', 'emergency', 'hospitals', 'nhs'
    ],
    'Police': [
        'police', 'law enforcement', 'constabulary', 'officer', 
        'crime unit', 'metropolitan police', 'sheriff', 
        'detective', 'patrol', 'police department'
    ],
    'Charities': [
        'relief', 'trust', 'mosque', 'trinity', 
        'foundation', 'non-profit', 'ngo', 'charity', 
        'fundraiser', 'aid', 'humanitarian', 'salvation army', 'homeless',
        'foodbank', 'shelter', 'food bank', 'feed', 'feedo', 'aid'
    ],
    'Not Known' : [
        'unknown', 'not known'
    ],
    'Self Referral' : [
        'self referral', 'self'
    ]
}

# this is a custom class to capture errors
class NotTextError(Exception):
    "Raised if the value passed is not a string"
    pass


def category_match(name: str, categories: dict) -> str:
    """takes a string and compares it to a list of keywords taken from a dictionary, 
    returning the closest matching key (or category)
    
    Args
      name (str): the value to compare within the given keyword
      categories (dict): the dictionary containing returnable categories and associated keywords
      
    Returns:
    
      The category of the closest matching keyword to the given name
    """
    
    # iterate over the category and keywords in the given dictionary
    try:
        if isinstance(name, str):
            for category, keywords in categories.items():
                for keyword in keywords:
                    # compare the keyword and find the closest match, return the associated category
                    if fuzz.partial_ratio(keyword.lower(), name.lower()) > 80:
                        return category
            return 'Other including Housing Associations'
        else:
            raise NotTextError
    except NotTextError:
        print("Name variable is not a string")
        
def replace_text(text):
  """Escapes single quotes within a string for safe MySQL insertion."""
  return text.replace("'", "\\'")

# import motherduck token and target source config
server_config = "/home/asha/airflow/duckdb-config.json"

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']

def motherduck_connection(token):
    def connection_decorator(func):
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # pass con as a keyword argument for use in other functions
            return func(*args, con=con, **kwargs)
    
        return wrapper
    return connection_decorator

@motherduck_connection(token=token)
def fuzzy_group_data(bronze_schema, bronze_table_name, column, group_column_name, con, **kwargs):
    """_docstring
    
    """
        
    # connect to motherduck
    con.sql("USE asha_production;")

    # get the bronze table
    df = con.sql(f"SELECT * FROM {bronze_schema}.{bronze_table_name};").df()
    df = df.drop_duplicates()
    
    for _, row in df.iterrows():
        
        input_value = replace_text(str(row[column]))
        resolved_value = category_match(input_value, categories=categories)
        
        if input_value != resolved_value:
            try:
                # Update the table via garbage collector
                # query = f"""
                # update base.{base_table}
                # set {group_column_name} = case
                #     when {column} in ('{input_value}') then '{resolved_value}' 
                #     else {column} 
                # end;            
                # """
                
                query = f"""
                UPDATE {bronze_schema}.{bronze_table_name}
                SET {group_column_name} = '{resolved_value}'
                WHERE {column} = '{input_value}';
                """
                # write motherduck
                con.sql(query)

            except Exception as e:
                print(f"Err: {e}")
    
    # close motherduck
    con.close()

if __name__ == "__main__":

    # this is the ETL task
    bronze_schema = 'bronze'
    bronze_table_name = None
    column = None
    
    fuzzy_group_data(
        token=token,
        bronze_schema=bronze_schema,
        bronze_table_name=bronze_table_name,
        column=column,
        group_column_name='GroupedReferralAgency'
    )