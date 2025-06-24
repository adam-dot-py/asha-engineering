# packages
import pandas as pd
import json
import mysql.connector
from fuzzywuzzy import fuzz

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

def fuzzy_group_data(host, user, root_pass, base_table, column, group_column_name):
    """_docstring
    
    """
        
    # Establish connection to base
    base_db = mysql.connector.connect(
        host=host,
        user=user,
        password=root_pass,
        database='base'
    )

    # attached to base
    base_cursor = base_db.cursor()
    
    base_query = f"select {column} from {base_table};"
    base_cursor.execute(base_query)

    # Fetch all the results
    base_result = base_cursor.fetchall()

    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
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
                UPDATE base.{base_table}
                SET {group_column_name} = '{resolved_value}'
                WHERE {column} = '{input_value}';
                """
                # create_query = row['Query']
                base_cursor.execute(query)
                print(f"Executed -> {query}")
            except Exception as e:
                print(f"Err: {e}")
    
    # commit changes and close connections
    base_db.commit()
    base_cursor.close()
    base_db.close()

if __name__ == "__main__":

    # get server config details
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, 'r') as fp:
        config = json.load(fp)

    host = config.get('host')
    user = config.get('user')
    root_pass = config.get('root_pass')
    base_table = None
    column = None
    
    fuzzy_group_data(host=host, user=user, root_pass=root_pass, base_table=base_table, column=column, group_column_name='GroupedReferralAgency')