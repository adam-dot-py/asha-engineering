# packages
import duckdb
import json
from rapidfuzz import fuzz
from functools import wraps

# dictionary containing keywords
categories = {
    'Local Councils': [
        'council', 'board', 'municipality', 'borough',
        'district', 'authority', 'parish', 'county',
        'local government', 'city council', 'township',
        'city council', 'bcc', 'birmingham city council', 'sandwell council',
        'walsall council', 'dudley council', 'solihull council', 'warwick council',
        'stafford bc', 'wolverhampton council', 'telford council', 'tamworth council',
        'stratford council', 'bromsgrove council', 'wychavon district council',
        'newcastle under lyme council', 'newcastle-under-lyme council',
        'derbyshire county', 'milton keynes council', 'redbridge', 'south tyneside council',
        'evesham council', 'lichfield council', 'malvern council', 'bradford council'
    ],
    'Hospitals': [
        'hospital', 'gp', 'general practitioner', 'health',
        'nhs', 'clinic', 'medical', 'urgent care',
        'surgery', 'care center', 'emergency', 'hospitals', 'nhs',
        'qe', 'qe hospital', 'heartlands hospital', 'sandwell hospital',
        'seacole hospital', 'medical discharge', 'crisis care team',
        'crisis team', 'mental health', 'nhs care centre', 'nhs mental health',
        'gp surgery', 'gp surgrey', 'health care angels', 'birmingham crisis team'
    ],
    'Police': [
        'police', 'law enforcement', 'constabulary', 'officer',
        'crime unit', 'metropolitan police', 'sheriff',
        'detective', 'patrol', 'police department', 'community police',
        'west midlands police', 'police safeguarding team', 'perry barr police'
    ],
    'Probation and Justice': [
        'probation', 'prison', 'hmp', 'justice', 'offender',
        'probation service', 'probation services', 'probation officer',
        'probation practitioner', 'home office accommodation', 'nacro',
        'restart scheme', 'hm prison & probation service', 'justice service'
    ],
    'Housing and Support': [
        'housing', 'housing association', 'homes', 'home', 'property', 'properties',
        'support', 'supported', 'lodge', 'cic', 'limited', 'ltd', 'smartmove',
        'room match', 'select homes', 'reliance housing', 'newlife housing',
        'trident', 'trident reach', 'aspect housing', 'comfort home',
        'comfort homes', 'phoenix residences', 'alpha housing', 'midland heart',
        'midlands housing', 'hyde housing', 'one way homes', 'clear housing',
        'elite support & housing', 'elite social housing', 'dawson housing',
        'ash-shahada', 'ash shahada', 'blue dome', 'bluedome', 'nch', 'nch-cic',
        'nch cic', 'nch - cic', 'nh-cic', 'nh cic', 'nc-cic', 'cqc housing',
        'cqc hosing', 'nch-cic', 'secured housing', 'secured housing'
    ],
    'Charities': [
        'relief', 'trust', 'mosque', 'trinity',
        'foundation', 'non-profit', 'ngo', 'charity',
        'fundraiser', 'aid', 'humanitarian', 'salvation army', 'homeless',
        'foodbank', 'shelter', 'food bank', 'feed', 'feedo', 'aid',
        'st basils', 'st basil', 'st. basils', 'st basail', 'sifa', 'sifa fireside',
        'as suffa', 'as-suffa', 'as-shada', 'ash shada', 'refugee', 'refugee action',
        'refugee & migrant centre', 'refugee and migrant center', 'women aid',
        'womens aid', 'woman''s aid', 'birmingham & solihull womens',
        'birmingham mind', 'mind', 'st giles trust', 'p3', 'p3 charity',
        'wolverhampton charity', 'sikh recovery network', 'bswaid', 'bswaid/hb',
        'narthex', 'change, grow, live', 'cgl', 'helping hands', 'giving hands mission',
        'bradford', 'food bank', 'foodbank', 'trussel trust', 'trussell trust',
        'crisis', 'crisis care', 'homeless pathway', 'homeless team', 'homelessness'
    ],
    'Self Referral' : [
        'self referral', 'self', 'self-referral', 'self referal', 'self refferal',
        'self refferral', 'self ref', 'self-ref', 's/referral', 's ref', 'walk in',
        'walk in''s', 'walk in s', 'call in', 'cal in', 'call in', 'direct referral',
        'phone referral', 'internet search', 'online search', 'website search',
        'website referral', 'google', 'facebook', 'word of mouth', 'friend',
        'by friend', 'refered by friend', 'referred by friend', 'tenant referral',
        'refferal from tenant', 'referral from tenant', 'new arrival', 'new arrival. to be signed up. direct referral.',
        'previous tenant', 'previous service user', 'previous resident', 'previous client'
    ],
    'Internal Move': [
        'internal move', 'internal transfer', 'internal swap', 'internal reference',
        'in-house transfer', 'in house', 'change of landlord', 'change of flat',
        'change the flat', 'moved from', 'moved from another hostel', 'moved from another property',
        'tenant already in property', 'move from', 'change of circumstances', 'change of address',
        'change the flat from flat', 'previous tenant, self referal', 'coc'
    ],
    'Referral and Access': [
        'referral', 'referall', 'refferal', 'refered', 'referal', 'direct', 'access',
        'options', 'housing options', 'housing option', 'support worker', 'social worker',
        'homeless pathway team', 'homeless pathway officer', 'homeless team',
        'homeless department', 'homeless concern', 'homeless centre', 'homeless',
        'outreach', 'out reach', 'broker', 'hub', 'mailbox', 'recommend', 'recommended',
        'network', 'connections', 'connection support', 'room match', 'job centre',
        'job centre plus', 'ad', 'advertising', 'online contact', 'website check'
    ],
    'Other' : [
        'other provider', 'other house', 'other housing', 'other including housing associations',
        'other', 'unknown', 'not known', 'vacant', 'local', 'provider', 'support wrker',
        'support worker', 'staff', 'aspect staff', 'previous service user / internal move',
        'internal', 'home office', 'home office accommodation', 'pr', 'bb', 'p', 's'
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
            return 'Other'
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

# def motherduck_connection(token):
#     def connection_decorator(func):
#         con = duckdb.connect(f'md:?motherduck_token={token}')
        
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             # pass con as a keyword argument for use in other functions
#             return func(*args, con=con, **kwargs)
    
#         return wrapper
#     return connection_decorator

# @motherduck_connection(token=token)
def fuzzy_group_data(schema, table_name, column, **kwargs):
    """_docstring
    
    """
        
    # connect to motherduck
    con = duckdb.connect('~/airflow/database/asha_prod.duckdb')

    # get the bronze table
    df = con.sql(f"SELECT * FROM {schema}.{table_name};").df()
    df = df.drop_duplicates()
    
    # update it and find matches
    df[column] = df[column].astype(str).apply(lambda x: category_match(replace_text(x), categories=categories))
    
    # close motherduck
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * from df;")
    con.close()

if __name__ == "__main__":

    # this is the ETL task
    schema = 'main_silver'
    table_name = None
    column = None
    group_column_name = None
    
    fuzzy_group_data(
        token=token,
        schema=schema,
        table_name=table_name,
        column=column,
        group_column_name=group_column_name
    )