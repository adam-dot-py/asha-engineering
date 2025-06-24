import pandas as pd
import mysql.connector
import json
from fuzzywuzzy import process

sexual_orientation_validations = [
    "ASEXUAL",
    "BISEXUAL",
    "DEMISEXUAL",
    "GAY",
    "HETEROSEXUAL",
    "LESBIAN",
    "PANSEXUAL",
    "QUEER",
    "OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

gender_validations = [
    "MALE",
    "FEMALE",
    "TRANSGENDER",
    "NON-BINARY",
    "OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

disability_validations = [
    "NO KNOWN DISABILITY",
    "A SPECIFIC LEARNING DIFFICULTY SUCH AS DYSLEXIA, DYSPRAXIA OR AD(H)D",
    "A SOCIAL/COMMUNICATION IMPAIRMENT SUCH AS ASPERGER'S SYNDROME/OTHER AUTISTIC SPECTRUM DISORDER",
    "A LONG STANDING ILLNESS OR HEALTH CONDITION SUCH AS CANCER, HIV, DIABETES, CHRONIC HEART DISEASE, OR EPILEPSY",
    "A MENTAL HEALTH CONDITION, SUCH AS DEPRESSION, SCHIZOPHRENIA OR ANXIETY DISORDER",
    "A PHYSICAL IMPAIRMENT OR MOBILITY ISSUES, SUCH AS DIFFICULTY USING ARMS OR USING A WHEELCHAIR OR CRUTCHES",
    "DEAF OR A SERIOUS HEARING IMPAIRMENT",
    "BLIND OR A SERIOUS VISUAL IMPAIRMENT UNCORRECTED BY GLASSES",
    "A DISABILITY, IMPAIRMENT OR MEDICAL CONDITION THAT IS NOT LISTED",
    "INFORMATION REFUSED",
    "NOT AVAILABLE",
    "OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

religion_validations = [
    "ATHEISM",
    "BUDDHISM",
    "CHRISTIANITY",
    "HINDUISM",
    "ISLAM",
    "JUDAISM",
    "SIKHISM",
    "OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

ethnicity_validations = [
    "AFRICAN",
    "ANY OTHER ASIAN BACKGROUND",
    "ANY OTHER BLACK, BLACK BRITISH, OR CARIBBEAN BACKGROUND",
    "ANY OTHER MIXED OR MULTIPLE ETHNIC BACKGROUND",
    "ANY OTHER WHITE BACKGROUND",
    "ARAB",
    "BANGLADESHI",
    "CARIBBEAN",
    "CHINESE",
    "ENGLISH, WELSH, SCOTTISH, NORTHERN IRISH OR BRITISH",
    "GYPSY OR IRISH TRAVELLER",
    "INDIAN",
    "IRISH",
    "PAKISTANI",
    "ROMA",
    "WHITE AND ASIAN",
    "WHITE AND BLACK AFRICAN",
    "WHITE AND BLACK CARIBBEAN",
    "OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

nationality_validations = [
    "1-BRITISH",
    "2-AFGHAN",
    "3-ALBANIAN",
    "4-ALGERIAN",
    "5-AMERICAN",
    "6-ANDORRAN",
    "7-ANGOLAN",
    "8-ANTIGUANS",
    "9-ARGENTINEAN",
    "10-ARMENIAN",
    "11-AUSTRALIAN",
    "12-AUSTRIAN",
    "13-AZERBAIJANI",
    "14-BAHAMIAN",
    "15-BAHRAINI",
    "16-BANGLADESHI",
    "17-BARBADIAN",
    "18-BARBUDANS",
    "19-BATSWANA",
    "20-BELARUSIAN",
    "21-BELGIAN",
    "22-BELIZEAN",
    "23-BENINESE",
    "24-BHUTANESE",
    "25-BOLIVIAN",
    "26-BOSNIAN",
    "27-BRAZILIAN",
    "28-BRUNEIAN",
    "29-BULGARIAN",
    "30-BURKINABE",
    "31-BURMESE",
    "32-BURUNDIAN",
    "33-CAMBODIAN",
    "34-CAMEROONIAN",
    "35-CANADIAN",
    "36-CAPE VERDEAN",
    "37-CENTRAL AFRICAN",
    "38-CHADIAN",
    "39-CHILEAN",
    "40-CHINESE",
    "41-COLOMBIAN",
    "42-COMORAN",
    "43-CONGOLESE",
    "44-COSTA RICAN",
    "45-CROATIAN",
    "46-CUBAN",
    "47-CYPRIOT",
    "48-CZECH",
    "49-DANISH",
    "50-DJIBOUTI",
    "51-DOMINICAN",
    "52-DUTCH",
    "53-EAST TIMORESE",
    "54-ECUADOREAN",
    "55-EGYPTIAN",
    "56-EMIRIAN",
    "57-EQUATORIAL GUINEAN",
    "58-ERITREAN",
    "59-ESTONIAN",
    "60-ETHIOPIAN",
    "61-FIJIAN",
    "62-FILIPINO",
    "63-FINNISH",
    "64-FRENCH",
    "65-GABONESE",
    "66-GAMBIAN",
    "67-GEORGIAN",
    "68-GERMAN",
    "69-GHANAIAN",
    "70-GREEK",
    "71-GRENADIAN",
    "72-GUATEMALAN",
    "73-GUINEA-BISSAUAN",
    "74-GUINEAN",
    "75-GUYANESE",
    "76-HAITIAN",
    "77-HERZEGOVINIAN",
    "78-HONDURAN",
    "79-HUNGARIAN",
    "80-I-KIRIBATI",
    "81-ICELANDER",
    "82-INDIAN",
    "83-INDONESIAN",
    "84-IRANIAN",
    "85-IRAQI",
    "86-IRISH",
    "87-ISRAELI",
    "88-ITALIAN",
    "89-IVORIAN",
    "90-JAMAICAN",
    "91-JAPANESE",
    "92-JORDANIAN",
    "93-KAZAKHSTANI",
    "94-KENYAN",
    "95-KITTIAN AND NEVISIAN",
    "96-KUWAITI",
    "97-KYRGYZ",
    "98-LAOTIAN",
    "99-LATVIAN",
    "100-LEBANESE",
    "101-LIBERIAN",
    "102-LIBYAN",
    "103-LIECHTENSTEINER",
    "104-LITHUANIAN",
    "105-LUXEMBOURGER",
    "106-MACEDONIAN",
    "107-MALAGASY",
    "108-MALAWIAN",
    "109-MALAYSIAN",
    "110-MALDIVIAN",
    "111-MALIAN",
    "112-MALTESE",
    "113-MARSHALLESE",
    "114-MAURITANIAN",
    "115-MAURITIAN",
    "116-MEXICAN",
    "117-MICRONESIAN",
    "118-MOLDOVAN",
    "119-MONACAN",
    "120-MONGOLIAN",
    "121-MOROCCAN",
    "122-MOSOTHO",
    "123-MOTSWANA",
    "124-MOZAMBICAN",
    "125-NAMIBIAN",
    "126-NAURUAN",
    "127-NEPALESE",
    "128-NEW ZEALANDER",
    "129-NI-VANUATU",
    "130-NICARAGUAN",
    "131-NIGERIAN",
    "132-NIGERIEN",
    "133-NORTH KOREAN",
    "134-NORTHERN IRISH",
    "135-NORWEGIAN",
    "136-OMANI",
    "137-PAKISTANI",
    "138-PALAUAN",
    "139-PANAMANIAN",
    "140-PAPUA NEW GUINEAN",
    "141-PARAGUAYAN",
    "142-PERUVIAN",
    "143-POLISH",
    "144-PORTUGUESE",
    "145-QATARI",
    "146-ROMANIAN",
    "147-RUSSIAN",
    "148-RWANDAN",
    "149-SAINT LUCIAN",
    "150-SALVADORAN",
    "151-SAMOAN",
    "152-SAN MARINESE",
    "153-SAO TOMEAN",
    "154-SAUDI",
    "155-SCOTTISH",
    "156-SENEGALESE",
    "157-SERBIAN",
    "158-SEYCHELLOIS",
    "159-SIERRA LEONEAN",
    "160-SINGAPOREAN",
    "161-SLOVAKIAN",
    "162-SLOVENIAN",
    "163-SOLOMON ISLANDER",
    "164-SOMALI",
    "165-SOUTH AFRICAN",
    "166-SOUTH KOREAN",
    "167-SPANISH",
    "168-SRI LANKAN",
    "169-SUDANESE",
    "170-SURINAMER",
    "171-SWAZI",
    "172-SWEDISH",
    "173-SWISS",
    "174-SYRIAN",
    "175-TAIWANESE",
    "176-TAJIK",
    "177-TANZANIAN",
    "178-THAI",
    "179-TOGOLESE",
    "180-TONGAN",
    "181-TRINIDADIAN OR TOBAGONIAN",
    "182-TUNISIAN",
    "183-TURKISH",
    "184-TUVALUAN",
    "185-UGANDAN",
    "186-UKRAINIAN",
    "187-URUGUAYAN",
    "188-UZBEKISTANI",
    "189-VENEZUELAN",
    "190-VIETNAMESE",
    "191-WELSH",
    "192-YEMENITE",
    "193-ZAMBIAN",
    "194-ZIMBABWEAN",
    "195-OTHER OR NON-DISCLOSURE",
    "UNKNOWN"
]

spoken_language_validations = [
    "ARABIC",
    "BENGALI",
    "CHINESE",
    "DUTCH",
    "ENGLISH",
    "FILIPINO",
    "FRENCH",
    "GERMAN",
    "HINDI",
    "ITALIAN",
    "JAPANESE",
    "KOREAN",
    "MARATHI",
    "OTHER",
    "PERSIAN",
    "POLISH",
    "PORTUGUESE",
    "RUSSIAN",
    "SPANISH",
    "TAMIL",
    "TELUGU",
    "THAI",
    "TURKISH",
    "UKRAINIAN",
    "URDU",
    "VIETNAMESE",
    "OTHER OR NON-DISCLOSURE",
    "ROMANIAN",
    "UNKNOWN"
]

risk_assessment_validations = [
    "LOW",
    "MEDIUM",
    "HIGH"
]

def fuzzy_lookup(x, column):
    
    if column == "SexualOrientation":
        match = process.extractOne(x, sexual_orientation_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if column == "Gender":
        match = process.extractOne(x, gender_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if column == "Disability":
        match = process.extractOne(x, disability_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if column == "Religion":
        match = process.extractOne(x, religion_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if column == "Nationality":
        match = process.extractOne(x, nationality_validations)
        best_match = match[0]
        similarity_score = match[1]
        
    if column == "Ethnicity":
        match = process.extractOne(x, ethnicity_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if column == "SpokenLanguage":
        match = process.extractOne(x, spoken_language_validations)
        best_match = match[0]
        similarity_score = match[1]
        
    if column == "RiskAssessment":
        match = process.extractOne(x, risk_assessment_validations)
        best_match = match[0]
        similarity_score = match[1]
    
    if similarity_score >= 90:
        return str(best_match)
    else: 
        return "OTHER OR NON-DISCLOSURE"
    
def replace_text(text):
  """Escapes single quotes within a string for safe MySQL insertion."""
  return text.replace("'", "\\'")

def cleanup_data(host, user, root_pass, base_table):
    """_docstring
    
    """

    column_checks = ['SexualOrientation', 'Disability', 'Gender', 'Religion', 'Nationality', 'Ethnicity', 'SpokenLanguage', "RiskAssessment"]

    for column in column_checks:
        
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
            resolved_value = replace_text(fuzzy_lookup(input_value, column))
            
            if input_value != resolved_value:
                try:
                    # Update the table via garbage collector
                    query = f"""
                    UPDATE base.{base_table}
                    SET {column} = CASE 
                        WHEN {column} IN ('{input_value}') THEN '{resolved_value}' 
                        ELSE {column} 
                    END;            
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
    
    cleanup_data(host=host, user=user, root_pass=root_pass, base_table=base_table)