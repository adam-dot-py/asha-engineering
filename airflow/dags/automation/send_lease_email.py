# packages
import duckdb
import time
import json
import pandas as pd
import smtplib
from functools import wraps
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# import motherduck token and target source config
target_source_config = "/home/asha/airflow/target-source-config.json"
server_config = "/home/asha/airflow/duckdb-config.json"
    
with open(target_source_config, "r") as t_con:
    target_config = json.load(t_con)

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']

def log_execution(func):
    """
    """
    
    @wraps(func)
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

def motherduck_connection(token):
    """_docstring
    """
    
    def connection_decorator(func):
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # pass con as a keyword argument for use in other functions
            return func(*args, con=con, **kwargs)
    
        return wrapper
    return connection_decorator

@log_execution
@motherduck_connection(token=token)
def send_lease_expiry_notification(token, schema, base_table_name, con, **kwargs):
    """
    """
    
    # establish connection
    con.sql("use asha_production;")
    # email setup
    email_config = "/home/asha/airflow/email-config.json"
    with open(email_config, 'r') as fp:
        email_config = json.load(fp)
        
    smtp_server = email_config.get('smtp_server')
    smtp_port = email_config.get('smtp_port')
    email_sender = email_config.get('email_sender')
    email_password = email_config.get('email_password')
    email_recipient = email_config.get('email_recipient')
    
    # estbalish dates
    current_date = datetime.today()
    threshold_date = current_date + timedelta(days=28)
    load_date = datetime.now()
    
    base_query = f"""
      select
        SupportProviders,
        PropertyAddress,
        LeaseEndDate
      from {schema}.{base_table_name}
    """
    
    df = con.sql(base_query).df()    
    expiring_leases = df[(df['LeaseEndDate'] >= current_date) & (df['LeaseEndDate'] <= threshold_date)]
    
    if not expiring_leases.empty:
    
        # expiring_leases_list = ["123 Test Road, London", "321 Another Test, London"]
        expiring_leases_list = [""]
        for _, row in expiring_leases.iterrows():
            property_address = row['PropertyAddress']
            support_provider = row['SupportProviders']
            lease_end_date = row['LeaseEndDate'].strftime(format='%d %B %Y')
            delta = row['LeaseEndDate'] - current_date
            expiring_lease_value = f"{support_provider}: {property_address} expires in {delta.days} days on {lease_end_date}"
            expiring_leases_list.append(expiring_lease_value)
        
        expiring_leases_fmt = '\n'.join(expiring_leases_list)
        
        # send email
        formatted_current_date = current_date.strftime("%d %B %Y")
        subject = f"Upcoming Expiring leases: {formatted_current_date}"
        body = f"Hi,\n\nThe following leases will expire within 28 days:\n{expiring_leases_fmt}\n\nThank you"
        msg = MIMEMultipart()
        msg['From'] = email_sender
        msg['To'] = ', '.join(email_recipient)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            # server.ehlo("ash-shahada.org")
            server.starttls()
            # server.ehlo("ash-shahada.org")
            server.login(email_sender, email_password)
            server.sendmail(email_sender, email_recipient, msg.as_string())
        
        print("Email sent")
        
    # else:
    #     print("No email to send")
        # # REMOVE AFTER TEST
        # # send email
        # formatted_current_date = current_date.strftime("%d %B %Y")
        # subject = f"TEST"
        # body = f"TEST SEND"
        # msg = MIMEMultipart()
        # msg['From'] = email_sender
        # msg['To'] = ', '.join(email_recipient)
        # msg['Subject'] = subject
        # msg.attach(MIMEText(body, 'plain'))
        
        # with smtplib.SMTP(smtp_server, smtp_port) as server:
        #     server.ehlo("ash-shahada.org")
        #     server.starttls()
        #     server.ehlo("ash-shahada.org")
        #     server.login(email_sender, email_password)
        #     server.sendmail(email_sender, email_recipient, msg.as_string())
    
if __name__ == "__main__":
    
    # this is the ETL task
    schema = 'bronze'
    base_table_name = 'dbo_lease_database'

    send_lease_expiry_notification(
        token=token,
        schema=schema,
        base_table_name=base_table_name
    )