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

def send_lease_expiry_notification(schema, silver_table, **kwargs):
    """
    """
    
    # establish connection
    con = duckdb.connect('/home/asha/airflow/database/asha_prod.duckdb')

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
        support_providers,
        property_address,
        lease_end_date
      from {schema}.{silver_table}
    """
    
    df = con.sql(base_query).df()    
    expiring_leases = df[(df['lease_end_date'] >= current_date) & (df['lease_end_date'] <= threshold_date)]
    
    if not expiring_leases.empty:
    
        # expiring_leases_list = ["123 Test Road, London", "321 Another Test, London"]
        expiring_leases_list = [""]
        for _, row in expiring_leases.iterrows():
            property_address = row['property_address']
            support_provider = row['support_providers']
            lease_end_date = row['lease_end_date'].strftime(format='%d %B %Y')
            delta = row['lease_end_date'] - current_date
            expiring_lease_value = f"- {support_provider}: {property_address} expires in {delta.days} days on {lease_end_date}"
            expiring_leases_list.append(expiring_lease_value)
        
        expiring_leases_fmt = '\n\n'.join(expiring_leases_list)
        
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
    schema = 'main_silver'
    silver_table = 'latest_lease_database'

    send_lease_expiry_notification(
        schema=schema,
        silver_table=silver_table
    )