# packages
import os
import time
import re
import json
import pandas as pd
import mysql.connector
import smtplib
from pathlib import Path
from mysql.connector import Error
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def log_execution(func):
    """
    """
    
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

def server_connection(host, user, root_pass, db_name):
    """
    """
    
    try:
        # Establish the MySQL connection
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database=db_name
        )
        
        if connection.is_connected():
            print(f"Connected to {db_name}")
        
        return connection
    
    except Exception as e:
        print(f"Connection failed -> {e}")
        return None

@log_execution
def send_lease_expiry_notification(host, user, root_pass, base_table_name, **kwargs):
    """
    """
    
    # email setup
    email_config = "/home/asha/airflow/email-config.json"
    with open(email_config, 'r') as fp:
        email_config = json.load(fp)
        
    smtp_server = email_config.get('smtp_server')
    smtp_port = email_config.get('smtp_port')
    email_sender = email_config.get('email_sender')
    email_password = email_config.get('email_password')
    email_recipient = email_config.get('email_recipient')
        
    # establish connection to base
    base_mysqlconnection = server_connection(host=host, user=user, root_pass=root_pass, db_name='base')
    
    # estbalish dates
    current_date = datetime.today().date()
    threshold_date = current_date + timedelta(days=28)
    load_date = datetime.now()
    
    base_cursor = base_mysqlconnection.cursor()
    
    base_query = f"""
      select
        SupportProviders,
        PropertyAddress,
        LeaseEndDate
      from base.{base_table_name}
    """
    
    base_cursor.execute(base_query)
    
    # Fetch all the results from the table
    base_result = base_cursor.fetchall()

    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
    
    # Close the connections
    base_cursor.close()
    base_mysqlconnection.close()
    
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
        
    else:
        print("No email to send")
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
    
    server_config = "/home/asha/airflow/server-config.json"
    
    with open(server_config, 'r') as fp:
        config = json.load(fp)
    
    # this is the ETL task
    host = config.get('host')
    user = config.get('user')
    root_pass = config.get('root_pass')
    base_table_name = 'dbo_lease_database'

    send_lease_expiry_notification(host=host, user=user, root_pass=root_pass, base_table_name=base_table_name)