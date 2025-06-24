import mysql.connector

def generate_latest_yearly_tenant_data(host, user, root_pass):
    """
    _docstring
    """
    
    latest_table = 'yearly_tenant_data'
    
    create_query = """
    create table latest.yearly_tenant_data as 
        select
          *
        from base.yearly_tenant_data ytd
        where 
          LoadDate = (
            select 
                max(LoadDate)
            from base.yearly_tenant_data
    );
    """
    
    drop_query = "drop table latest.yearly_tenant_data;"
    
    # Establish connection to latest
    latest_db = mysql.connector.connect(
        host=host,
        user=user,
        password=root_pass,
        database='latest'
    )
    
    # establish latest cursor
    latest_cursor = latest_db.cursor()
    
    # drop the table
    try:
      latest_cursor.execute(drop_query)
      print(f"Dropped -> {latest_table}")
    except:
      pass
    
    # create the table
    latest_cursor.execute(create_query)
    print(f"Created -> {latest_table}")
    
    # commit and close everything
    latest_db.commit()
    latest_cursor.close()
    latest_db.close()
    
if __name__ == "__main__":
    
     # prepare the details to connect to the databases
    host = "localhost"
    user = "root"
    root_pass = "admin"
    
    generate_latest_yearly_tenant_data(host, user, root_pass)