#!/bin/bash

# Check if the Ubuntu version is supported
if ! [[ "18.04 20.04 22.04 23.04 24.04" == *"$(lsb_release -rs)"* ]]; then
    echo "Ubuntu $(lsb_release -rs) is not currently supported."
    exit 1
fi

# Import the Microsoft GPG key
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc

# Register the Microsoft Ubuntu repository
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update package lists
sudo apt-get update

# Install the ODBC Driver 18 for SQL Server
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Optional: Install sqlcmd and bcp
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18

# Add sqlcmd and bcp to PATH
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

# Optional: Install unixODBC development headers
sudo apt-get install -y unixodbc-dev

# Confirmation message
echo "Installation completed successfully."