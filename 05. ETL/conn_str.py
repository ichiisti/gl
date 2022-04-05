import os

server = input("Enter server name: ")
database = input("Eneter db name: ")
username = input("Enter user name: ")
password = input("Eneter password: ")
driver = "{ODBC Driver 17 for SQL Server}"
tbl_name = input("Enter table name: ")

conn_str = (
    "Driver="
    + driver
    + ";Server="
    + server
    + ";Port=1433;Database="
    + database
    + ";UID="
    + username
    + ";PWD="
    + password
)
