# ora2pg_mssql

The ora2pg tool can currently handle migrations from both Oracle and MySQL to PostgreSQL. This fork is an ongoing work on SQL Server integration. The goal is to have it accepted upstream on [darold/ora2pg](https://github.com/darold/ora2pg) . 

# Disclaimer

Even if it has been used for several databases internally, this project is currently of ALPHA quality. __Please use it for testing purposes only.__

# Current features

Feature | Completion estimation | Comments
--- | --- | ---
connect to remote SQL Server | 100% |
listing and exporting tables | 80% |
external tables | 0% |
global temporary tables | |Currently listing all global temp tables used in stored procedures 
comments on columns |80%|
encrypted columns | 0% |
identity columns | 100% | Converting all identity columns to sequences
views | 70%| Complex one need to check
materialized views | |
indexes | 80%|
constraints | 80%|
FK | 80% |
sequences | |
users | |
grants | |
dblink | |
jobs | | Only name and description are used so far.
triggers | |
functions |0% |
procedures | 0%|
tablespaces | 0% |
partitions | |
subpartitions | 0% |
synonyms | |
custom types | 0%| Only Listing is done so far .
audit queries | 0% |
exporting data | 60% | Common datatypes should not be an issue, but complex ones (binary/spatial/json/...) have not been tested enough yet. 
migration estimation | 60% | An estimation is provided but is rather rough so far.  
translating tsql to plpgsql | 1% |
schema selection | |
override column defaults | |
export_schema.sh | | 
import_all.sh | |



# Prerequisites

This fork has many of the same prerequisites as ora2pg. Please head to http://ora2pg.darold.net/documentation.html#INSTALLATION for installation instructions. 
You then need to install the `DBD::ODBC` Perl module. Once done, you will need to install a SQL Server client and/or server. 

## Client install

For ODBC connectivity, sqlcmd (the command-line client) and bcp (bulk loading utility), please head to https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server .

For a GUI client that can connect to both mssql and PostgreSQL, you can have a look at:
- Azure Data Studio (https://docs.microsoft.com/en-us/sql/azure-data-studio/download). The PostgreSQL extension will also need to be installed: https://docs.microsoft.com/en-us/sql/azure-data-studio/postgres-extension.
- DBeaver (https://dbeaver.io/download/) .

## Server install

If you wish to install a full mssql instance, please follow the documentation at https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup. 

# Usage

The connection string should be changed in ora2pg to the following format (please adapt the ODBC Driver according to the previous installation step) :

```
ORACLE_DSN      dbi:ODBC:Driver={ODBC Driver 17 for SQL Server};Server=myserver.myhost,myport;Database=mydatabase
ORACLE_USER     mssql_user
ORACLE_PWD      mssql_pwd
```

One should check that everything works correctly by testing this connection:

```
$ ora2pg -t SHOW_VERSION -c config/ora2pg.conf
Microsoft SQL Server 2014 (SP1-GDR) (KB4019091) - 12.0.4237.0 (X64)
```
