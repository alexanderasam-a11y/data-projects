# ⚡ SQL Query Exporter

A desktop GUI tool to run `.sql` files against various databases and export 
the results as formatted Excel files (`.xlsx`).

Built with Python and CustomTkinter.

## Features

- Supports **MS SQL Server**, **SQLite**, **PostgreSQL** and **MySQL**
- Windows Authentication support for MS SQL Server
- Auto-detects SQL file encoding (UTF-8, UTF-8-BOM, cp1252 ...)
- Exports results as Excel with auto-width columns, frozen header row and autofilter
- Adds an export timestamp column automatically
- Saves last-used settings to `config.json` (password excluded)
- Runs queries in a background thread – GUI stays responsive

## Requirements

- Python 3.10+
- MS SQL Server: [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Installation
```bash
git clone https://github.com/alexanderasam-a11y/sql-query-exporter.git
cd sql-query-exporter
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and enter your database password:
