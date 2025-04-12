# ETL Pipeline Project

## Description
A complete ETL pipeline pulling data from 5 sources:

CSV File
JSON File
Google Sheets API
MongoDB Atlas
REST API (OpenWeatherMap)

## Features
Data cleaning, timestamp formatting, unit conversion
Daily automation using schedule
CI/CD via GitHub Actions
Final output stored in MongoDB & CSV

## Setup Instructions
Create virtualenv and install requirements
Add config/db_config.json and credentials.json
Run python etl_pipeline.py
Use scheduler.py for daily automation
