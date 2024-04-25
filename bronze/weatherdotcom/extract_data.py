# Databricks notebook source
import requests
from common.utils import store_json_file, os
from datetime import datetime

# COMMAND ----------

API_KEY = dbutils.secrets.get("weather-api", "api-key")
BASE_URL = "http://api.weatherapi.com/v1"
STORAGE_PATH = "/Volumes/prod/bronze/file_storage/"

CITIES = ["London", "Paris", "Berlin", "Tokio"]

now = datetime.now() 
date = now.strftime("%d%m%Y")
time = now.strftime("%H%M%S")
source_system = "weatherdotcom"
path = os.path.join(STORAGE_PATH, source_system, date)

# COMMAND ----------

for city in CITIES:
    data = requests.get(f"{BASE_URL}/current.json?key={API_KEY}&q={city}").json()
    store_json_file(data=data, path=path, file_name=f"{city.lower()}")
