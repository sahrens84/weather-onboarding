# Databricks notebook source
import requests
import json
from pyspark.sql.functions import rand

def fetch_joke():
    # Function to fetch a joke from prod.gold.jokes

    # Extract random row containing joke data
    df = spark.table("prod.gold.jokes")
    random_row = df.orderBy(rand()).limit(1)
    
    # Return joke content based on the joke type 
    joke_type = random_row.select("type").collect()[0][0]
    if joke_type == "single":
        return random_row.select("joke").collect()[0][0]
    elif joke_type == "twopart": 
        return random_row.select("setup").collect()[0][0], random_row.select("delivery").collect()[0][0]
    else: 
        raise ValueError("Unknown joke type.")

def send_joke_to_slack(joke):
    # Function to send a joke to Slack channel
    webhook_url = dbutils.secrets.get("jokes-api", "slack-webhook-url")

    # Define payload based on joke type 
    if len(joke) == 1:
        payload = {
            "text": f"*{joke[0]}*"
        }
    elif len(joke) == 2:
        payload = {
            "text": f"*{joke[0]}*\n{joke[1]}"
        }
    else:
        raise ValueError("Unknown joke type. Unable to send to Slack.")

    headers = {'Content-type': 'application/json'}
    requests.post(webhook_url, data=json.dumps(payload), headers=headers)


# Fetch a joke
joke = fetch_joke()

# Send the joke to Slack
send_joke_to_slack(joke)
