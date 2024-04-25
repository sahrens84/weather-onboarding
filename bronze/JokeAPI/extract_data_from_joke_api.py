# Databricks notebook source
import requests 
import os 
import json 
from datetime import datetime

# Define URL of JokesAPI 
api_url = "https://v2.jokeapi.dev/joke/"

# Define request parameters 
joke_category = "Programming"
joke_amount = 10 

# Define and create (if not exists) directory to save jokes data to 
save_dir = "/Volumes/prod/bronze/file_storage/jokesapi"
os.makedirs(save_dir, exist_ok=True)

# Define counter to keep track of amount of succesfully extracted jokes 
joke_counter = 0 

# Define number of jokes to extract 
num_jokes_to_extract = 100 

# While less then num_jokes_to_extract jokes are extracted continue extracting jokes
while joke_counter < num_jokes_to_extract:
    # Make GET request to JokeAPI 
    response = requests.get(f"{api_url}{joke_category}?format=json&amount={joke_amount}")

    # Check request status 
    if response.status_code == 200:
        # Extract JSON response
        api_response = response.json()

        # Loop through each joke in the response
        for joke in api_response['jokes']:
            # Extract joke id and joke 
            joke_id = joke['id']
            
            # Create filename with joke ID and timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"{save_dir}/joke_{joke_id}_{timestamp}.json"
            
            # Save joke data to JSON file
            with open(file_path, 'w') as file:
                json.dump(joke, file)

            # Print success message
            print(f"Successfully extracted joke with id {joke_id} and saved to {file_path}.")

            # Increase joke couunter
            joke_counter += 1 
    else:
        # Print error message
        print("Error:", response.status_code, response.reason)

print(f"Succesfully extracted {joke_counter} jokes and saved to {save_dir}")

