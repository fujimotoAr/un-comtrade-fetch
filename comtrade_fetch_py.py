import requests
import os
import time as time
import pandas as pd
import numpy as np

def generate_period():
    # Setting the year to between 2015 and 2019
    yyyy=[str(i) for i in range(2015,2020)]
    mm=['0'+str(i) if i<10 else str(i) for i in range(1,13)]

    date=[]
    for year in yyyy:
        for month in mm:
            date.append(year+month)
    return date

def fetch_data():
    url = "https://comtradeapi.un.org/data/v1/get/C/M/HS"
    arr=[]
    errors=[]
    for p in generate_period():

        querystring = {
            # "reporterCode":"360", # Comment out include data from all reporter countries

            "period":p, # Every month from the beginning of 2015 to the end of year 2019

            # "partnerCode":"36", # Comment out include data from all partner countries

            "cmdCode":'2604,260400', # Nickel data only

            "flowCode":"M", # Limiting trade flow to 'import'

            "includeDesc":"true",

            "":""
            }

        payload = ""
        headers = {"Ocp-Apim-Subscription-Key": ""} # Fill with subscription key 
        trial=0

        # Making initial request
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

        # If there is a timeout, try again
        while list(dict(response.json()).keys()).count('statusCode') > 0 and trial < 50:
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
            trial+=1
            time.sleep(10)
        if trial == 50:
            errors.append(p)

        # Not appending data to array if empty
        try:
            if len(response.json()['data'])==0:
                pass
            for r in response.json()['data']:
                arr.append(r)
        except:
            pass
        
    return pd.DataFrame(arr), errors
        
# Fetching data using the API #
df, errors = fetch_data()

# Checking whether there are any errors #
print(errors)

# Saving data to MySQL database #

# username=''
# password=''
# host=''
# database=''

from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(username, password, host, database))
df.to_sql(name='raw_table_all', con=engine, if_exists='append', index=False)