import requests
from selenium import webdriver  
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.options import Options
import time
import random
import json
import pandas as pd
from get_other_info import other_info
import os
from selenium.webdriver.support.wait import WebDriverWait
from telnetlib import EC
from configparser import ConfigParser


configur = ConfigParser()
configur.read('config.ini')

## Config ##
output_path = configur.get('development','output_path')
rpr_email = configur.get('development','rpr_email')
rpr_password = configur.get('development','rpr_password')

if not os.path.exists(output_path) :
    os.mkdir(output_path)

if not os.path.exists(output_path+'/ALL') :
    os.mkdir(output_path+'/ALL')

if not os.path.exists(output_path+'/PROPERTIES') :
    os.mkdir(output_path+'/PROPERTIES')

county_df = pd.read_csv('County.csv')

for county in county_df.itertuples():
    total_property_count = 0
    county = county[1]
    print(county)
    if os.path.exists(output_path+'/PROPERTIES/'+county+'.csv') :
        continue

    URL = "https://www.narrpr.com/home"
    options = Options()
    options.add_experimental_option("detach", True)
    options.add_argument("headless")
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.get(URL)
    time.sleep(random.randint(8, 10))
    driver.find_element(By.ID,"SignInEmail").send_keys(rpr_email)
    time.sleep(random.randint(0, 10))
    driver.find_element(By.ID,"SignInPassword").send_keys(rpr_password)
    time.sleep(random.randint(0, 10))
    driver.find_element(By.ID,"SignInBtn").click()
    time.sleep(random.randint(20, 25))
    
    
    my_input_text = driver.find_element(By.XPATH,'//*[@name="searchInputBox"]')     
    my_input_text.clear()
    my_input_text.send_keys(county+", Florida")
    my_input_text.send_keys(u'\ue007')
    time.sleep(random.randint(5, 15))
    search_id = driver.current_url.split('scid=')[1]
    token = driver.get_cookie('oidc.at').get('value')


    headers = {
        'Content-Type': 'application/json',
        'Authorization' : 'Bearer '+ str(token)
    }
    get_payload_data_url = "https://webapi.narrpr.com/propertysearches/criteria/"+search_id
    payload_data = requests.get(get_payload_data_url,headers=headers).json()
    payload_data.update({'searchCriteriaId':search_id,'propertyTypeIds':[1],'includeTransactionTypeForLease':'false','includeTransactionTypeForSale':'true','includeTransactionTypePublicRecord':'false','listingStatusDateRanges':[{'listingStatus': 2}, {'listingStatus': 3}, {'listingStatus': 4}]})
    price_range = [[0,0],[0,25000],[25000,50000],[50000, 100000], [100000, 150000], [150000, 200000], [200000, 250000], [250000, 300000], [300000, 330000],[330000, 350000], [350000, 380000],[380000, 400000],[400000, 430000], [450000, 480000], [480000, 500000], [500000, 550000], [550000, 600000], [600000, 650000], [650000, 700000], [700000, 750000], [750000, 800000], [800000, 850000], [850000, 900000], [900000, 950000], [950000, 1000000], [1000000, 1050000], [1050000, 1100000], [1100000, 1150000], [1150000, 1200000], [1200000, 1250000], [1250000, 1300000], [1300000, 1350000], [1350000, 1400000], [1400000, 1450000], [1450000, 1500000], [1500000, 1550000], [1550000, 1600000], [1600000, 1650000], [1650000, 1700000], [1700000, 1750000], [1750000, 1800000], [1800000, 1850000], [1850000, 1900000], [1900000, 1950000], [1950000, 2000000], [2000000, 2050000], [2050000, 2100000], [2100000, 2150000], [2150000, 2200000], [2200000, 2250000], [2250000, 2300000], [2300000, 2350000], [2350000, 2400000], [2400000, 2450000], [2450000, 2500000], [2500000, 2550000], [2550000, 2600000], [2600000, 2650000], [2650000, 2700000], [2700000, 2750000], [2750000, 2800000], [2800000, 2850000], [2850000, 2900000], [2900000, 3000000],[3000000, 4000000], [4000000, 5000000],[5000000, 10000000]]
    main_df = pd.DataFrame()

    for price_from,price_to in price_range :
        print(str(price_from)+'------>'+str(price_to))
        
        if price_to == 0 and price_from == 0 : 
            payload_data.pop("priceFrom", None)
            payload_data.pop("priceTo", None)
            
            set_payload_data_url = "https://webapi.narrpr.com/propertysearches/criteria"
            response = requests.request("PUT", set_payload_data_url, headers=headers, data=json.dumps(payload_data))
            set_payload_data = requests.put(set_payload_data_url,headers=headers,data=payload_data)
            
            check_count_url = 'https://webapi.narrpr.com/propertysearches/results/count?searchCriteriaId='+search_id
            response = int(requests.get(check_count_url,headers=headers).text)
            total_property_count = response
            
            print(county+'----->>>'+str(response))
            if response == 0:
                break

            if response <= 500 :
                data_url = "https://webapi.narrpr.com/propertysearches/"+search_id+"/results"

                data = requests.get(data_url,headers=headers)
                df = pd.json_normalize(data.json().get('results'))
                main_df = pd.concat([main_df,df],axis=0,ignore_index=True)
                break
        else :
            payload_data.update({"priceFrom": price_from,"priceTo" : price_to,'includeTransactionTypeForLease':'false','includeTransactionTypeForSale':'true','includeTransactionTypePublicRecord':'false','listingStatusDateRanges':[{'listingStatus': 2}, {'listingStatus': 3}, {'listingStatus': 4}]})
            set_payload_data_url = "https://webapi.narrpr.com/propertysearches/criteria"
            response = requests.request("PUT", set_payload_data_url, headers=headers, data=json.dumps(payload_data))
            set_payload_data = requests.put(set_payload_data_url,headers=headers,data=payload_data)
            data_url = "https://webapi.narrpr.com/propertysearches/"+search_id+"/results"
            data = requests.get(data_url,headers=headers)
            
            try :
                df = pd.json_normalize(data.json().get('results'))
                print(len(df.index))
                main_df = pd.concat([main_df,df],axis=0,ignore_index=True)
            except:
                continue
    
    ### FOR LOGOUT ###
    driver.get('https://www.narrpr.com/auth/sign-out')
    time.sleep(random.randint(5, 10))
    ### FOR LOGOUT ###
    
    driver.close()
    
    if total_property_count != 0:
        main_df.rename(columns={'listingId':'listingid'},inplace=True)
        
    main_df.drop_duplicates(subset=['listingid']).to_csv(output_path+'PROPERTIES/'+county+'.csv',index=False)
    
    if total_property_count != 0:
        otherinfo = other_info()
        otherinfo.county_name = county
        otherinfo.headers = {
            'Authorization' : 'Bearer '+ str(token)
        }
        otherinfo.processStart()