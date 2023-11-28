import json
import pandas as pd
import requests
from threading import Thread
from multiprocessing.pool import ThreadPool as Pool
import time
import os 
from configparser import ConfigParser


configur = ConfigParser()
configur.read('config.ini')


class other_info :
    pool_size = 100
    county_name = ''
    headers = {}
    
    ## Config ##
    output_path = configur.get('development','output_path')

    def property_owned_by(self,property):  
        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])  
            property_owned_by_df = pd.DataFrame()
            fetch_propety_owned_by_url = "https://webapi.narrpr.com/properties/"+property_id+"/owner-facts?orgId="+org_id+"&listingId="+listing_id+"&propertyMode="+property_mode+"&zipPlaceId="+zip_place_id
            propety_owned_by_data = requests.get(fetch_propety_owned_by_url,headers=self.headers).json().get('Owner Occupied')
            if propety_owned_by_data != None:
                property_owned_by_df['occupied_by'] = ['OWNER']
            else :
                propety_owned_by_data = requests.get(fetch_propety_owned_by_url,headers=self.headers).json().get('Vesting')
                if propety_owned_by_data != None:
                    property_owned_by_df['occupied_by'] = ['COMPANY']
            property_owned_by_df['listingid'] = [listing_id]
            property_owned_by_df.to_csv(self.output_path+'/property_owned_by/'+listing_id+'_property_owned_by.csv',index=False)
        except ValueError:
            print('error : property_owned_by :',ValueError)

    def tax_record(self,property):  
        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])  
            try :
                fetch_tax_record_url = "https://webapi.narrpr.com/properties/"+property_id+"/tax-records"
                tax_record_data = requests.get(fetch_tax_record_url,headers=self.headers).json()
                tax_df = pd.DataFrame()
                for data in tax_record_data :
                    if data[0] == 'Assessment Year' :
                        tax_df = pd.DataFrame(columns= [ "Tax Assessment Year - " + s for s in data[1:]])
                    if data[0] == 'Total Assessed Value' :
                        df = tax_df.columns
                        tax_df = pd.DataFrame([data[1:]],columns=df)
                tax_df['listingid'] = [listing_id]
                tax_df.to_csv(self.output_path+'/tax_record/'+listing_id+'_tax_record.csv',index=False)
            except :
                pass
        except ValueError:
            print('error : tax_record :',ValueError)
        
        

    def deed_record(self,property):   
        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5]) 
            fetch_deed_record_url = "https://webapi.narrpr.com/properties/"+property_id+"/deed-records"
            deed_record_data = requests.get(fetch_deed_record_url,headers=self.headers).json()
            deed_df = pd.DataFrame()
            for data in deed_record_data :
                if data[0] == "Recorder's Book #" :
                    deed_df["Recorder's Book #"] = [data[1]]
                if data[0] == "Recorder's Page #" :
                    deed_df["Recorder's Page #"] = [data[1]]
            deed_df['listingid'] = [listing_id]
            deed_df.to_csv(self.output_path+'/deed_record/'+listing_id+'_deed_record.csv',index=False)
        except ValueError:
            # print('error : deed_record :',ValueError)
            pass
            

    def legal_description(self,property):

        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])
            fetch_legal_description_url = "https://webapi.narrpr.com/properties/"+property_id+"/legal-description?orgId="+org_id+"&listingId="+listing_id+"&propertyMode="+property_mode+"&zipPlaceId="+zip_place_id
            legal_description_data = requests.get(fetch_legal_description_url,headers=self.headers).json()
            legal_df = pd.json_normalize(legal_description_data)
            legal_df['listingid'] = [listing_id]
            legal_df.to_csv(self.output_path+'/legal_description/'+listing_id+'_legal_description.csv',index=False)
            
        except ValueError:
            print('error : legal_description :',ValueError)

    def listing_detial(self,property):
            
        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])
            fetch_listing_details_url = "https://webapi.narrpr.com/properties/"+property_id+"/listing-details?orgId="+org_id+"&listingId="+listing_id+"&propertyMode="+property_mode+"&zipPlaceId="+zip_place_id
            listing_details_data = requests.get(fetch_listing_details_url,headers=self.headers).json()
            listing_details_df = pd.json_normalize(listing_details_data)
            listing_details_df['listingid'] = [listing_id]
            listing_details_df.to_csv(self.output_path+'/listing_detial/'+listing_id+'_listing_detial.csv',index=False)
        except ValueError:
            print('error : listing_detial :',ValueError)

    def agent(self,property):
        try:    
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])
            fetch_listing_agent_url = "https://webapi.narrpr.com/properties/"+property_id+"/listing-agent?orgId="+org_id+"&listingId="+listing_id
            listing_agent_data = requests.get(fetch_listing_agent_url,headers=self.headers).json()
            listing_agent_df = pd.json_normalize(listing_agent_data)
            listing_agent_df['listingid'] = [listing_id]
            listing_agent_df.to_csv(self.output_path+'/listing_agent/'+listing_id+'_listing_agent.csv',index=False)
        except ValueError:
            print('error : agent :',ValueError)

    def history(self,property):
        try:
            property_id = str(property[1])
            listing_id = str(property[2])
            org_id = str(property[3])
            zip_place_id = str(property[4])
            property_mode = str(property[5])
            fetch_listing_history_url = "https://webapi.narrpr.com/properties/"+property_id+"/listing-history?orgId="+org_id+"&listingId="+listing_id
            # print('headers----------->>>>>>',self.headers)
            listing_history_data = requests.get(fetch_listing_history_url,headers=self.headers).json()
            listing_history_df = pd.DataFrame()
            i = 1
            for date in listing_history_data[0:2] :
                listing_history_dict = {}
                active_date = date.get('changeDate')
                listing_history_df['active_date - '+str(i)] = [active_date]
                i += 1
            listing_history_df['listingid'] = [listing_id]
            listing_history_df.to_csv(self.output_path+'/listing_history/'+listing_id+'_listing_history.csv',index=False)
        except ValueError:
            print('error : history :',ValueError)

    def processStart(self):
        print(self.headers)
        if not os.path.exists(self.output_path) :
            os.mkdir(self.output_path)
        
        if not os.path.exists(self.output_path+'/ALL') :
            os.mkdir(self.output_path+'/ALL')
        
        if not os.path.exists(self.output_path+'/PROPERTIES') :
            os.mkdir(self.output_path+'/PROPERTIES')
  
        if not os.path.exists(self.output_path) :
            os.mkdir(self.output_path)

        # if not os.path.exists(self.output_path+self.county_name) :
        #     os.mkdir(self.output_path+self.county_name)

        if not os.path.exists(self.output_path+'/listing_history') :
            os.mkdir(self.output_path+'/listing_history')

        if not os.path.exists(self.output_path+'/listing_agent') :
            os.mkdir(self.output_path+'/listing_agent')

        if not os.path.exists(self.output_path+'/listing_detial') :
            os.mkdir(self.output_path+'/listing_detial')

        if not os.path.exists(self.output_path+'/legal_description') :
            os.mkdir(self.output_path+'/legal_description')

        if not os.path.exists(self.output_path+'/deed_record') :
            os.mkdir(self.output_path+'/deed_record')

        if not os.path.exists(self.output_path+'/tax_record') :
            os.mkdir(self.output_path+'/tax_record')

        if not os.path.exists(self.output_path+'/property_owned_by') :
            os.mkdir(self.output_path+'/property_owned_by')

        pool = Pool(self.pool_size)
        properties = pd.read_csv(self.output_path+'PROPERTIES/'+self.county_name+'.csv').filter(['propertyId','listingid','orgId','zipPlaceId','propertyMode'])
        for item in properties.itertuples():
            pool.apply_async(self.history,(item,))
            pool.apply_async(self.agent,(item,))
            pool.apply_async(self.property_owned_by,(item,))
            pool.apply_async(self.tax_record,(item,))
            pool.apply_async(self.deed_record,(item,))
            pool.apply_async(self.legal_description,(item,))
            pool.apply_async(self.listing_detial,(item,))
            
        pool.close()
        pool.join()
