import os
import pandas as pd
from configparser import ConfigParser
from threading import *


class merge_all():

    def __init__(self) :

        configur = ConfigParser()
        configur.read('config.ini')
        
        ## Config ##
        self.output_path = configur.get('development','output_path')

        self.ALL = 'ALL/'

        self.deed_record = 'deed_record/'
        self.legal_description = 'legal_description/'
        self.listing_agent = 'listing_agent/'
        self.listing_detial = 'listing_detial/'
        self.listing_history = 'listing_history/'
        self.property_owned_by = 'property_owned_by/'
        self.tax_record = 'tax_record/'
        self.PROPERTIES = 'PROPERTIES/'

        self.df_deed_record = pd.DataFrame()
        self.df_legal_description = pd.DataFrame()
        self.df_listing_agent = pd.DataFrame()
        self.df_listing_detial = pd.DataFrame()
        self.df_listing_history = pd.DataFrame()
        self.df_property_owned_by = pd.DataFrame()
        self.df_tax_record = pd.DataFrame()
        self.df_PROPERTIES = pd.DataFrame()

    def properties(self):
        for file_PROPERTIES in os.listdir(self.output_path+self.PROPERTIES):
            try :
                temp_prop_df = pd.read_csv(self.output_path+self.PROPERTIES+file_PROPERTIES)
                self.df_PROPERTIES = pd.concat([self.df_PROPERTIES,temp_prop_df],axis=0)
            except :
                continue
        print('----df_PROPERTIES----')
        print(self.df_PROPERTIES)

    def deeds_record(self) :
        for file_deed_record in os.listdir(self.output_path+self.deed_record):
            try :
                temp_deed_df = pd.read_csv(self.output_path+self.deed_record+file_deed_record)
                self.df_deed_record = pd.concat([self.df_deed_record,temp_deed_df],axis=0)
            except :
                continue
        print('----df_deed_record----')
        print(self.df_deed_record)
    
    def legal_descriptions(self) :
        for file_legal_description in os.listdir(self.output_path+self.legal_description) :
            try :
                temp_legal_df = pd.read_csv(self.output_path+self.legal_description+file_legal_description)
                self.df_legal_description = pd.concat([self.df_legal_description,temp_legal_df],axis=0)
            except : 
                continue
        print('----df_legal_description----')
        print(self.df_legal_description)

    def lisitng_agents(self):    
        for file_listing_agent in os.listdir(self.output_path+self.listing_agent):
            try :
                temp_agent_df = pd.read_csv(self.output_path+self.listing_agent+file_listing_agent)
                self.df_listing_agent = pd.concat([self.df_listing_agent,temp_agent_df],axis=0)
            except :
                 continue
        print('----df_listing_agent----')
        print(self.df_listing_agent)

    def listing_details(self) :    
        for file_listing_detail in os.listdir(self.output_path+self.listing_detial):
            try : 
                temp_listing_detail_df = pd.read_csv(self.output_path+self.listing_detial+file_listing_detail)
                self.df_listing_detial = pd.concat([self.df_listing_detial,temp_listing_detail_df],axis=0)
            except : 
                continue
        print('----df_listing_detial----')
        print(self.df_listing_detial)
    
    def lisiting_historys(self):
        for file_listing_history in os.listdir(self.output_path+self.listing_history):
            try : 
                temp_listing_history_df = pd.read_csv(self.output_path+self.listing_history+file_listing_history)
                self.df_listing_history = pd.concat([self.df_listing_history,temp_listing_history_df],axis=0)
            except : 
                continue
        print('----df_listing_history----')
        print(self.df_listing_history)

    def properties_owned_by(self):    
        for file_property_owned_by in os.listdir(self.output_path+self.property_owned_by):
            try :
                temp_property_owned_df = pd.read_csv(self.output_path+self.property_owned_by+file_property_owned_by)
                self.df_property_owned_by = pd.concat([self.df_property_owned_by,temp_property_owned_df],axis=0)
            except : 
                continue
        print('----df_property_owned_by----')
        print(self.df_property_owned_by)

    def tax_records(self):
        for file_tax_record in os.listdir(self.output_path+self.tax_record):
            try :
                temp_tax_df = pd.read_csv(self.output_path+self.tax_record+file_tax_record)
                self.df_tax_record = pd.concat([self.df_tax_record,temp_tax_df],axis=0)
            except : 
                continue
        print('----df_tax_record----')
        print(self.df_tax_record)

    def merge(self):
        print('merge file started')
        main_df = pd.merge(self.df_PROPERTIES,self.df_deed_record,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_legal_description,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_listing_agent,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_listing_detial,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_listing_history,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_property_owned_by,how='left',on='listingid')
        main_df = pd.merge(main_df,self.df_tax_record,how='left',on='listingid')
        
        main_df.fillna(' ',inplace=True)
        main_df.replace('nan','',inplace=True)
        main_df.replace('&ndash;','',inplace=True)

        main_df.to_csv(self.output_path+self.ALL+'FINAL_OUTPUT.csv',index=False)
        print('merge file end')

merge = merge_all()

property_thread = Thread(target=merge.properties())
deeds_thread = Thread(target=merge.deeds_record())
legal_thread = Thread(target=merge.legal_descriptions())
agent_thread = Thread(target=merge.lisitng_agents())
lisitng_details_thread = Thread(target=merge.listing_details())
listing_history_thread = Thread(target=merge.lisiting_historys())
property_owned_by_thread = Thread(target=merge.properties_owned_by())
tax_record_thread = Thread(target=merge.tax_records())

property_thread.start()
deeds_thread.start()
legal_thread.start()
agent_thread.start()
lisitng_details_thread.start()
listing_history_thread.start()
property_owned_by_thread.start()
tax_record_thread.start()

property_thread.join()
deeds_thread.join()
legal_thread.join()
agent_thread.join()
lisitng_details_thread.join()
listing_history_thread.join()
property_owned_by_thread.join()
tax_record_thread.join()

merge.merge()