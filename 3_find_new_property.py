import pandas as pd
from configparser import ConfigParser


configur = ConfigParser()
configur.read('config.ini')

## Config ##
output_path = configur.get('development','output_path')

df = pd.read_csv(output_path+'ALL/FINAL_OUTPUT.csv')
df_old = pd.read_csv(output_path+'ALL/FINAL_OUTPUT_OLD.csv')

df_old = df_old['propertyId']

print(df_old.head())

df = df.merge(df_old,on=['propertyId'],how='left',indicator=True).query('_merge == "left_only"').drop(columns='_merge')
df.to_csv(output_path+'ALL/unique.csv',index=False)