# RPR
Realtors Property Resource® (RPR) is free for members. Scrape All Realtors property that are in Florida. 

# How to setup on local
**Step 1 :** First of all download the zip or clone the repo.

**Step 2 :** Unzip the file that you have downloaded from above step

**Step 3 :** Add the local path under the ‘ **config.ini** ’ file like this if file not present make ‘ **config.ini** ’ file:

        [give name as you like]
                output_path = <local path>        
                rpr_email = <email for login to rpr>
                rpr_password = <password>

**Step 4 :** Then add this line to python file (Replace output_path with below): 

        **1_properties.py :** 
                output_path = configur.get('name given in above step','output_path')
                rpr_email = configur.get('name given in above step','rpr_email')
                rpr_password = configur.get('name given in above step','rpr_password')

        **get_other_info.py :**
                output_path = configur.get('name given in above step','output_path')
    
       **2_merge_all_thread.py :**
                output_path = configur.get('name given in above step','output_path')

        **3_find_new_property.py :**
                output_path = configur.get('name given in above step','output_path')

**Step 5 :** Download chromedriver according to your chromeverison from ‘ https://chromedriver.chromium.org/downloads ’

# How to run

**Step 1 :** Run 1_properties.py it will scrape all the property and their details according to the county given in county.csv

**Step 2 :** Run 2_merge_all_thread.py it will merge all the properties and their details in one final file with name FINAL_OUTPUT.csv (It will be under the <local path provided in config file/ALL/>)

**Step 3 :** Run 3_find_new_property.py it will find new properties of the current week. (Make sure to put last run file in <local path provided in config file/ALL/> with name FINAL_OUTPUT_OLD.csv)
