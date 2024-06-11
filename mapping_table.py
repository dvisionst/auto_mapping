from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import datetime
import os
import shutil
import hashlib
from datetime import datetime
import http.client
import json
import csv


# variables used for the mapping table function raw data path is where all of the output files will be stored 
# raw_data_path = "C:\\Users\\JoseFlores\\OneDrive - Wireless Guardian\\Documents - Wireless Guardian\\Data Department\\Monthly Executive Reports\\Raw Data\\December 2023"
raw_data_path = "C:\\Users\\JoseFlores\\Desktop\\Sand_box"
chrome_d_path = "C:\\Users\\JoseFlores\\OneDrive - Wireless Guardian\\Documents - Wireless Guardian\\Data Department\\Python Scripts\\Final Versions\\SentinelScraper\\chromedriver.exe"
deact_before_date = '2023-12-01'
act_before_date = '2024-01-01'


# variables used in the aggregator functions
imsi_folder_path = "C:\\Users\\JoseFlores\\OneDrive - Wireless Guardian\\Documents - Wireless Guardian\\Data Department\\November_Counts_Project\\IMSIs"
macs_folder_path = "C:\\Users\\JoseFlores\\OneDrive - Wireless Guardian\\Documents - Wireless Guardian\\Data Department\\November_Counts_Project\\MACs"



# Convert the time to a Unix timestamp in milliseconds
current_time = time.localtime()
now = int(time.mktime(current_time) * 1000)
now_time = str(now)
#print(now_time)

token_secret = 'b0um83r0g4ziinzigxdfymtuenn42hms'
x_sentinel_time = now_time

# Concatenate the strings
input_string = token_secret + ':' + x_sentinel_time

# Calculate the SHA256 hash
sha256_hash = hashlib.sha256(input_string.encode()).hexdigest()

conn = http.client.HTTPSConnection("data.wgsentinel.com", timeout=None)
payload = ''
headers = {
    'x-sentinel-data': 'XDCh8bb2Y3KSblVXsDY6Q4mnfmXwXqb_RJQWwlhBnry9_uUoBCeZvJFV8KlQhJRaKwlwaVyB1lCkBcAa1b1mSHLeI8PcY8uIeZrdJ2e8RF1YYOBeqEziA8ORneYXSeC5',
    'x-sentinel-time': now_time,
    'x-sentinel-secret': sha256_hash
}

# allSigints function from Lab API
def allSigints(all_sigints_path):
    conn.request("GET", f"/api/v1/sigint", payload, headers)
    res = conn.getresponse()
    confirm = json.loads(res.read().decode('utf-8'))
    j_name = 'sigints.json'
    csv_file = "all_sigints.csv"
    with open(f"{all_sigints_path}\\{j_name}", 'w') as json_file:
        json.dump(confirm, json_file, indent=4)

    # Specify the CSV file name
 
    with open(f"{all_sigints_path}\\{csv_file}", mode="w", newline="", encoding="utf-8") as csv_file:
        #Create a CSV writer
        csv_writer = csv.writer(csv_file)

        # Write the header row
        header = [key for key in confirm[0].keys()]
        csv_writer.writerow(header)

        # Write data rows
        for row in confirm:
            row_data = [value for key, value in row.items()]
            csv_writer.writerow(row_data)
    
    print(f"CSV file '{csv_file}' has been created.")
    return "all_sigints.csv"



# WGDataLab pulls inside of a function
def database_sites(csv_file_name):
    conn = http.client.HTTPSConnection("api.wgdatalab.com", timeout=None)
    payload = ''
    headers = {
        'X-API-KEY': 'YRn6!sEt9nVDuqb5rFR8QM9TMD4#qVDxY442Rs8rQhKa%tgVhXtrWVe2KZjx7z6YKy*@BceaQ8mdhvNP2x7s%sB#fDN%zgd5F5D'
    }

    conn.request("GET", f"/api/v1/sites/search", payload, headers)
    res = conn.getresponse()
    confirm = json.loads(res.read().decode('utf-8'))

    # Specify the CSV file name
    sites_file_name = "all_sites_wg.csv"
    # Open the CSV file for writing
    with open(f"{csv_file_name}\\{sites_file_name}", mode="w", newline="", encoding="utf-8") as csv_file:
        # Create a CSV writer
        csv_writer = csv.writer(csv_file)

        # Write the header row
        header = ['WGSiteID', 'GoLive', 'AADT', 'Site Name', 'Reseller Name', 'site']
        csv_writer.writerow(header)

        # Write data rows
        for row in confirm:
            row_data = [row['wgSiteId'], str(row['goLiveDate']), row['aadt'], row['siteName'], row['resellerName'], row['sentinelAliasIds']]
            csv_writer.writerow(row_data)

    print(f"CSV file '{csv_file_name}' has been created.")
    print(confirm)
    print("HOOORRAYYYY!!!")
    return sites_file_name




def map_table(activated_before, deactivated_before, raw_data_folder, chrome_path):
    
    # Path to your ChromeDriver executable
    chrome_driver_path = chrome_path

    # Create a new instance of the Chrome driver with the Service class
    driver = webdriver.Chrome(service=Service(chrome_driver_path))

    # Navigate to a website
    driver.get("https://wgsentinel.com/login")
    driver.maximize_window()
    username = 'sbevilacqua'
    password = 'LetsGetScrapy1!'

    login = driver.find_element(By.CSS_SELECTOR, "input[type='text']").send_keys(username)
    password = driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(password)
    password = driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(Keys.RETURN)
    
    time.sleep(7)
    # Click the site groups button
    site_groups_button = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.CLASS_NAME, 'btn-orange')))
    site_groups_button.click()

    # Give some extra time for the next page to load (adjust this timing as needed)
    time.sleep(7)

    # Click the export button
    export_button = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.CLASS_NAME, 'btn-outline-purple')))
    export_button.click()
    time.sleep(7)
    # Close the browser at the end, ensuring it closes regardless of the outcome
    driver.quit() 
    
    # Path to  Downloads folder and the destination folder
    downloads_folder = "C:\\Users\\JoseFlores\\Downloads"  
    destination_folder = raw_data_folder
    
    # Get a list of files in the Downloads folder along with their modification times
    files = []
    for filename in os.listdir(downloads_folder):
        file_path = os.path.join(downloads_folder, filename)
        if os.path.isfile(file_path):
            modification_time = os.path.getmtime(file_path)
            files.append((file_path, modification_time))

    # Sort the files by modification time (newest first)
    files.sort(key=lambda x: x[1], reverse=True)

    # Check if there are files in the Downloads folder
    if files:
        # Get the path of the most recent file
        most_recent_file_path = files[0][0]
        
        new_file_name = "sentinel_sites.csv"

        new_file_path = os.path.join(destination_folder, new_file_name)
        # Move the most recent file to the destination folder
        shutil.move(most_recent_file_path, new_file_path)
        print(f"Moved {most_recent_file_path} to {destination_folder}")
    else:
        print("No files found in the Downloads folder")
    


    
    # The pathways for the 3 files used to create the mapping table this should be in the 
    # Shared One Drive
    all_sigs_file = allSigints(all_sigints_path=raw_data_folder)
    sites_file = database_sites(csv_file_name=raw_data_folder)

    # Creating the different dataframes for the three files in the aforementioned pathways


    df_wg_alvan = pd.read_csv(f"{raw_data_folder}\\{sites_file}")
    df_sentinel = pd.read_csv(f"{raw_data_folder}\\{new_file_name}")
    df = pd.read_csv(f"{raw_data_folder}\\{all_sigs_file}")


    # function that converts unixcode time into actual time
    def convert(timestamp_ms):

        # Unix timestamp in milliseconds
        # Convert milliseconds to seconds
        timestamp_sec = timestamp_ms // 1000

        # Create a datetime object from the Unix timestamp in seconds
        dt = datetime.fromtimestamp(timestamp_sec)

        # Convert datetime object to a formatted string
        formatted_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
        return formatted_datetime


    # while loop to convert unixtimecode in the activated and deactivated columns of the all sigints file
    i = 0 
    while i < len(df):
        df.at[i, 'activated'] = convert(df.iloc[i]['activated'])
        if df.iloc[i]['deactivated'] != 0:
            df.at[i, 'deactivated'] = convert(df.iloc[i]['deactivated'])
        i += 1

    # While loop that collects all of the indeces of the sigints to be removed for the month
    index_remove_list = []
    i = 0 
    while i < len(df):
        if df.iloc[i]['activated'] > activated_before:
            index_remove_list.append(i)
        if df.iloc[i]['deactivated'] != 0:
            if df.iloc[i]['deactivated'] < deactivated_before:
                index_remove_list.append(i)
        i += 1
    


    # creating a copy then deleting all of the sigints that were activated after the month or deactivated before the month
    df_info =df.copy()
    df_info.drop(df_info.index[index_remove_list], inplace=True)
    not_needed = 'mtyt-wipz-gbcb'
    df_info = df_info.drop(df_info[df_info.apply(lambda row: row.eq(not_needed)).any(axis=1)].index)
    filtered_name = "all_sigints_filtered.csv"
    df_info.to_csv(f"{raw_data_folder}\\{filtered_name}", index=False)
    # while loop that maps the sigints with the proper WG Site ID from the sites file. In the loop the Site Name gets parsed to extract AA###
    df_sentinel['WGSite'] = 'missing'
    i = 0 
    while i < len(df_sentinel):
        if df_sentinel.iloc[i]['Site Name'][1] == 'A':
            df_sentinel.at[i, 'WGSite'] = df_sentinel.iloc[i]['Site Name'][:5]
        else:
            df_sentinel.at[i, 'WGSite'] = df_sentinel.iloc[i]['Site Name'][:6]
        i += 1

    # Empty arrays that store all of the needed values for the corresponding columns
    aa_num = []
    sitegroup_list = []
    site_name_list = []

    # While loop that matches the site from the sigints file to find and store the values corresponding WGSiteID, Sitegroup Name, and Site Name
 
    i = 0
    while i< len(df_info):
        j = 0
        while j < len(df_sentinel):
               
            if df_info.iloc[i]['site'] == df_sentinel.iloc[j]['Site']:
                aa_num.append(df_sentinel.iloc[j]['WGSite'])
                sitegroup_list.append(df_sentinel.iloc[j]['Sitegroup Name'])
                site_name_list.append(df_sentinel.iloc[j]['Site Name'])
                break
                
            elif j == len(df_sentinel) -1:
                aa_num.append('')
                sitegroup_list.append('')
                site_name_list.append('')

                break
            else:
                j += 1            

        i += 1

    # adding the lists with the values to the dataframe which will be the final df used for the mapping table
    df_info['WGSite_ID'] = aa_num
    df_info['Sitegroup Name'] = sitegroup_list
    df_info['Site Name'] = site_name_list
        
    # empty arrays used to store the GoLive and AADT values 
    gld_list = []
    aadt_list = []

    # While loop that matches the WGSite ID to find and store the values corresponding to the GoLive date and the AADT number
 
    i = 0
    while i< len(df_info):
        j = 0
        while j < len(df_wg_alvan):   
            if df_info.iloc[i]['WGSite_ID'] == df_wg_alvan.iloc[j]['WGSiteID']:
                gld_list.append(df_wg_alvan.iloc[j]['GoLive'])
                aadt_list.append(df_wg_alvan.iloc[j]['AADT'])
                break
            elif df_info.iloc[i]['WGSite_ID'] == '' or j == len(df_wg_alvan) -1:
                gld_list.append('')
                aadt_list.append('')
                break
            else:
                j += 1
            
        i += 1
    # adding the lists with values to the dataframe
    df_info['GoLiveDate'] = gld_list
    df_info['AADT'] = aadt_list

    output_file = f"{raw_data_folder}\\mapping.csv"
    df_info.to_csv(output_file, index=False)

    df_info = df_info.drop_duplicates(subset='WGSite_ID', keep='first')

    # converting the dataframe to the Info Pb csv Mapping Table

    output_file = f"{raw_data_folder}\\Info Pb.csv"
    df_info.to_csv(output_file, index=False)


    return print(f"\n\nInfo Pb table has been created at: {output_file}\n\n")


def counts_aggregator(folder, info_path):
    # empty DataFrame objects to aggregate all of files
    master_df = pd. DataFrame()

    # for loop that goes through all of the files in a folder to combine them
    files_in_folder = 0
    for filename in os.listdir(folder):
            files_in_folder += 1
    file_list = []
    for filename in os.listdir(folder):
                
        if filename.endswith("_counts.csv"):
            file_path = os.path.join(folder, filename)
            name = filename[-15:]
            df = pd.read_csv(file_path)
            df_mapping = pd.read_csv(f"{info_path}\\mapping.csv")

            site_list = []
            i = 0
            while i < len(df):
                j = 1
                if df.iloc[i]['Sigint'] == df_mapping.iloc[i]['sigint']:
                    site_list.append(df_mapping.iloc[i]['site'])
                else:
                    while j < len(df_mapping):
                        if df.iloc[i]['Sigint'] == df_mapping.iloc[j]['sigint']:
                            site_list.append(df_mapping.iloc[j]['site'])
                            break
                        elif j == len(df_mapping) - 1:
                            site_list.append('Not Found')
                            break
                        else:
                            j += 1
                i += 1

            df['site'] = site_list
            master_df = pd.concat([master_df, df], ignore_index=True) 
            file_list.append(filename[:13])   
            print(f"\nThis {filename[:13]} just processed.\n\n{len(file_list)} files out {files_in_folder} have been processed.") 
                    
    master_df.to_csv(f"{folder}\\{name}", index=False)


# Calling the function only need to modify the date values for whichever Monthly report needs to be generated

map_table(activated_before=act_before_date, deactivated_before=deact_before_date, raw_data_folder=raw_data_path, chrome_path=chrome_d_path)

# counts_aggregator(folder=imsi_folder_path, info_path=raw_data_path)
# counts_aggregator(folder=macs_folder_path, info_path=raw_data_path)