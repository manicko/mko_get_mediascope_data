# MEDIASCOPE DATA LOADER

## 🛠️ Description
This Python project designed to load data using [Mediascope API](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib)

**Key Features :**
* Load data if network is not stable
* Create settings for multiple reports and load them at once
* Easy and versatile report settings
* Automated slicing long period into parts for faster calculation and loading
* Supports multiple target audience
* Asynchronous data loading increase speed significantly
* Export data in CSV


## ⚙️ Setup
 - #### Download the package
 - #### Configure virtual environment. At the moment the package uses Python v3.12 
 - #### Be sure to install all required packages
   - Run `pip install -r requirements.txt` to install all the requirements.

 - ### Configure Mediascop API connection settings
   - Create `mediascope.json` file and put it in `settings/connections/` folder 
   - Configure your Mediascope API connection settings (for more details please refer to [Mediascope API settings](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib?tab=readme-ov-file#%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B3%D1%83%D1%80%D0%B0%D1%86%D0%B8%D1%8F)):    
     ```yml 
       {
         "username": "you username",
         "passw": "you password",
         "client_id": "client_id",
         "client_secret": "00000000-0000-0000-0000-000000000000",
         "auth_server": "https://auth.mediascope.net/.....",
         "root_url": "https://api.mediascope.net/...."
       }
     ```


    
- ### Configure the report settings
  - The report settings are located in the `settings/reports` folder
  - Copy the existing report settings and modify it according to your needs
  - The key parameters with comments are highlighted below:
     ```  yaml         
    'report_subtype': 'DYNAMICS_BY_SPOTS', # defines the loader type
    'data_lang': 'en', # language of the output data
    'category_name': 'cars', # the report output directory
    'folder':'raw_data', # the report output subdirectory
    'multiple_files': True, # recommended to set True for periods longer that a month
     ............
    'period': {
      'date_filter': ['2020-01-01', '2023-12-31'], # if set explicitly, 'period_num' will be ignored if set to Null, will use 'period_num' to get period from current date 
      'last_time': { # This block is used if need to calculate the date referencing to current e.g. 3 months ago
        'period_type': 'm', 
        'period_num': '3',
        'include_current': True # Try to include this month or week if it is accessible in the base
      }
      ............
      ```
- ### Loader types:
  - Using CrossTab:    
    - `DYNAMICS_BY_SPOTS`:
    - `DYNAMICS_BY_SPOTS_DICT`:
    - `TOP_NAT_TV_ADVERTISERS`: 
    - `TOP_NAT_TV_PROGRAMS`: 
    - `NAT_TV_CHANNELS_BA`: 
  - Using TimeBand:
    - `NAT_TV_CHANNELS_ATV`: 
    - `NAT_TV_CHANNELS_TVR`:
    - `NAT_TV_CHANNELS_SOC_DEM`: 