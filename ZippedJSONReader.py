import io
import base64
import os
import zipfile
import pathlib
import json
import pandas as pd

# set table names (enter the names of your tables in the list) 
EXPORT_TABLES =[]
# set file path 
zip_file_path = 

# endcode zip_file_path to base64
with open(zip_file_path, 'rb') as file:
    binary_contents = file.read()
encoded_contents = base64.b64encode(binary_contents).decode('ascii')


# determines if the zip file is valid, 
def parse_contents(contents):
    
    parsed_files = {}
    is_export = True
    tables_missing = {table_name: True for table_name in EXPORT_TABLES}
    
    try:
        decoded = base64.b64decode(contents)
        zip_string = io.BytesIO(decoded)
    
        with zipfile.ZipFile(zip_string) as z:
            for fileName in z.namelist():
                baseName = pathlib.Path(fileName).stem
                if pathlib.Path(fileName).suffix == ".json":
                    tables_missing.update({baseName: False})
                    with z.open(fileName) as f:
                        data = f.read()
                        data_list = json.loads(data.decode("utf-8-sig"))
                        if type(data_list) is list:
                            parsed_df_file = pd.json_normalize(data_list)
                            parsed_files[baseName] = parsed_df_file.to_dict("records")
                        elif type(data_list) is dict:
                            parsed_df_file = pd.DataFrame([data_list])
                            parsed_files[baseName] = parsed_df_file.to_dict("records")
                else:
                    is_sar_export = False
    except Exception as e:
        print(f"Unable to decode file. \nException: {e}")
        is_sar_export = False
    
    if any(tables_missing.values()):
        is_sar_export = False
    
    return parsed_files, is_sar_export

# additional refining and cleaning
def convert_to_df(raw_file):
    file_df = {}
    for file in raw_file:
        if type(raw_file[file]) is list:
            file_df[file] = pd.json_normalize(raw_file[file])
        elif type(raw_file[file]) is dict:
            file_df[file] = pd.DataFrame([raw_file[file]])
    return file_df

''' 
To create the dataframes:

parsed_files, is_export = parse_contents(encoded_contents)

dfs = convert_to_df(parsed_files)
'''