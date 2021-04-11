%%writefile testutility.py
import logging
import os
import subprocess
import yaml
import datetime
import gc
import re

################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            
def replacer(string, char):
    pattern= char + '{2,}'
    string= re.sub(pattern, char, string)
    return string

def col_header_val(df, table_config):
    
    df.columns= df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]', '_', regex= True)
    df.columns= list(map(lambda x: x.strip('_'), list(df.columns))) 
    df.columns= list(map(lambda x: replacer(x,'_'), list(df.columns))) 
    expected_col= list(map(lambda x: x.lower(), table_config['columns']))
    expected_col.sort()
    df.columns= list(map(lambda x: x.lower(), list(df.columns)))
    df= df.reindex(sorted(df.columns), axis=1)
    if len(df.columns)==len(expected_col) and list(expected_col)== list(df.columns):
        print("column validation is passed")
        return 1
    else:
        print("column validation is failed")
        mismatced_column_file= list(set(df.columns).difference(expected_col))
        print("following file columns are not in the yaml file", mismatced_column_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("following yml columns are not in the file uploaded", missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns : {expected_col}')
        return 0
    

%%writefile file.yaml 
file_type: csv
dataset_name: chess_games
file_name: chess_games
table_name: chess_games
inbound_delimiter: ","
outbound_delimiter: ","
skip_leading_rows: 1
columns:
    - Event
    - White
    - Black
    - Result
    - UTCDate
    - UTCTime
    - WhiteElo
    - BlackElo
    - WhiteRatingDiff
    - BlackRatingDiff
    - Opening
    - TimeControl
    - Termination
    - AN
    
import testutility as util

config_data= util.read_config_file("file.yaml")
config_data['file_type']
config_data

import pandas as pd

df_sample= pd.read_csv("chess_games.csv", delimiter=",")
df_sample.head()


file_type= config_data['file_type']
source_file= "./" + config_data['file_name'] + f'.{file_type}'

df= pd.read_csv(source_file, config_data['inbound_delimiter'])

util.col_header_val(df, config_data)

print("columns of files are:" ,df.columns)
print("columns of YAML are:", config_data['columns'])

if util.col_header_val(df, config_data)==0:
    print("validation is failed")
    
else:
    print("validation is passed")

pd.read_csv("./chess_games.csv")


import csv

with open('/Users/irem/Desktop/week6/chess_games.csv', 'rb') as fin, \
     open('/Users/irem/Desktop/week6/chess_games.txt', 'wb') as fout:
    reader = csv.DictReader(fin)
    writer = csv.DictWriter(fout, reader.fieldnames, delimiter='|')
    writer.writeheader()
    writer.writerows(reader)


import csv

reader = csv.reader(open("chess_games.csv", "rU"), delimiter=',')
writer = csv.writer(open("output.txt", 'w'), delimiter='|')
writer.writerows(reader)

print("Delimiter successfully changed")

# computing number of rows
rows = len(df.axes[0])
  
# computing number of columns
cols = len(df.axes[1])
  
print("Number of Rows: ", rows)
print("Number of Columns: ", cols)


import enum
# Enum for size units
class SIZE_UNIT(enum.Enum):
   BYTES = 1
   KB = 2
   MB = 3
   GB = 4
def convert_unit(size_in_bytes, unit):
   """ Convert the size from bytes to other units like KB, MB or GB"""
   if unit == SIZE_UNIT.KB:
       return size_in_bytes/1024
   elif unit == SIZE_UNIT.MB:
       return size_in_bytes/(1024*1024)
   elif unit == SIZE_UNIT.GB:
       return size_in_bytes/(1024*1024*1024)
   else:
       return size_in_bytes
import os
def get_file_size(file_name, size_type = SIZE_UNIT.BYTES ):
   """ Get file in size in given unit like KB, MB or GB"""
   size = os.path.getsize(file_name)
   return convert_unit(size, size_type)

file_path = 'chess_games.csv'
# get file size in GB
size = get_file_size(file_path, SIZE_UNIT.GB)
print('Size of file is : ', size ,  'GB')















    

