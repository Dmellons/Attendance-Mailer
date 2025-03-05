import requests
from datetime import datetime
from datetime import date
from dateutil import parser
import re

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import insert

import collections
import numpy as np
#from styleframe import StyleFrame
#from styleframe import Styler

import hashlib
from sqlalchemy import create_engine
import collections
import uuid
import html
import hashlib

import sql_queries

import sys
from decouple import config

def stumiss_width(row):
    if row['num_all_day_abs'] >= row['num_days_avg_miss']:
        val = 100
    else:
        val = round((row['num_all_day_abs']/row['num_days_avg_miss'])*100)
    return val

def avgmiss_width(row):
    if row['num_days_avg_miss'] >= row['num_all_day_abs']:
        val = 100
    else:
        val = round((row['num_days_avg_miss']/row['num_all_day_abs'])*100)
    return val

# func to apply bg color
def abs_graph_bg_ret(s):
    baseline = max(3, s['num_days_avg_miss'])
    
    if(s['num_all_day_abs'] <= baseline):
        return 'greenbar'
    elif(s['num_all_day_abs'] <= (baseline*2)):
        return 'yellowbar'
    else:
        return 'redbar'

def collapse_list(vals):
    out = ', '.join(vals)

    return out


server = config('SERVER')
database = config('DATABASE')
username = config('DB_USERNAME')
password = config('DB_PASSWORD')

engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server")
today = date.today()
run_date = today.strftime('%Y-%m-%d')   
queries = {}

# Get the weekday as an integer (0 = Monday, 6 = Sunday)
weekday_number = today.weekday()

if weekday_number == 1:
    scs = "'16'"
    msg_template = "att_weekly_update_secondary"
elif weekday_number == 2:
    scs = "'2','3','4','5','6','7','8','9'"
    msg_template = "att_weekly_update_elementary"
elif weekday_number == 3:
    scs = "'11','12'"
    msg_template = "att_weekly_update_secondary"
else:
    # print("No send today")
    # sys.exit("No send today")
    scs = "'16'"
    msg_template = "att_weekly_update_secondary"


queries['stu_contacts'] = sql_queries.q_stu_contacts.format(sc_str=scs, date=run_date)
queries['abs_counts'] = sql_queries.q_abs_counts.format(sc_str = scs, date=run_date)
queries['days'] = sql_queries.q_days.format(date=run_date)
queries['abs_codes'] = sql_queries.q_abs_codes.format(sc_str = scs)
queries['att_details'] = sql_queries.q_att_details.format(sc_str = scs, date=run_date)

# Query information from SIS
with engine.connect() as cnxn:
    df_stu_contacts = pd.read_sql(queries['stu_contacts'], cnxn)
    df_abs_counts = pd.read_sql(queries['abs_counts'], cnxn)
    df_cal = pd.read_sql(queries['days'], cnxn)
    df_abs_codes = pd.read_sql(queries['abs_codes'], cnxn)
    df_attd = pd.read_sql(queries['att_details'], cnxn)
df_att_codes = pd.read_excel('attendance_code_map.xlsx')
# Dataframe cleanup
df_stu_contacts.fillna('', inplace=True)

df_cal.sort_values(by=['DT'], inplace=True)
df_cal = df_cal.reset_index()

# Convert the language to int

df_abs_counts['cl'] = df_abs_counts['cl'].astype(np.int64)
df_abs_counts.to_excel('./out_data/df_abs_counts.xlsx', index=False)

med_day_abs = df_abs_counts['num_all_day_abs'].median()
med_day_abs = round(med_day_abs)

df_abs_counts['num_days_avg_miss'] = med_day_abs
df_abs_counts['stumiss_width'] = df_abs_counts.apply(stumiss_width, axis=1)
df_abs_counts['avgmiss_width'] = df_abs_counts.apply(avgmiss_width, axis=1)
df_abs_counts['abs_graph_bg'] = df_abs_counts.apply(abs_graph_bg_ret, axis=1)
df_abs_counts['codes_used'] = np.empty((len(df_abs_counts), 0)).tolist()

df_abs_counts = df_abs_counts[['sc','id','ln','fn','cl','num_all_day_abs','num_days_avg_miss','stumiss_width','avgmiss_width','abs_graph_bg','codes_used']]

for d in range (0,5,1):
    df_abs_counts['d'+str(d+1)+'_date'] = df_cal.iloc[d,1]
    
    df_abs_counts['d'+str(d+1)+'_aL'] = ''
    df_abs_counts['d'+str(d+1)+'_aL'+'_bg'] = ''
    
    for p in range (1,7,1):
        df_abs_counts['d'+str(d+1)+'_p'+str(p)] = ''
        df_abs_counts['d'+str(d+1)+'_p'+str(p)+'_bg'] = ''
    
for index, row in df_abs_counts.iterrows():
    #print(row)
    
    for d in range (0,5,1):
        
        lookup = df_attd[(df_attd['DT'] == df_cal.iloc[d,1]) 
                     & (df_attd['ID'] == row['id'])]
        
        #print(lookup)

        if len(lookup.index) > 0:
            # all day abs
            p = 'L'
            att_code = lookup.iloc[0, lookup.columns.get_loc('A' + str(p))]
            
            
            # all day code 
            if att_code and att_code != ' ':
                #print(p, att_code)
                

                lcd = df_att_codes[(df_att_codes['CD'] == att_code)]
                #print(lcd)

                cd_word = lcd.iloc[0, lcd.columns.get_loc('TI')]

                str_code = att_code + ' - ' + cd_word
                
                
                df_abs_counts.at[index,'d'+str(d+1)+'_a'+str(p)] = str_code

                cur_codes = row['codes_used']
                if str_code not in cur_codes:
                    cur_codes.append(str_code)

                # check AD and TY
                ad = lcd.iloc[0, lcd.columns.get_loc('AD')]
                ty = lcd.iloc[0, lcd.columns.get_loc('TY')]

                if att_code == 'T':
                    df_abs_counts.at[index,'d'+str(d+1)+'_a'+str(p)+'_bg'] = 'yellowbg'
                elif ty in [2]:
                    df_abs_counts.at[index,'d'+str(d+1)+'_a'+str(p)+'_bg'] = 'greenbg'
                elif ad == 0:
                    df_abs_counts.at[index,'d'+str(d+1)+'_a'+str(p)+'_bg'] = 'redbg'
                elif ty in [2,4,5]:
                    df_abs_counts.at[index,'d'+str(d+1)+'_a'+str(p)+'_bg'] = 'greenbg'
            
            # period abs
            for p in range (1,7,1):
                att_code = lookup.iloc[0, lookup.columns.get_loc('A' + str(p))]
                
                if att_code and att_code != ' ':
                    #print(p, att_code)
                    df_abs_counts.at[index,'d'+str(d+1)+'_p'+str(p)] = att_code
                    
                    lcd = df_att_codes[(df_att_codes['CD'] == att_code)]
                    #print(lcd)
                    
                    cd_word = lcd.iloc[0, lcd.columns.get_loc('TI')]
                    
                    str_code = att_code + ' - ' + cd_word
                    
                    cur_codes = row['codes_used']
                    if str_code not in cur_codes:
                        cur_codes.append(str_code)
                        
                    # check AD
                    ad = lcd.iloc[0, lcd.columns.get_loc('AD')]
                    ty = lcd.iloc[0, lcd.columns.get_loc('TY')]
                    
                    if att_code == 'T':
                        df_abs_counts.at[index,'d'+str(d+1)+'_p'+str(p)+'_bg'] = 'yellowbg'
                    elif ty in [2]:
                        df_abs_counts.at[index,'d'+str(d+1)+'_p'+str(p)+'_bg'] = 'greenbg'
                    elif ad == 0:
                        df_abs_counts.at[index,'d'+str(d+1)+'_p'+str(p)+'_bg'] = 'redbg'
                    elif ty in [2,4,5]:
                        df_abs_counts.at[index,'d'+str(d+1)+'_p'+str(p)+'_bg'] = 'greenbg'
                        
    
    
    
    if index > 100000:
        break
    


# convert dates
dtfmt = '%A %#m/%#d'
for d in range (0,5,1):
    #print(d)
    df_abs_counts['d'+str(d+1)+'_date'] = df_abs_counts['d'+str(d+1)+'_date'].dt.strftime(dtfmt)
    



df_abs_counts['codes_used'] = df_abs_counts['codes_used'].apply(collapse_list)



df_abs_counts.to_excel('./out_data/test_df_abs_counts.xlsx', index=False)
df_abs_counts.to_csv('./out_data/att_list_ps.csv')
df_abs_counts.head().to_csv('./out_data/att_list_ps_5rows.csv')
print(df_abs_counts.head())
