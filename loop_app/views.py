from django.http import HttpResponse
from django.shortcuts import render
from datetime import datetime, timedelta
import secrets
import string
import gdown
import pandas as pd
import sqlite3
import pytz

url1='https://drive.google.com/u/0/uc?id=1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025&export=download'
url2='https://drive.google.com/u/0/uc?id=1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg&export=download'
url3='https://drive.google.com/u/0/uc?id=101P9quxHoMZMZCVWQ5o-shonk2lgK1-o&export=download'
output1 = 'table1.csv'
output2 = 'table2.csv'
output3 = 'table3.csv'
d={}

def generate(request):
    alphabet = string.ascii_letters + string.digits
    strin=''.join(secrets.choice(alphabet) for _ in range(10))
    d[strin]='running'
    con = sqlite3.connect('db.sqlite3')
    gdown.download(url1, output1, quiet=False)
    gdown.download(url2, output2, quiet=False)
    gdown.download(url3, output3, quiet=False)
    df1 = pd.read_csv(output1)
    df1.to_sql('table1', con, if_exists='replace', index=False)
    # df1=df1.head(50)  #for testing
    df1['timestamp_utc']=df1['timestamp_utc'].str[:-4]
    df1['timestamp_utc']=pd.to_datetime(df1['timestamp_utc'], errors='coerce')
    df1['timestamp_utc'] = pd.to_datetime(df1['timestamp_utc'] , format='%Y-%m-%d %H:%M:%S.%f')
    df2 = pd.read_csv(output2)
    df2.to_sql('table2', con, if_exists='replace', index=False)
    df3 = pd.read_csv(output3)
    df3.to_sql('table3', con, if_exists='replace', index=False)
    df4=df1.merge(df3, on='store_id', how='left')
    df4['timezone_str']=df4['timezone_str'].fillna('America/Chicago')
    def convert_to_local(row):
        utc_time = row['timestamp_utc'].tz_localize(pytz.UTC)
        local_time = utc_time.tz_convert(pytz.timezone(row['timezone_str']))
        return local_time.tz_localize(None)
    df4['local_time'] = df4.apply(convert_to_local, axis=1) 
    print(df4.head(3)) 
    df4['day'] = df4['local_time'].dt.weekday
    df4['time'] = df4['local_time'].dt.time
    df5=df4.merge(df2,on=['store_id', 'day'],how='left')
    print(df5.head(3),end='final result is : ')
    df5['start_time_local']=df5['start_time_local'].fillna('00:00:00')
    df5['end_time_local']=df5['end_time_local'].fillna('23:59:59')
    df5['start_time_local']=pd.to_datetime(df5['start_time_local'],format='%H:%M:%S').dt.time
    df5['end_time_local']=pd.to_datetime(df5['end_time_local'],format='%H:%M:%S').dt.time
    filtered_df = df5[(df5['status'] == 'active') & (df5['time'] >= df5['start_time_local']) & (df5['time'] <= df5['end_time_local'])]
    result=filtered_df[['store_id','timestamp_utc']]    
    result=result.rename(columns={'timestamp_utc': 'uptime'})
    # result = filtered_df.groupby('store_id')['timestamp_utc'].max().reset_index()
    filtered_df2 = df5[(df5['status'] == 'inactive') & (df5['time'] >= df5['start_time_local']) & (df5['time'] <= df5['end_time_local'])]
    result2=filtered_df2[['store_id','timestamp_utc']]
    result2=result2.rename(columns={'timestamp_utc': 'downtime'})
    current_timestamp = datetime.now()
    max_time_diff1 = timedelta(hours=1)
    max_time_diff2 = timedelta(days=1)
    max_time_diff3 = timedelta(weeks=1000)
    print(max_time_diff3)
    print(current_timestamp)
    result['time_diff'] = current_timestamp - result['uptime']
    result2['time_diff'] = current_timestamp - result2['downtime']
    filtered_dff1 = result[result['time_diff'] <= max_time_diff3]
    count_1= filtered_dff1.groupby('store_id').size().reset_index(name='count')
    filtered_dff2 = result2[result2['time_diff'] <= max_time_diff3]
    count_2= filtered_dff2.groupby('store_id').size().reset_index(name='counts')
    print(str(len(filtered_dff2))+" is "+str(len(filtered_dff1)))
    print("count1 is ")
    print(filtered_dff1.head(5))
    count=pd.merge(count_1, count_2, how='outer', on='store_id')
    count['count']=count['count'].fillna(int('0'))
    count['counts']=count['counts'].fillna(int('0'))
    count['uptime_last_hour(in minutes)']=60*(count['count']/(count['count']+count['counts']))
    count['downtime_last_hour(in minutes)']=60-count['uptime_last_hour(in minutes)']
    print(count.head(2))
    d[strin]=count
    # con.execute("DELETE from table1")
    # con.execute("DELETE from table2")
    # con.execute("DELETE from table3")
    # con.execute("DELETE from timezones")
    # con.execute("DELETE from your_table")
    con.commit()
    con.close()
    print(d)

    return HttpResponse(strin)

def report(request,id):
    if id not in d :
        return HttpResponse("Report not yet generated")
    else:
        return HttpResponse(d[id].to_html())