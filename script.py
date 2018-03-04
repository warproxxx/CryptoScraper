import datetime
import calendar
import requests
import pandas as pd
import json
import os.path
import time

if (os.path.isfile("finex.csv")): #if the file already exists start from the latest date
    starttime = datetime.datetime.fromtimestamp(int(str(int(pd.read_csv('finex.csv', header=None).iloc[-1][0]))[:-3])) #read the last timestamp for csv file. Bitstamp takes and returs date date with 3 extra zeros. So that
else:
    starttime = datetime.datetime.strptime('01/04/2013', '%d/%m/%Y') #Start collecting from April 1, 2013

start_unixtime = calendar.timegm(starttime.utctimetuple())

latest_time = int(time.time() - 60 * 60 * 24) #The real ending time. Collect data from starttime to current time - 24 hours

track_time = time.time() #because bitstamp only allows 10 requests per minute. Take rest if we are faster than that
count = 0

while (start_unixtime < latest_time):
    end_unixtime = start_unixtime + 60*60*24*30 #30 days at a time
    
    if (end_unixtime > latest_time):
        end_unixtime = latest_time #if the time is in future.

    url = 'https://api.bitfinex.com/v2/candles/trade:1h:tBTCUSD/hist?start={}&end={}&limit=1000'.format(str(start_unixtime) + "000", str(end_unixtime) + "000") #1 hour can be changed to any timeframe
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data).set_index(0).sort_index() #set the date column as index and sort all data

    df.to_csv('finex.csv',header=None,mode='a') #append the data
    
    print('Saved till {}'.format(datetime.datetime.fromtimestamp(int(end_unixtime)).strftime('%Y-%m-%d %H:%M:%S')))
    
    start_unixtime = end_unixtime + 60 * 60 #to prevent duplicates
    count = count + 1
    
    if (count == 10): #if 10 requests are made
        count = 0 #reset it
        
        diff = time.time() - track_time
        
        if (diff <= 60):
            print('Sleeping for {} seconds'.format(str(60 - diff)))
            time.sleep(60 - diff) #sleep
            
        
        track_time = time.time()
    #bitstamp limits to 10 requests per minute
	

#add the header
df = pd.read_csv('finex.csv', header=None, index_col=None)

df.columns = ['Time', 'Open', 'Close', 'High', 'Low', 'Volume']
    
df.set_index('Time')
df.to_csv('final.csv', index=False) 