import datetime
import calendar
import requests
import pandas as pd
import json
import os.path
import time
import CryptoScraper

class BtcFinex:
    
    def __init__(self):
        self.currdir = os.path.dirname(CryptoScraper.__file__)
        self.cacheloc = self.currdir + "/cache/downloading.csv"
            
        if (os.path.isfile(self.cacheloc)): #if the file already exists start from the latest date
            self.starttime = datetime.datetime.fromtimestamp(int(str(int(pd.read_csv(self.cacheloc, header=None).iloc[-1][0]))[:-3])) #read the last timestamp for csv file. Bitstamp takes and returs date date with 3 extra zeros. So that
        else:
            self.starttime = datetime.datetime.strptime('01/04/2013', '%d/%m/%Y') #Start collecting from April 1, 2013
    
	
    def loadData(self):
        '''
        Downloads data from bitfinex (from zero or from cache)
        '''
        start_unixtime = calendar.timegm(self.starttime.utctimetuple())
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

            df.to_csv(self.cacheloc,header=None,mode='a') #append the data

            print('Saved till {}'.format(datetime.datetime.fromtimestamp(int(end_unixtime)).strftime('%Y-%m-%d %H:%M:%S')))

            start_unixtime = end_unixtime
            count = count + 1

            if (count == 10): #if 10 requests are made
                count = 0 #reset it

                diff = time.time() - track_time

                if (diff <= 60):
                    print('Sleeping for {} seconds'.format(str(60 - diff)))
                    time.sleep(60 - diff) #sleep


                track_time = time.time()
            #bitstamp limits to 10 requests per minute
        
    def getCleanData(self):
        '''
        Some values are missing in data bitfinex provides. Forward fill them. Optionally print amount of missing data and total.
        '''
		#add the header
        df1 = pd.read_csv(self.cacheloc, header=None, index_col=None)
        df1.columns = ['Time', 'Open', 'Close', 'High', 'Low', 'Volume']
        df1.set_index('Time')
        df1.to_csv(self.currdir + '/cache/final-rough.csv', index=False) 
		
        df = pd.read_csv(self.currdir + '/cache/final-rough.csv')
        #df['Time'] = [str(x)[:-3] for x in df['Time']]
        #pd.to_datetime(df['Time'], unit='s')
        dates = pd.DataFrame([x for x in range(df['Time'][0], int(df.iloc[-1]['Time']), 3600000)])
        dates.columns = ['Time']
        df.set_index('Time', inplace=True)
        dates.set_index('Time', inplace=True)
        df.drop_duplicates(inplace=True)
        full_data = pd.concat([df, dates], axis=1).fillna(method='ffill')
        full_data.index = [str(x)[:-3] for x in full_data.index]
        full_data.index.name = 'Time'
        full_data.to_csv(self.currdir + '/cache/final-clean.csv')
        df = pd.read_csv(self.currdir + '/cache/final-clean.csv')
        assert(sum(df['Time'] - df.shift(1)['Time'] != 3600.0) == 1)
        return df