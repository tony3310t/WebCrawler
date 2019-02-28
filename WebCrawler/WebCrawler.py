import sys
import configparser
from datetime import datetime, timedelta
import requests
from io import StringIO
import pandas as pd
import numpy as np

cfg = "./config.cfg"
config_raw = configparser.RawConfigParser()
config_raw.read(cfg)
defaults = config_raw.defaults()
connString = config_raw.get('DEFAULT', 'connstring')
print(connString)

def GetStockList():
	#dateString = '20190224'
	for idx in range(10):
		try:
			dateString = datetime.strftime(datetime.now() - timedelta(idx), '%Y%m%d')
			print(dateString)
			r = requests.post(connString + dateString + '&type=ALLBUT0999')
			df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
											 for i in r.text.split('\n') 
											 if len(i.split('",')) == 17 and i[0] != '='])), header=0)
			print(df['證券代號'])
			break
		except:
			print('Error. Retry: ' + str(idx+1))



if __name__ == "__main__":
	cmd = 1
	#cmd = int(sys.argv[1])

	if cmd == 1:
		GetStockList()
