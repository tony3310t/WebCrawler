import sys
import configparser
from datetime import datetime, timedelta
import requests
from io import StringIO
import pandas as pd
import numpy as np
from MongoDB import *
from operator import itemgetter

cfg = "./config.cfg"
config_raw = configparser.RawConfigParser()
config_raw.read(cfg)
defaults = config_raw.defaults()
connString = config_raw.get('DEFAULT', 'url')
print(connString)

def GetDataFrame(dateString):
	r = requests.post(connString + dateString + '&type=ALLBUT0999')
	df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
										for i in r.text.split('\n') 
										if len(i.split('",')) == 17 and i[0] != '='])), header=0)

	return df
	

def GetStockList():
	#dateString = '20190224'
	for idx in range(10):
		try:
			dateString = datetime.strftime(datetime.now() - timedelta(idx), '%Y%m%d')
			print(dateString)			
			
			df = GetDataFrame(dateString)
			stockList = []
			stockNOList = df['證券代號']
			stockNameList = df['證券名稱']
			
			if len(stockNOList) != len(stockNameList):
				print('Stock list parse error.')
				break

			for index in range(len(stockNOList)):
				stock = {}
				stock['Name'] = stockNameList[index]
				stock['No'] = stockNOList[index]				
				stockList.append(stock)
			
			InsertStockList(stockList)
			break
		except:
			print('Error. Retry: ' + str(idx + 1))

def GetStockInfo():
	tmpList = GetAllLog()
	logList = sorted(tmpList, key=itemgetter('Date'), reverse=True)
	dateList = []

	if(len(logList) >0):
		latestDate = logList[0].get("Date")
		count = 0
		while True:
			tmpDate = datetime.strftime(datetime.now() - timedelta(count), '%Y%m%d')
			if tmpDate == latestDate:
				break

			dateList.append(tmpDate)
			count = count + 1
		
		matches = [d['Date'] for d in logList if d['IsSuccess'] == False and d["IsWeekend"] == False]
		dateList.extend(matches)
	else:
		days = 3000
		#days = Not set
		for idx in range(days):
			tmpDate = datetime.strftime(datetime.now() - timedelta(idx), '%Y%m%d')
			dateList.append(tmpDate)
		
	for idx in range(len(dateList)):
		isWeekend = False
		dateString = dateList[idx]

		try:
			weekNo = datetime.strptime(dateString, '%Y%m%d').weekday()
			if weekNo >= 5:
				isWeekend = True
		except:
			isWeekend = False

		print(dateString)
		try:		
			df = GetDataFrame(dateString)

			stockInfoList = []
			stockNOList = df['證券代號']
			stockTranMountList = df['成交股數']
			stockTranPieceList = df['成交筆數']
			stockOpenList = df['開盤價']
			stockHighList = df['最高價']
			stockLowList = df['最低價']
			stockCloseList = df['收盤價']
			stockPERatioList = df['本益比']

			for index in range(len(stockNOList)):
				stockInfo = {}
				stockInfo['StockInfo'] = {
					"StockPERatio":stockPERatioList[index],
					"stockCloseList":stockCloseList[index],
					"stockLowList":stockLowList[index],
					"stockHighList":stockHighList[index],
					"stockOpenList":stockOpenList[index],
					"stockTranPieceList":stockTranPieceList[index],
					"stockTranMountList":stockTranMountList[index]
				}
				stockInfo['Date'] = dateString
				stockInfo['No'] = stockNOList[index]				
				stockInfoList.append(stockInfo)

			InsertOrUpdateStockInfo(stockInfoList)
			log = {"Date":dateString, "IsSuccess":True, "IsWeekend":isWeekend}
			InsertParserLog(log)
		except:
			log = {"Date":dateString, "IsSuccess":False, "IsWeekend":isWeekend}
			InsertParserLog(log)
			print('Error')

if __name__ == "__main__":
	cmd = 1
	#cmd = int(sys.argv[1])
	if cmd == 1314:
		GetStockList()
	elif cmd == 1:
		GetStockInfo()