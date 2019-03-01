from pymongo import MongoClient
import configparser
# connection

cfg = "./config.cfg"
config_raw = configparser.RawConfigParser()
config_raw.read(cfg)
defaults = config_raw.defaults()
connString = config_raw.get('MongoDB', 'connstring')
database = config_raw.get('MongoDB', 'database')
stockListCollection = config_raw.get('MongoDB', 'stockListCollection')
stockInfoCollection = config_raw.get('MongoDB', 'stockInfoCollection')
stockParserLogCollection = config_raw.get('MongoDB', 'stockParserLogCollection')

if connString == '':
	conn = MongoClient()
else:
	conn = MongoClient(connString)

db = conn[database]

def InsertStockList(stockList):	
	collection = db[stockListCollection]
	collection.insert_many(stockList)

def InsertOrUpdateStockInfo(stockInfoList):
	collection = db[stockInfoCollection]
	collection.insert_many(stockInfoList)

def InsertParserLog(log):
	collection = db[stockParserLogCollection]
	collection.insert_one(log)