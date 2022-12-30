import pandas as pd
import sqlite3
import requests
from datetime import datetime
import os
import tweepy
import logging
import urllib3
import time
from dotenv import load_dotenv
load_dotenv()

apiKey = os.environ.get('apiKey')
apiSecret = os.environ.get('apiSecret')
accessToken = os.environ.get('accessToken')
accessSecret = os.environ.get('accessSecret')
sleep = os.environ.get('sleep')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(filename='main.log', format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

url = 'https://www.mketf.com/wp-content/fund_files/files/TidalETF_Services.40ZZ.K3_Holdings_PP.csv'
if not os.path.exists('db'):
    os.makedirs('db')
conn = sqlite3.connect('db/database.db')

def initDB():
    cursor = conn.cursor()
    query = 'CREATE VIEW if not EXISTS latest as SELECT * from holdings WHERE Date = (SELECT DISTINCT date from holdings ORDER BY date DESC LIMIT 1 OFFSET 0)'
    cursor.execute(query)
    query = 'CREATE VIEW if not EXISTS dayBefore as SELECT * from holdings WHERE Date = (SELECT DISTINCT date from holdings ORDER BY date DESC LIMIT 1 OFFSET 1)'
    cursor.execute(query)
    query = '''
    CREATE VIEW if not EXISTS change as SELECT stockticker, 
    (latest.Shares - dayBefore.Shares) as change, 
    (latest.Shares - 0) as latest, 
    (latest.Weightings - 0) as latestW, 
    (dayBefore.Shares - 0) as dayBefore,
    (dayBefore.Weightings - 0) as dayBeforeW
    from latest left JOIN dayBefore using (stockticker)
    union
    SELECT stockticker, 
    (latest.Shares - dayBefore.Shares) as change, 
    (latest.Shares - 0) as latest, 
    (latest.Weightings - 0) as latestW, 
    (dayBefore.Shares - 0) as dayBefore,
    (dayBefore.Weightings - 0) as dayBeforeW
    from dayBefore left JOIN latest using (stockticker)
    '''
    cursor.execute(query)
    conn.commit()
    for filename in os.listdir('files/'):
        df = pd.read_csv('files/' + filename)
        importPP(df)

def getPP():
    response = requests.get(url)
    with open('db/temp.csv', 'w') as f:
        f.write(response.text)
    df = pd.read_csv('db/temp.csv')
    df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y").strftime("%Y-%m-%d"))
    today = df.iloc[0, df.columns.get_loc('Date')]
    df.to_csv('files/' + today + '.csv', index=False)
    return df

def importPP(df):
    df.to_sql('holdings', conn, if_exists='append', index=False)
    cursor = conn.cursor()
    cursor.execute("""DELETE FROM holdings
    WHERE EXISTS (
    SELECT 1 FROM holdings p2 
    WHERE holdings.Date = p2.Date
    AND holdings.StockTicker = p2.StockTicker
    AND holdings.rowid > p2.rowid
    );
    """)
    conn.commit()

def checkDate():
    cursor = conn.cursor()
    query = 'SELECT DISTINCT date from holdings ORDER BY date LIMIT 1 OFFSET 1'
    cursor.execute(query)
    x = cursor.fetchall()
    conn.commit()
    return x[0][0]

def getChange():
    query = 'select * from change'
    df = pd.read_sql(query, conn)
    print(df)
    return(df)

def writeDate(text):
    with open('db/date.txt', 'w') as f:
        f.write(text)

def readDate():
    with open('db/date.txt', 'r') as f:
        text = f.read()
    return text

def generateResponses(stocks):
        responses = []
        for index, stock in stocks.iterrows():
            if stock['change'] != stock['change']:
                if stock['latest'] != stock['latest']:
                    responses.append("❌ Kevin closes his positon in ${}. Previously, {}% of $PP".format(stock['StockTicker'], stock['dayBeforeW']) )
                else:
                    responses.append("✅ Kevin opens a positon in ${}. Currently, {}% of $PP".format(stock['StockTicker'], stock['latestW']) )
            elif stock['change'] > 0:
                responses.append("🟢 Kevin increases his positon in ${} by {}%. Currently, {}% of $PP".format(stock['StockTicker'], round(stock['change']/stock['dayBefore'] * 100, 2), stock['latestW']) )
            elif stock['change'] < 0:
                responses.append("🔴 Kevin decreases his positon in ${} by {}%. Currently, {}% of $PP".format(stock['StockTicker'], round(stock['change']/stock['dayBefore'] * -100, 2), stock['latestW']) )
            else:
                continue
        return responses

def postTweet(msg):
    logging.info(msg)
    print(msg)
    try:
        auth = tweepy.OAuthHandler(apiKey, apiSecret)
        auth.set_access_token(accessToken, accessSecret)
        api = tweepy.API(auth)
        api.update_status(msg)
    except Exception as e:
        logging.critical("Tweet Error")
        logging.CRITICAL(e)

def main():
    initDB()
    while (1):
        try:
            logging.info("Checking Holdings")
            df = getPP()
            importPP(df)
            currentDate = checkDate()
            if  currentDate != readDate():
                stocks = getChange()
                responses = generateResponses(stocks)
                for response in responses:
                    postTweet(response)
                writeDate(currentDate)
        except Exception as e:
            logging.critical("Main Error")
            logging.critical(e)

        logging.info("Sleeping")
        time.sleep(int(sleep))
            


if __name__ == "__main__":
    main()