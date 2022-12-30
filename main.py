import pandas as pd
import sqlite3
import requests
from datetime import datetime
import os
import tweepy

url = 'https://www.mketf.com/wp-content/fund_files/files/TidalETF_Services.40ZZ.K3_Holdings_PP.csv'

conn = sqlite3.connect('database.db')

def initDB():
    cursor = conn.cursor()
    query = 'CREATE VIEW if not EXISTS latest as SELECT * from holdings WHERE Date = (SELECT DISTINCT date from holdings LIMIT 1 OFFSET 0)'
    cursor.execute(query)
    query = 'CREATE VIEW if not EXISTS dayBefore as SELECT * from holdings WHERE Date = (SELECT DISTINCT date from holdings LIMIT 1 OFFSET 1)'
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
    with open('temp.csv', 'w') as f:
        f.write(response.text)
    df = pd.read_csv('temp.csv')
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
    query = 'SELECT DISTINCT date from holdings LIMIT 1 OFFSET 1'
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
    with open('date.txt', 'w') as f:
        f.write(text)

def readDate():
    with open('date.txt', 'r') as f:
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
    print(msg)
    # auth = tweepy.OAuthHandler("wEiv7tt5dp87HAEIrMSaEhkAI", "hhU1Vw29KYCveMAAhVGGj9deV8MEobdpduXuV6FeSdo8JGUeEz")
    # auth.set_access_token("1575742272960102400-k1loWQ9cnITIcmWPehyvzb1m7vmm1J", "9WGA5LnsWHNVT2cUhmMs0E0ybZrQP51AAz4eUHwGbinV4")
    # api = tweepy.API(auth)
    # api.update_status(msg)

def main():
    initDB()
    getPP()
    currentDate = checkDate()
    if  currentDate != readDate():
        stocks = getChange()
        responses = generateResponses(stocks)
        for response in responses:
            postTweet(response)
        writeDate(currentDate)
        


if __name__ == "__main__":
    main()