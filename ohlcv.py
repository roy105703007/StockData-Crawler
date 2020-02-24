import requests
import urllib3
from bs4 import BeautifulSoup
import re
from io import StringIO
import pandas as pd
import pymssql
from datetime import datetime

conn = pymssql.connect(server='10.5.20.86', user='sa', password='118758', database='CMoneyDB')
cursor = conn.cursor()


def cleantxt(raw):
    fil = re.compile(u'[^0-9a-zA-Z\u4e00-\u9fa5.，,。？“”]+', re.UNICODE)
    return fil.sub(' ', raw)

def findOHLCV(code):
    lastDate = None
    datestr = str(datetime.timestamp(datetime.strptime("2020-02-24", "%Y-%m-%d"))).split('.')[0]
    site = "https://query1.finance.yahoo.com/v7/finance/download/" + code + ".TW?period1=0&period2=" + datestr + "&interval=1d&events=history&crumb=hP2rOschxO0"
    response = requests.post(site)
    df = pd.read_csv(StringIO(response.text))
    df = df.dropna()
    dflist = df.values.tolist()
    try :
        cursor.execute('SELECT TOP 1 term FROM dbo.OHLCV WHERE stockCode = ' + code + ' ORDER BY term DESC')
        lastDate = cursor.fetchone()[0]
        pass
    except Exception as e:
        print(e)
        pass
    for lst in dflist:
        if not lastDate or lst[0].replace('-','') > lastDate: 
            sqlString = "INSERT INTO dbo.OHLCV (stockCode, term, openPrice, highPrice, lowPrice, closePrice, adjustClosePrice, volumn) VALUES "+ str((code, lst[0].replace('-',''), lst[1], lst[2], lst[3], lst[4], lst[5], lst[6]))
            cursor.execute(sqlString)
            conn.commit()



# # Google 搜尋 URL
google_url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
r = requests.get(google_url, verify=False)

soup = BeautifulSoup(r.text, 'html.parser')
items = soup.find('table',{"class" : "h4"})
data = []
for row in items.find_all("tr"):
    for col in row.find_all("td"):
        if '權證' in col.text:
            exit(0)
        if col.get('bgcolor') == '#FAFAD2' and col.get('colspan') == None:
            newData = cleantxt(col.text).split(' ')
            cursor.execute("SELECT TOP 1 * FROM dbo.stock WHERE code = " + newData[0])
            if not cursor.fetchone():
                sqlString = "INSERT INTO dbo.stock (code, stockName) VALUES "+ str((newData[0],newData[1]))
                cursor.execute(sqlString)
                conn.commit()
            print(newData[0])
            findOHLCV(newData[0])
        break
