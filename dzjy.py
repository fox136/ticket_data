import requests
import MySQLdb
import datetime
import json
from lxml import etree

date = ""    # 2021-03-16
db = MySQLdb.connect("127.0.0.1", "root", "**************", "sa", charset='utf8')
cursor = db.cursor()

if (len(date) == 0):
    date = datetime.datetime.now().strftime('%Y-%m-%d')

my_list = []
def get_lhb_data():
    #       'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?callback=jQuery1123017594276632936667_1616069471476&st=SECUCODE&sr=1&ps=50&p=1&type=DZJYXQ&js=%7Bpages%3A(tp)%2Cdata%3A(x)%7D&filter=(Stype%3D%27EQA%27)(TDATE%3E%3D%5E2021-03-18%5E+and+TDATE%3C%3D%5E2021-03-18%5E)&token=70f12f2f4f091e459a279469fe49eca5'
    url = 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?callback=jQuery11230013004667705900363_1615997891496&st=SECUCODE&sr=1&ps=5000&p=1&type=DZJYXQ&js=%7Bpages%3A(tp)%2Cdata%3A(x)%7D&filter=(Stype%3D%27EQA%27)(TDATE%3E%3D%5E' + date + '%5E+and+TDATE%3C%3D%5E' + date + '%5E)&token=70f12f2f4f091e459a279469fe49eca5'
    r = requests.get(url)
    print(url)
    html = r.content
    html = html.decode('utf-8')
    html = html[html.find('[{'):-2]
    html = json.loads(html)
    for v in html:
        print(v)
        ticker = v['SECUCODE']
        secShortName = v['SNAME']
        tradeDate = date
        buyerBd = v['BUYERNAME']
        sellerBd = v['SALESNAME']
        tradePrice = v['PRICE']
        tradeVal = v['TVAL']
        tradeVol = v['TVOL']
        yjl = str(round(v['Zyl'] * 100, 2)) + "%"

        #selectSql = "select * from dzjy where ticker = '%s' and tradeDate = '%s' and buyerBd = '%s' and sellerBd = '%s'and tradePrice = %s and tradeVal = %s and tradeVol = %s and yjl = '%s'" % \
        #            (ticker, tradeDate, buyerBd, sellerBd, str(tradePrice), str(tradeVal), str(tradeVol), yjl)
        #cursor.execute(selectSql)
        #if cursor.fetchall():
        #    continue

        insertSql = "insert into dzjy (ticker, secShortName, tradeDate, buyerBd, sellerBd, tradePrice, tradeVal, tradeVol, yjl) " \
                    "values('%s', '%s', '%s', '%s', '%s', %s, %s, %s, '%s')" % (ticker, secShortName, tradeDate, buyerBd, sellerBd, str(tradePrice), str(tradeVal), str(tradeVol), yjl)
        print(insertSql)
        my_list.append(insertSql)
        cursor.execute(insertSql)
        db.commit()
    print(len(html))
get_lhb_data()

print(len(my_list))