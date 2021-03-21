import requests
import MySQLdb
import datetime
import json
import time
from lxml import etree

date = "2021-02-03"    # 2021-02-03
db = MySQLdb.connect("127.0.0.1", "root", "**********", "sa", charset='utf8')
cursor = db.cursor()

days = 130

test_code = ['300003', '300005'] # 如果配了这个数据，则只跑这些股票

if (len(date) == 0):

    date = datetime.datetime.now().strftime('%Y-%m-%d')
    print(date)

def get_codes():
    sql = 'select code from tick_data GROUP BY code'
    cursor.execute(sql)
    list_tmp = []
    for v in cursor.fetchall():
        list_tmp.append(v[0])
    return list_tmp

list_code = get_codes()


def get_avg(list_data, n):
    total = 0
    m = 0
    for v in list_data:
        total += v[2]
        m += 1
        if (m == n):
            break
    if m > 0:
        return round(total / m, 2)
    else:
        return 0

def get_up(code, close, avgn, n):
    sql = "select up10, up30 from xuangu where code = '" + code + "' and date <= '" + date + "'  ORDER BY date desc limit 1"
    count = 0
    cursor.execute(sql)
    data_tmp = cursor.fetchall()
    if (len(data_tmp) > 0):
        tmp = data_tmp[0]
        if n == 10:
            count = int(tmp[0])
        elif n == 30:
            count = int(tmp[1])
    if close >= avgn:
        count += 1
    else:
        count = 0
    print(count)
    return count

def get_highest(list_data, high):
    count = 0
    for v in list_data:
        if high >= v[3]:
            count += 1
        else:
            break
    return count - 1

def xuangu():
    for code in list_code:
        if len(test_code) > 0 and code not in test_code:
            continue
        sql = "select date, open, close, high, low, volume, code, amount, id, zj, avgv from tick_data where code = '" + code + "' and date <= '" + date + "' ORDER BY date desc LIMIT "+ str(days)
        print(sql)
        cursor.execute(sql)
        list_data = cursor.fetchall()
        if len(list_data) < 2:
            continue
        if list_data[0][0] != date:
            continue
        chg = (list_data[0][2] - list_data[1][2]) / list_data[1][2]
        chg = round(chg * 100, 2) #涨跌幅
        chgType = 1 if chg >= 0 else 2  #1表示涨， 2表示跌
        close = list_data[0][2]
        avg10 = get_avg(list_data, 10)
        avg30 = get_avg(list_data, 30)

        up10 = get_up(code, close, avg10, 10)
        up30 = get_up(code, close, avg30, 30)
        chg10= round((close - avg10)/avg10*100, 2)
        chg30= round((close - avg30)/avg30*100, 2)
        avgv = list_data[0][10]
        highest = 0 # get_highest(list_data, list_data[0][3])
        insertSql = "insert into xuangu(chg, chg_type, close, avg10, avg30, up10, up30, chg10, chg30, avgv, highest, code, date) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %\
            (chg, chgType, close, avg10, avg30, up10, up30, chg10, chg30, avgv, highest, code, date)
        print(insertSql)
        try:
            cursor.execute(insertSql)
            db.commit()
            pass
        except:
            print("写入失败：insertSql", insertSql)

def get_next_date():
    # 字符串转换为时间戳
    timeArray = time.strptime(date, "%Y-%m-%d")
    timeStamp = int(time.mktime(timeArray))

    # 时间戳转换为字符串
    timeArray = time.localtime(timeStamp + 86400)
    timeStr = time.strftime("%Y-%m-%d", timeArray)
    return timeStr

def get_now_date():
    return datetime.datetime.now().strftime('%Y-%m-%d')

while True:
    xuangu()
    print(date, get_now_date(), date > get_now_date())
    date = get_next_date()
    if date > get_now_date():
        break

cursor.close()
db.close()
