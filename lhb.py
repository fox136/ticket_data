import requests
import MySQLdb
import datetime
import json
from lxml import etree
import sys

date = "2021-03-17"    # 2021-03-16
db = MySQLdb.connect("127.0.0.1", "root", "************", "sa", charset='utf8')
cursor = db.cursor()

if (len(date) == 0):
    date = datetime.datetime.now().strftime('%Y-%m-%d')

def get_lhb_data():
    url = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=20000,page=1,sortRule=-1,sortType=,startDate=" + date + ",endDate=" + date + ",gpfw=0,js=var%20data_tab_1.html?rt=26931818"
    r = requests.get(url)
    print(url)
    html = r.content
    html = html.decode('gbk')
    html = html[html.find('{'):]
    html = json.loads(html)
    code_table = {}
    for v in html["data"]:
        code_table[v["SCode"]] = v
    print(len(code_table))
    return code_table

def write_csv(datas):
    with open('country.txt', 'w', newline='') as f:
        for row in datas:
            f.write(row + '\n')


def get_type(typeDes):
    type = ""
    if "偏离值达" in typeDes:
        type = "0"
    elif "偏离值累计达" in typeDes:
        type = "1"
    elif "换手率达" in typeDes:
        type = "2"
    return type

def string_to_number(str):
    try:
        return float(str)
    except:
        return 0

my_list = []
code_table = get_lhb_data()
for k in code_table:
    v = code_table[k]
    url = "http://data.eastmoney.com/stock/lhb," + date + "," + v["SCode"] + ".html"
    print(url)
    r = requests.get(url)
    html = r.content
    html = etree.HTML(html)
    #try:
    if True:
        for i in range(0,5):
            for j in range(0,2):
                for j2 in range(0,3):  # 最多有三组
                    try:
                        #rootPath = '/html/body/div[1]/div[2]/div/div[2]/div/div'
                        #rootItem = html.xpath(rootPath)
                        #if (j2*3+7 > len(rootItem)-1):
                        #    continue
                        typeDesPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+6) + ']/div/div[1]/text()'
                        typeDes = html.xpath(typeDesPath)[0].strip() or 'NULL'
                        while True:
                            try:
                                trNumPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[2]/tbody/tr'
                                trNum = len(html.xpath(trNumPath))
                                jingePath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[2]/tbody/tr[' + str(trNum) + ']/td[6]/span/text()'
                                jinge = html.xpath(jingePath)[0].strip() or 'NULL'
                                type = get_type(typeDes)
                                buyPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[2]/tbody/tr[' + str(trNum) + ']/td[2]/span/text()'
                                salePath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[2]/tbody/tr[' + str(trNum) + ']/td[4]/span/text()'
                                buy = html.xpath(buyPath)[0].strip() or 'NULL'
                                sale = html.xpath(salePath)[0].strip() or 'NULL'
                                buy = string_to_number(buy)
                                sale = string_to_number(sale)
                                selectSql = "select * from lhb where ticker = '%s' and date = '%s' and type = '%s' and side = '%s' and dep = 'ss'" % \
                                            (v["SCode"], date, type, 'S')
                                cursor.execute(selectSql)
                                if cursor.fetchall():
                                    break
                                insertSql = "insert into lhb (ticker, name, date, dep, buy, sale, side, type, jinge) values('%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s')" % \
                                            (v["SCode"], v["SName"], date, 'ss', buy, sale, "S", type, jinge)
                                print(typeDes)
                                my_list.append(insertSql + ';')
                                cursor.execute(insertSql)
                                db.commit()
                                break
                            except:
                                break

                        jingePath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[7]/text()'
                        jinge = html.xpath(jingePath)[0] or 'NULL'

                        rankPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[1]/text()'
                        depPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[2]/div[1]/a[2]/text()'
                        buyPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[3]/text()'
                        salePath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[5]/text()'
                        timesPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[2]/div[2]/div[1]/span[1]/text()'
                        percentPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[2]/div[2]/div[1]/span[2]/text()'
                        flagPath = '/html/body/div[1]/div[2]/div/div[2]/div/div[' + str(j2*3+7) + ']/table[' + str(j+1) + ']/tbody/tr[' + str(i+1) + ']/td[2]/div[2]/div[1]/span[2]/attribute::*'

                        rank = html.xpath(rankPath)[0].strip() or 'NULL'
                        if rank == "NULL":
                            continue
                        dep = html.xpath(depPath)[0].strip() or 'NULL'
                        buy = html.xpath(buyPath)[0].strip() or 'NULL'
                        sale = html.xpath(salePath)[0].strip() or 'NULL'
                        side = "B" if j == 0 else 'S'
                        type = get_type(typeDes)
                        times = html.xpath(timesPath)[0].strip() or 'NULL'
                        percent = html.xpath(percentPath)[0].strip() or 'NULL'
                        color = html.xpath(flagPath)
                        flag = '0' if "color:red" in color else 'NULL'
                        flag = '1' if "color:Green" in color and flag != '0' else 'NULL'

                        buy = string_to_number(buy)
                        rank = string_to_number(rank)
                        flag = string_to_number(flag)
                        sale = string_to_number(sale)
                        #selectSql = "select * from lhb where ticker = '%s' and date = '%s' and type = '%s' and side = '%s' and rank = '%s'" %\
                        #            (v["SCode"], date, type, side, rank)
                        #cursor.execute(selectSql)
                        #if cursor.fetchall():
                        #    continue
                        insertSql = "insert into lhb (ticker, name, date, dep, buy, sale, rank, side, type, percent, times,flag, jinge) values('%s', '%s', '%s', '%s', %s, %s, %s, '%s', '%s', '%s', '%s', %s, %s)" %\
                            (v["SCode"], v["SName"], date, dep, buy, sale, rank, side, type, percent, times, flag, jinge)
                        #print(insertSql)
                        my_list.append(insertSql + ';')
                        cursor.execute(insertSql)
                        db.commit()
                    except Exception as e:
                        print(j2, j, i, sys._getframe().f_lineno, 'str(e):\t\t', str(e))
                        pass
    #except Exception as e:
        pass

print('总共写入数据条数：', len(my_list))

#write_csv(my_list)