import requests
import MySQLdb
import datetime
import json
from lxml import etree

date = ""    # 2021-03-16
db = MySQLdb.connect("127.0.0.1", "root", "******", "sa", charset='utf8')
cursor = db.cursor()

caiji_date = '2020-12-31'    # 采集日期

sql_date = '2022-12-31'      # 入表日期

#url = 'http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date=2020-09-30&code=SZ002056'

def get_codes():
    codes = []
    f = open('stock.txt')
    while True:
        content = f.readline()
        if content == '':
            break
        content = content[0:6]
        codes.append(content[0:6])
    f.close()
    return codes

codes = get_codes()

def get_last_cc(code):
    sql = "select volume from sdltgd where ticker = '%s' and name = '基金/机构家数'" % (code)
    cursor.execute(sql)
    for v in cursor.fetchall():
        return v[0]
    return "0"

def write_sql(code, ccjs, zltgbl):
    deleteSql = "delete from jgcc where code = '%s' and date = '%s' and ccjs = '%s' and zltgbl = '%s'" % (
    code, sql_date, ccjs, zltgbl)
    cursor.execute(deleteSql)
    insertSql = "insert into jgcc (code, date, ccjs, zltgbl) " \
                "values('%s', '%s', '%s', '%s')" % (code, sql_date, ccjs, zltgbl)
    print(insertSql)
    cursor.execute(insertSql)
    db.commit()

    last_ccjs = get_last_cc(code)
    change = "增加" if int(ccjs) >= int(last_ccjs) else "减少"
    deleteSql = "delete from sdltgd where ticker = '%s' and date = '%s' and name = '基金/机构家数'" % (
    code, sql_date)
    cursor.execute(deleteSql)
    insertSql = "insert into sdltgd (code, ticker, name, date, volume, ratio, `change`, getdate) values('11', '%s', '基金/机构家数', '%s', %s, '%s', '%s', '%s 00:00:00') " \
                % (code, sql_date, ccjs, zltgbl, change, caiji_date)
    print(insertSql)
    cursor.execute(insertSql)
    db.commit()

def get_jgcc_data(code): # 获取机构持仓数据
    # url = 'http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date=2020-09-30&code=SZ002056'
    ticket = 'SZ' + code if code < '600000' else 'SH' + code
    url = 'http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date=' + caiji_date + '&code=' + ticket
    r = requests.get(url)
    print(url)
    html = r.content
    html = json.loads(html)
    for v in html:
        print(v)
        if v['jglx'] == "合计":
            ccjs = v['ccjs'] if v['ccjs'] != "--" else ""
            zltgbl = v['zltgbl']  if v['zltgbl'] != "--" else ""
            print(ccjs, zltgbl)
            if len(ccjs) == 0 and len(zltgbl) == 0:
                return
            write_sql(code, ccjs, zltgbl)


def write_ccjc():
    for v in codes:
        get_jgcc_data(v)

#get_jgcc_data('002056')
write_ccjc()