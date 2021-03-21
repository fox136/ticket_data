#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import MySQLdb

db = MySQLdb.connect("127.0.0.1", "root", "*********", "sa", charset='utf8')
cursor = db.cursor()

def get_diff_value(code, holding_value):
    select_sql = "select holding_value from hk_hold where ts_code = %s ORDER BY trade_date desc LIMIT 1;" % (code)
    cursor.execute(select_sql)
    for v in cursor.fetchall():
        return holding_value - v[0]
    return 0
def get_url(p):
    tmp = requests.get("http://data.eastmoney.com/hsgtcg/list.html").content.decode('gb2312')
    begin = tmp.find("个股排行<span>（") + len("个股排行<span>（")
    date = tmp[begin : begin + 10]
    url = "http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGT20_GGTJ_SUM&token=894050c76af8597a853f5b408b759f5d&st=ShareSZ_Chg_One&sr=-1&p=" + str(p) + "&ps=5000&js=var%20vXyYqBmc={pages:(tp),data:(x)}&filter=(DateType=%271%27%20and%20HdDate=%27" + date + "%27)&rt=52961756"
    return url
url = get_url(1)
print(url)
r = requests.get(url)
html = r.content
html = html.decode('utf-8')
html = html[html.find('{'):]
html = html.replace("pages", "'pages'").replace("data", "'data'")
print(html)
html = eval(html)
for a in html['data']:
    try:

        1/0
        diff_value = get_diff_value(a['SCode'], a['ShareSZ'])
        insert_sql = "insert into hk_hold(code, trade_date, ts_code, vol, ratio, holding_value, diff_value) value(%s, '%s', '%s', %s, %s, %s, %s)" % \
                     ('0', a['HdDate'].replace("-", ""), a['SCode'], str(int(a['ShareHold'])), a['SharesRate'], str(int(a['ShareSZ'])), str(int(diff_value)))
        cursor.execute(insert_sql)
        print(insert_sql)
        db.commit()
    except:
        print("errir: ", a)