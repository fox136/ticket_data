import requests
import MySQLdb
import datetime
import json
import time
from lxml import etree

date = ""    # 2021-03-16
db = MySQLdb.connect("127.0.0.1", "root", "**************", "sa", charset='utf8')
cursor = db.cursor()

chg_type = []
chg = [-1]
chg_off_set = 2    # chg可接受的误差

def get_codes():
    sql = 'select code from tick_data GROUP BY code'
    cursor.execute(sql)
    list_tmp = []
    for v in cursor.fetchall():
        list_tmp.append(v[0])
    return list_tmp

list_code = get_codes()

def write_csv(datas, name):
    with open(name, 'w', newline='') as f:
        for row in datas:
            f.write(row + '\n')

def xuangu():
    list_chg_type = []
    list_chg = []
    for code in list_code:
        sql = "select chg, chg_type, close, avg10, avg30, up10, up30, chg10, chg30, avgv, highest, code, date from xuangu where code = '" + code + "' order by date desc"
        print(sql)
        cursor.execute(sql)
        list_data = cursor.fetchall()
        for i in range(len(chg_type)):
            if len(list_data) <= i:
                break
            if float(list_data[i][1]) != chg_type[i]:
                break
            if i == len(chg_type) - 1:
                list_chg_type.append(code)
                break

        for i in range(len(chg)):
            if len(list_data) <= i:
                break
            tmp_chg = float(list_data[i][0])
            if chg[i] < 0:
                if tmp_chg < chg[i] - chg_off_set or tmp_chg > chg[i]:
                    break
            elif chg[i] > 0:
                if tmp_chg > chg[i] + chg_off_set or tmp_chg < chg[i]:
                    break
            else:
                if tmp_chg < chg[i] - chg_off_set or tmp_chg > chg[i] + chg_off_set:
                    break
            if i == len(chg) - 1:
                list_chg.append(code)
                break

    print(len(list_chg_type))
    print(len(list_chg))
    write_csv(list_chg_type, "chg_type.txt")
    write_csv(list_chg, "list_chg.txt")
xuangu()



cursor.close()
db.close()