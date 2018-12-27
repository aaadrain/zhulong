import time

import psycopg2
import os

city = input("查询的城市：")
os.chdir(os.path.dirname(os.getcwd()))
os.chdir(city)
current_doculist = os.listdir()

con = psycopg2.connect(user='postgres',password='since2015',host='192.168.3.171',port="5432",database=city)

cur = con.cursor()
sql = "select * from pg_tables"

# 查出所有城市
sql1 = """select distinct schemaname from pg_tables;"""
cur.execute(sql1)
schemaname = cur.fetchall()
# [('danyang',), ('yancheng',), ('wuxi',), ('nanjing',), ('zhenjiang',), ('jiangyin',), ('dongtai',), ('information_schema',), ('nantong',), ('taizhou',), ('pg_catalog',), ('changshu',)
city_name = []
for name in schemaname:
    if name[0] +".py" in current_doculist:
        city_name.append(name[0])
# print("city_lenth",len(city_name),city_name)

# 查出某个城市所有表名
# sql2 = """select schemaname,tablename from pg_tables where schemaname='changshu';"""
sql2 = """select schemaname,tablename from pg_tables;"""
cur.execute(sql2)
s_tablename = cur.fetchall()
#[('changshu', 'zfcg_biangen_gg_cdc'), ('changshu', 'zfcg_biangen_gg'), ('changshu', 'zfcg_zhongbiao_gg'), ('changshu', 'zfcg_zhaobiao_danyilaiyuan_gg'), ('changshu', 'gg'), ('changshu
table_name = []
for name in s_tablename:
    table_name.append(name[0])
# print("table_lenth",len(table_name),table_name)

#查询表内是否有重复数据
count = 0
for name in s_tablename:
    if name[0] in city_name and name[1] != 'gg_html':
        sql3 = """select count(href),name,ggstart_time,href from %s.%s group by name,ggstart_time,href having count(href) >2 order by ggstart_time desc;"""%(name[0],name[1])
        cur.execute(sql3)
        result = cur.fetchall()
        if result != []:
            if "cdc" not in name[1]:
                print("模式名：",name[0],"表名：",name[1],"重复数量：",len(result))
                title = "模式名：",name[0],"表名：",name[1],"重复数量：",len(result)
                with open("./" + city + '.txt', "a", encoding="utf-8") as f:
                    f.write(str(title)+'\n')
                for re in result:
                    print(re)
                    with open("./"+city+'.txt',"a",encoding="utf-8") as f:
                        f.write(str(re) + '\n')
                count += 1

print("总表数量：",len(table_name),"重复数量：",count)
print("时间："+time.strftime('%Y.%m.%d %H:%M:%S',time.localtime(time.time())))
with open("./" + city + '.txt', "a", encoding="utf-8") as f:
    f.write("\n\n"+"时间："+str(time.strftime('%Y.%m.%d %H:%M:%S',time.localtime(time.time()))) + '\n')
cur.close()
con.close()