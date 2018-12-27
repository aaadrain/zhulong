import os
import traceback
import logging
import psycopg2
import sys

list = os.listdir()
list.remove(os.path.basename(__file__))
if "profile" in list:
    list.remove("profile")
if "task.py" in list:
    list.remove("task.py")
if "__init__.py" in list:
    list.remove("__init__.py")
if ".idea" in list:
    list.remove(".idea")




upname =os.getcwd().split("\\")[-1]

log_filename = upname + ".log"
logging.basicConfig(filename=log_filename,filemode="a",level=logging.DEBUG)

# 全部爬之前删gg.
for l in list:
    name = l.split('.')[0]
    conp = ["postgres", "since2015", "192.168.3.171", "%s"%upname, "%s"%name]
    sql = """drop table if exists %s.gg"""%name
    con = psycopg2.connect(user=conp[0], password=conp[1], host=conp[2], port="5432", database=conp[3])
    cur = con.cursor()
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()


arg = sys.argv
if len(arg) >2:
    for l in list:
        try:
            os.system("python ./%s %d %d %d"%(l,int(arg[1]),int(arg[2]),int(arg[3])))
        except Exception as e:
            traceback.print_exc(e)
            logging.debug(e)

    for l in list:
        try:
            os.system("python ./%s %d %d %d"%(l,int(arg[1]),int(arg[2]),int(arg[3])))
        except Exception as e:
            logging.debug(e)
            traceback.print_exc(e)

elif len(arg) == 2:
    for l in list:
        try:
            os.system("python ./%s %d"%(l,int(arg[1])))
        except Exception as e:
            logging.debug(e)
            traceback.print_exc(e)
    for l in list:
        try:
            os.system("python ./%s %d"%(l,int(arg[1])))
        except Exception as e:
            logging.debug(e)
            traceback.print_exc(e)

else:
    for l in list:
        try:
            os.system("python ./%s"%l)
        except Exception as e:
            traceback.print_exc(e)
            logging.debug(e)

    for l in list:
        try:
            os.system("python ./%s"%l)
        except Exception as e:
            traceback.print_exc(e)
            logging.debug(e)
