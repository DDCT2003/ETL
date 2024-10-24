
import traceback
import pandas as pd
from util.db_connection import Db_Connection

def trasnformar_films ():

    try:
        type='mysql'
        host= 'localhost'
        port='3306'
        user='root'
        pwd='Root_12345'
        db='staging'

        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt="SELECT \
                     film_id,\
                     title,\
                     release_year,\
                     language_id,\
                     rating,\
                     CASE \
                     WHEN length < 60 THEN '< 1h'\
                     WHEN length < 90 THEN '< 1.5h'\
                     WHEN length < 120 THEN '< 2h'\
                     ELSE '> 2h'\
                     END AS length\
                     FROM staging.ext_film;"
        films_tra=pd.read_sql(sql_stmt, ses_db)
    
        
        return films_tra
    except:
        traceback.print_exc()
    finally:
        pass