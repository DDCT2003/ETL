
import traceback
import pandas as pd
from util.db_connection import Db_Connection

def cargar_dates ():

    try:
        type='mysql'
        host= 'localhost'
        port='3306'
        user='root'
        pwd='Root_12345'
        db='staging'

        con_sta_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt="SELECT date_id, date, month, year\
                From ext_dates"
        dates_tra=pd.read_sql(sql_stmt, ses_sta_db)
    
       
        type='mysql'
        host= 'localhost'
        port='3306'
        user='root'
        pwd='Root_12345'
        db='sor'

        con_sor_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")

        dim_date_dict ={
            "date_bk": [],
            "date": [],
            "month": [],
            "year": [],
        }
    
        if not dates_tra.empty:
            for bk,dat,mon,yea \
                in zip(dates_tra['date_id'], dates_tra['date'], dates_tra['month'], dates_tra['year']):
                dim_date_dict['date_bk'].append(bk)
                dim_date_dict['date'].append(dat)
                dim_date_dict['month'].append(mon)
                dim_date_dict['year'].append(yea)
        
        if dim_date_dict['date_bk']:
            df_dim_date = pd.DataFrame(dim_date_dict)
            df_dim_date.to_sql('dim_date',ses_sor_db, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass