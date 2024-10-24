
import traceback
import pandas as pd
from util.db_connection import Db_Connection

def extraer_address ():

    try:
        type='mysql'
        host= 'localhost'
        port='3306'
        user='root'
        pwd='Root_12345'
        db='oltp'

        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
    
        addresses = pd.read_sql ('SELECT \
                address_id, \
                address, \
                address2, \
                district, \
                city_id, \
                postal_code, \
                phone, \
                ST_AsText(location) AS location, \
                last_update \
            FROM address', ses_db)
        return addresses
    except:
        traceback.print_exc()
    finally:
        pass