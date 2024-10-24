
import traceback
import pandas as pd
from util.db_connection import Db_Connection

def cargar_films ():

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
        
        sql_stmt="SELECT film_id, title, release_year, language_id, rating, length\
                From tra_film"
        films_tra=pd.read_sql(sql_stmt, ses_sta_db)
    
       
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

        dim_film_dict ={
            "film_bk": [],
            "title": [],
            "release_year": [],
            "language_id": [],
            "rating": [],
            "length": [],
        }
    
        if not films_tra.empty:
            for bk,tit,yea,lan,rat,len \
                in zip(films_tra['film_id'], films_tra['title'], films_tra['release_year'], films_tra['language_id'], films_tra['rating'], films_tra['length']):
                dim_film_dict['film_bk'].append(bk)
                dim_film_dict['title'].append(tit)
                dim_film_dict['release_year'].append(yea)
                dim_film_dict['language_id'].append(lan)
                dim_film_dict['rating'].append(rat)
                dim_film_dict['length'].append(len)

        if dim_film_dict['film_bk']:
            df_dim_film_ = pd.DataFrame(dim_film_dict)
            df_dim_film_.to_sql('dim_film',ses_sor_db, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass