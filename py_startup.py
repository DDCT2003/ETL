# this file is a kind of python startup module used for manual unit testing

#from util.db_connection import Db_Connection
from extract.ext_countries import extraer_countries
from extract.ext_dates import extraer_dates
from extract.ext_stores import extraer_stores
from extract.ext_cities import extraer_cities
from extract.ext_address import extraer_address
from extract.ext_inventories import extraer_inventories
from extract.ext_films import extraer_films
from extract.per_staging import persistir_staging
from transform.tra_stores import trasnformar_stores
from transform.tra_films import trasnformar_films
from load.load_stores import cargar_stores
from load.load_films import cargar_films
from load.load_dates import cargar_dates


import traceback
import pandas as pd


try:
   # con_db = Db_Connection('mysql','localhost','3306','root','Root_12345','oltp')
    #ses_db = con_db.start()
    #if ses_db == -1:
    #    raise Exception("El tipo de base de datos dado no es válido")
    #elif ses_db == -2:
     #   raise Exception("Error tratando de conectarse a la base de datos ")
    
    #databases = pd.read_sql ('show databases', ses_db)
    #print (databases)
    print("Extrayendo Countries de un  cvs")
    countries= extraer_countries()
    #print(countries)

    print("Persistiendo en Staging de countries")
    persistir_staging(countries,'ext_country')

    print("Extrayendo Dates de un  cvs")
    dates= extraer_dates()
    #print(dates)

    print("Persistiendo en Staging de dates")
    persistir_staging(dates,'ext_dates')
   
    print("Extrayendo Stores de un db")
    stores= extraer_stores()
    #print(stores)

    print("Persistiendo en Staging de stores")
    persistir_staging(stores,'ext_store')

    print("Extrayendo cities de un db")
    cities= extraer_cities()
    #print(cities)

    print("Persistiendo en Staging de cities")
    persistir_staging(cities,'ext_city')

    print("Extrayendo addresses de un db")
    addresses= extraer_address()
    #print(addresses)

    print("Persistiendo en Staging de addresses")
    persistir_staging(addresses,'ext_address')

    print("Extrayendo films de un db")
    films= extraer_films()
    #print(addresses)

    print("Persistiendo en Staging de films")
    persistir_staging(films,'ext_film')

    print("Extrayendo inventories de un db")
    inventories= extraer_inventories()
    #print(inventories)

    print("Persistiendo en Staging de inventories")
    persistir_staging(inventories,'ext_inventory')

    print("Transformando datos de store en el staging")
    tra_store= trasnformar_stores()
    #print(tra_store)

    print("Persistiendo datos en staging datos transformados")
    persistir_staging(tra_store,'tra_store')

    print("Cargando datos de stores en sor")
    cargar_stores()

    print("Transformando datos de film en el staging")
    tra_films= trasnformar_films()

    print("Persistiendo datos en staging datos transformados")
    persistir_staging(tra_films,'tra_film')
    print("Cargando datos de films en sor")
    cargar_films()

    print("Cargando datos de dates en sor")
    cargar_dates()



except:
    traceback.print_exc()
finally:
    None
#    ses_db_oltp = con_db.stop()