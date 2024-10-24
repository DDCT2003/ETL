import traceback
import pandas as pd

def extraer_dates ():

    try:
        filename = './CSVS/dates.csv'
        dates = pd.read_csv(filename)
        return dates

    except:
        traceback.print_exc()
    finally:
        pass
