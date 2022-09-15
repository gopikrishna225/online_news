import cx_Oracle
import pandas as pd
import datetime
from datetime import timedelta
from fastapi import FastAPI
import uvicorn

def news_paper_api():
    ys_date = datetime.date.today()-timedelta(days=2)
    con = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
    data = pd.read_sql("SELECT * FROM DXP_NEWSPAPER_ANALYSIS WHERE PUBLISHED_DATE = '{}' ".format(ys_date),con)
    return data.to_json(orient='records')

# app = FastAPI()

# @app.get("/NEWS_DATA")
# @app.post("/NEWS_DATA/")
# async def rpa_validation():
#     resp =news_paper_api()
#     return resp

# if __name__ == '__main__':
#     uvicorn.run(app)
