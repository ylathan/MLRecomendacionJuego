'''Se trabajaran las funciones que iran se cominicaran con la API'''

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import pandas as pd

app = FastAPI(title = 'Sistema de Recomendacion de Videojuegos (MLOps)',
            description='API para realizar consultas',
            version=' Nathaly Castro (2024)')

df_developer = pd.read_parquet('Dataset/ETL_games_clean')

@app.get('/', tags=['inicio'])
async def inicio():
    cuerpo = '<center><h1 style="background-color:#541EF7;">Proyecto Individual Numero 1:<br>Sistema de Recomendacion de Videojuegos (MLOps)</h1></center>'
    return HTMLResponse(cuerpo)

@app.get('/developer/{desarrollador}')
async def developer(desarrollador: str):
    '''Con esta primer funcion le daremos solucion al primer Endpoint que nos pide
    que tengamos la cantidad de contenido free por año, y el porcentaje de esa cantidad'''
    #Filtramos segun el desarrollador que se ingrese en la API
    data_dev = df_developer[df_developer['developer'] == desarrollador]

    #Se agrupa la cantidad de items por año
    items_per_year = data_dev.groupby('release_date')['item_id'].count().to_dict()

    # Calculamos la cantidad de contenido free por año y convertimos a un diccionario
    free_count = data_dev[data_dev['price'] == 0.0].groupby('release_date')['item_id'].count().fillna(0).to_dict()

    # Calculamos el porcentaje free
    free_percentage = {year: (free_count.get(year, 0) /
                               items_per_year.get(year, 1)) * 100 for year in items_per_year}

    developer_dict = {}
    for year in items_per_year.keys():
        year_int = int(year)
        developer_dict[year_int] = {
            'Cantidad de Items': items_per_year.get(year, 0),
            '% Contenido Free': free_count.get(year, 0),
            'Porcentaje Free': free_percentage.get(year, 0)
        }
    return developer_dict
