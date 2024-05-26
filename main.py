'''Se trabajaran las funciones que iran se cominicaran con la API'''

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import pandas as pd

app = FastAPI(title = 'Sistema de Recomendacion de Videojuegos (MLOps)',
            description='API para realizar consultas',
            version=' Nathaly Castro (2024)')

df_developer = pd.read_parquet('.\\Dataset\\ETL_games_clean')

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

df_user_data =pd.read_parquet ('.\\Dataset\\user_data_price.parquet')
df_reviews =pd.read_parquet('.\\Dataset\\user_reviews_clean.parquet')
@app.get('/userdata/{User_id}')
async def  userdata(User_id : str):
    '''Con el desarrollo de esta función se busca devolver la cantidad de dinero 
    gastado por el usuario, el porcentaje de recomendación y la cantidad de items'''
    user = None  # Inicializar con un valor por defecto
    # Verificar si el user_id es un entero
    if isinstance(User_id, int): User_id = str(User_id)

    # Buscar el usuario en el DataFrame
    #if str(User_id) in df_reviews['user_id'].astype(str).values: user = df_reviews[df_reviews['user_id'] == User_id]
    if User_id in df_reviews['user_id'].values: user = df_reviews[df_reviews['user_id'] == User_id]

    # Validar si el usuario ha sido encontrado
    if user is None or user.empty:
        return {'message': 'No se encontraron datos para el usuario {}'.format(User_id)}
         
    # Se comienza filtrando por el usuario que se ingresa en la API
    user = df_reviews[df_reviews['user_id'] == User_id]
    # Se hace el calculo de la cantidad de dinero gastado para ese usuario
    price_count = df_user_data[df_user_data['user_id']== User_id]['price'].iloc[0]
    # Se obtiene la cantidad de items para ese usuario   
    items_count = df_user_data[df_user_data['user_id']== User_id]['items_count'].iloc[0]
    # Se busca el total de recomendaciones por ese usuario
    user_recom_count = user['reviews_recommend'].sum()
    # Se calcula el total de reviews de todos los usuarios
    reviews_total = len(df_reviews['user_id'].unique())
    # Ahora se puede calcular el porcentaje de ese usuario
    user_percentage = (user_recom_count / reviews_total) * 100

    user_data_dic = {
        'cantidad_dinero': int(price_count),
        'porcentaje_recomendacion': round(float(user_percentage), 2),
        'total_items': int(items_count)
    }

    return user_data_dic

df_user_genre = pd.read_parquet('.\\Dataset\\user_for_genre.parquet')
@app.get('/UserForGenre/{genero}')
async def UserForGenre(genero:str):
    # Se filtra para este genero 
    one_genre_data = df_user_genre[df_user_genre['genres'] == genero]
    # Se calcula el usuario con más horas
    top_user_max = one_genre_data.groupby(['user_id'])['playtime_hrs'].sum().idxmax()
    
    # Crear una lista de diccionarios con años y horas jugadas para cada año
    horas_por_año = []

    available_years = one_genre_data.loc[one_genre_data['user_id'] == top_user_max, 'release_date'].unique()

    for year in available_years:
        horas_año = one_genre_data[(one_genre_data['user_id'] == top_user_max) & (one_genre_data['release_date'] == year)]['playtime_hrs'].sum()
        if horas_año > 0:
            horas_por_año.append({'Año': int(year), 'Horas': int(horas_año)})

    # Construir el diccionario final
    user_gnr_result = {
        f"Usuario con más horas jugadas para Género {genero}": top_user_max,
        "Horas jugadas": horas_por_año
    }
    return user_gnr_result

df_best_developer_year = pd.read_parquet('.\\Dataset\\best_developer_year.parquet')
@app.get('/best_developer_year/{anio}')
async def best_developer_year(anio: int): 

    # Se filtra el DataFrame para el año dado
    year_data = df_best_developer_year[(df_best_developer_year['release_date'] == anio) & (df_best_developer_year['sentiment_analysis'] == 2)]
    # Se obtienen los 3 principales desarrolladores con más juegos recomendados por usuarios
    top_developers = year_data.groupby('developer').size().nlargest(3)    
    # Se construye el diccionario con el resultado
    best_dev_result = {
    "Puesto 1": top_developers.index[0],
    "Puesto 2": top_developers.index[1],
    "Puesto 3": top_developers.index[2]
    }
    return best_dev_result