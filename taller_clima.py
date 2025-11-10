import requests
import pandas as pd

# --- 1 CONFIGURACIÓN DE LA API ---
API_KEY = '7f1a9594919a067e5e0558e8c07d97a6' 
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITIES = ['Bogota', 'Medellin', 'Cali'] 
UNITS = 'metric'  # grados Celsius
LANG = 'es'

# --- 2 INICIO DEL PROCESO ---
print("--- Iniciando extracción de datos del clima ---")

all_weather_data = []

for city in CITIES:
    params = {
        'q': city,
        'appid': API_KEY,
        'units': UNITS,
        'lang': LANG
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        weather = {
            'ciudad': data.get('name'),
            'temperatura': data['main']['temp'],
            'humedad': data['main']['humidity'],
            'descripcion': data['weather'][0]['description']
        }
        all_weather_data.append(weather)
        print(f" Datos obtenidos para {city}")
    except Exception as e:
        print(f" Error al obtener datos de {city}: {e}")

# --- 3 CREAR DATAFRAME ---
df = pd.DataFrame(all_weather_data)
print("\n--- Vista previa del DataFrame ---")
print(df)

print("\nProceso completado correctamente ")
# --- FASE 4: Procesamiento y Análisis de Datos con Pandas ---
print("\n--- Iniciando Fase 4: Procesamiento y Análisis de Datos ---")

# 1️ Verificar valores nulos
print("\nConteo de valores nulos por columna:")
print(df.isnull().sum())

# 2️ Eliminar filas con datos importantes faltantes
df.dropna(subset=['temperatura', 'humedad', 'descripcion'], inplace=True)

# 3️ Convertir tipos de datos si es necesario
df['temperatura'] = pd.to_numeric(df['temperatura'], errors='coerce')
df['humedad'] = pd.to_numeric(df['humedad'], errors='coerce')

# 4️ Crear una nueva columna categorizando la temperatura
def clasificar_temp(temp):
    if temp < 10:
        return 'Frío Extremo'
    elif temp < 18:
        return 'Frío'
    elif temp < 25:
        return 'Templado'
    elif temp < 30:
        return 'Cálido'
    else:
        return 'Calor Extremo'

df['categoria_temp'] = df['temperatura'].apply(clasificar_temp)

# 5️ Mostrar resumen de estadísticas
print("\n--- Resumen estadístico ---")
print(df.describe())

# 6️ Mostrar el DataFrame procesado
print("\n--- DataFrame después del procesamiento ---")
print(df)
# --- FASE 5: Visualización de Datos ---
import matplotlib.pyplot as plt
import seaborn as sns

print("\n--- Iniciando Fase 5: Visualización de Datos ---")

# 1️ Gráfico de barras: temperatura por ciudad
plt.figure(figsize=(8,5))
sns.barplot(x='ciudad', y='temperatura', data=df, palette='coolwarm')
plt.title('Temperatura actual por ciudad')
plt.ylabel('Temperatura (°C)')
plt.xlabel('Ciudad')
plt.tight_layout()
plt.show()

# 2️ Gráfico de barras: humedad por ciudad
plt.figure(figsize=(8,5))
sns.barplot(x='ciudad', y='humedad', data=df, palette='Blues')
plt.title('Humedad actual por ciudad')
plt.ylabel('Humedad (%)')
plt.xlabel('Ciudad')
plt.tight_layout()
plt.show()

# 3️ Gráfico de pastel: clasificación de temperaturas
plt.figure(figsize=(6,6))
df['categoria_temp'].value_counts().plot.pie(autopct='%1.1f%%', startangle=90)
plt.title('Distribución de categorías de temperatura')
plt.ylabel('')
plt.show()
# --- FASE 6: Almacenamiento en MongoDB ---
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, PyMongoError
from datetime import datetime

print("\n--- Iniciando Fase 6: Almacenamiento en MongoDB ---")

MONGO_CONNECTION_STRING ="mongodb+srv://DanielaUser:1007296328@cluster0.c0vmuhk.mongodb.net/?appName=Cluster0"

try:
    print("Conectando a MongoDB Atlas...")
    client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("Conexión exitosa a MongoDB.")

    # Crear base de datos y colección
    db = client["clima_db"]
    coleccion = db["datos_meteorologicos"]

    # Convertir el DataFrame en una lista de diccionarios
    datos_para_mongo = df.to_dict("records")

    # Agregar fecha/hora de registro a cada documento
    for d in datos_para_mongo:
        d["fecha_registro"] = datetime.now()

    print(f"\nBorrando documentos anteriores en la colección '{coleccion.name}'...")
    coleccion.delete_many({})
    print(f"Insertando {len(datos_para_mongo)} documentos nuevos...")
    coleccion.insert_many(datos_para_mongo)
    print("Datos insertados correctamente en MongoDB Atlas.")

    # Verificar los documentos insertados
    print("\n--- Verificando los datos guardados ---")
    for doc in coleccion.find({}, {"_id": 0, "ciudad": 1, "temperatura": 1, "fecha_registro": 1}):
        print(doc)

    client.close()
    print("\nConexión cerrada correctamente con MongoDB.")

except ServerSelectionTimeoutError:
    print("\n No se pudo conectar a MongoDB (verifica tu cadena o conexión a Internet).")
except PyMongoError as e:
    print(f"\n Error de PyMongo: {e}")
except Exception as e:
    print(f"\n Error inesperado: {e}")



