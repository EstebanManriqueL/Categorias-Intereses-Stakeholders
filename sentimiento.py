import csv
import pandas as pd
import numpy as np
from google.colab import auth
import gspread
from oauth2client.client import GoogleCredentials
import gspread_formatting as gsf 
import sys
import time
import progressbar
import gc
gc.enable()
import os
import json
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, SentimentOptions

#Autentificacion de usuario y selección de archivo/sheet
def seleccion_Archivo(id_archivo):
  auth.authenticate_user()
  gc = gspread.authorize(GoogleCredentials.get_application_default())
  global documento
  documento = gc.open_by_key(id_archivo) #https://docs.google.com/spreadsheets/d/1buYWCl7Zq9J-098sDKgU1w_g3KN26SLNPmmDwn0lt_k/edit#gid=0, se ponde id del spreadsheet con el cual trabajar

#Creacion de pestanas a partir de Excel con datos pre-establecidos
def creacion_Pestañas():
  pestana_pestanas = documento.worksheet("PestanasACrear")
  global arreglo_pestanas
  arreglo_pestanas = pestana_pestanas.col_values(1)
  for pestana in arreglo_pestanas:
    if str(pestana) == "Nombre_Pestana":
      continue
    else:
      documento.add_worksheet(title = str(pestana), rows = "1000", cols = "150")

#Agregar pestana que no este pre-establecida en Excel
def añadir_Pestaña(nombre_pestana):
    documento.add_worksheet(title = nombre_pestana, rows = "1000", cols = "150")

#Lectura de grupos de stakeholders, incluido grupos a aplicar en Filtro de Exclusion
def lectura_Stakeholders(path_archivo):
  with open(path_archivo, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ",")
    global stakeholders
    stakeholders = []
    stakeholders_number = 0
    for row in csv_reader:
      stakeholders.append(row)
    
    stakeholders[0][0] = stakeholders[0][0][1:]
    stakeholders_number = len(stakeholders[0])

    global stakeholders_dictonary
    stakeholders_dictonary = []

    df = pd.read_csv(path_archivo, names=stakeholders[0])
    counter = 0
    row_size = 0
    while counter < stakeholders_number:
      column = df[stakeholders[0][counter]]
      stakeholders_dictonary.append(column.array)
      counter += 1
      row_size = len(column.array)

    counter = 0
    while counter < stakeholders_number:
      stakeholder_counter = 0
      while stakeholder_counter < row_size:
        if type(stakeholders_dictonary[counter][stakeholder_counter]) != float:
          print(stakeholders_dictonary[counter][stakeholder_counter])
        stakeholder_counter+= 1
      counter+= 1

#Definicion de columnas para aplicar filtros
def definicion_Columnas_Filtro(path_archivo):
  csv.field_size_limit(sys.maxsize)
  with open(path_archivo, "r", errors='ignore') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ",")
    global columns_data
    columns_data = []
    columns_number_data = 0
    for row in csv_reader:
      columns_data.append(row)
      if(len(columns_data) == 1):
        break
    no_vacios = 0
    for cabezera in columns_data[0]:
      if len(cabezera) > 0:
        no_vacios += 1
    columns_data = columns_data[0][0:no_vacios]

#Definicion de parametros y configuracion para uso de Watson Sentiment
def configuracionWatson():
  authenticator = IAMAuthenticator('0Xenkpw7rEhbHWIOU-PGQZ0U8EpWbuGocVztjobjyjlG')
  global natural_language_understanding
  natural_language_understanding = NaturalLanguageUnderstandingV1(
    version='2021-08-01',
    authenticator=authenticator
  )
  natural_language_understanding.set_service_url('https://api.us-south.natural-language-understanding.watson.cloud.ibm.com/instances/4fecbee7-cf18-45f6-a388-d156027c9df7')

#Escribe en el documento con las inetracciones de Brandwatch, sNPS y sentimiento de acuerdo a Watson
def inclusionSentimientoCSV(path_archivo):
    df = pd.read_csv(path_archivo, names=columns_data, encoding='latin1', usecols=columns_data)
    watsonScore = []
    watsonSentiment = []
    sNPS = []
    tweets = df["Full text"]
    for tweet in tweets:
        if tweet == tweets[0]:
            watsonScore.append("Watson Score")
            watsonSentiment.append("Watson Sentiment")
            sNPS.append("sNPS")
        else:
            texto = str(tweet)
            response = natural_language_understanding.analyze(
                text = texto,
                language = "Spanish",
                features=Features(sentiment=SentimentOptions(document=True))).get_result()
            watsonScore.append(float(response["sentiment"]["document"]["score"]))
            
            if float(response["sentiment"]["document"]["score"]) > 0:
                watsonSentiment.append("Positive")
            elif float(response["sentiment"]["document"]["score"]) < 0:
                watsonSentiment.append("Negative")
            else:
                watsonSentiment.append("Neutral")

            if float(response["sentiment"]["document"]["score"]) > 0.4:
                sNPS.append(10)
            elif float(response["sentiment"]["document"]["score"]) < 0:
                sNPS.append(0)
            else:
                sNPS.append(7)
    
    df["Watson Score"] = watsonScore
    df["Watson Sentiment"] = watsonSentiment
    df["sNPS"] = sNPS
    
    with open(path_archivo, "w", encoding="latin1") as f:
        df.to_csv(f, index=False, header=False)

#Calculo de social NPS           
def calculoSNPS(nombre_archivo, nombre_pestana, categoria, country, profession, fecha_inicio, fecha_fin, mencionados):
    df = pd.read_csv(nombre_archivo, names=columns_data, encoding='latin1', usecols=columns_data)
    country_name = "Country Code" #Columna de codigos de paises para filtro
    profession_name = "Professions" #Columna de profesiones para filtro
    mentioned_name = "Mentioned Authors"
    author_name = "Author"

    actores = []
    

    indiceCategoria = stakeholders[0].index(categoria)
    for stakeholder in stakeholders_dictonary[indiceCategoria]:
        if type(stakeholder) != float and stakeholder != stakeholders_dictonary[indiceCategoria][0]:
            if mencionados == "YES" and stakeholder[0] == "@":
                actores.append(stakeholder)
            elif mencionados == "NO" and stakeholder[0] != "@":
                actores.append(stakeholder)
            else:
                pass
    
    if fecha_inicio != "-" and fecha_fin != "-":
        df = (df.loc[(df["Date"] >= fecha_inicio) & (df["Date"] <= fecha_fin)])
    
    if profession == "ALL":
        if country == "ALL":
            pass
        else:
            df = df.loc[df[country_name] == country]
        if mencionados == "YES":
        elif
    else:
        if country == "ALL":
            pass
        else:
            df = df.loc[df[country_name] == country]
        df = df.loc[df[profession_name].str.contains(profession, regex=False, na=False, case=False)]

    añadir_Pestaña(nombre_pestana)
    pestana = documento.worksheet(nombre_pestana)
    time_sleep = 0.25

    pestana.update("A1", [["Calculo sNPS"], ["Pais", country], ["Categoria analizada", categoria], ["Total Filas analizadas"], ["Fecha inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])
    time.sleep(time_sleep)

