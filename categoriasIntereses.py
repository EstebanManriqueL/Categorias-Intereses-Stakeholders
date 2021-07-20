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
from sentiment_analysis_spanish import sentiment_analysis
import os

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

#Impresion y obtencion de categorias a analizar
def definicion_Categorias(path_archivo):
  with open(path_archivo, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ",")
    global columns
    columns = []
    columns_number = 0
    for row in csv_reader:
      columns.append(row)
    columns_number = len(columns[0])
    print(columns[0])
  
  global column_dictonary
  column_dictonary = []

  df = pd.read_csv(path_archivo, names=columns[0])
  counter = 0
  row_size = 0
  while counter < columns_number:
    column = df[columns[0][counter]]
    column_dictonary.append(column.array)
    counter += 1
    row_size = len(column.array)

  counter = 0
  while counter < columns_number:
    category_counter = 0
    while category_counter < row_size:
      if type(column_dictonary[counter][category_counter]) != float:
        print(column_dictonary[counter][category_counter])
      category_counter+= 1
    counter+= 1
    print("")

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
    print(columns_data)

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
    print(stakeholders[0])

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
      print("")

#Filtro por token/hashtag y por género, con sentimiento
def aplicacion_filtros_demograficos(nombre_archivo, nombre_pestana, country, profession, fecha_inicio, fecha_fin):
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(nombre_archivo, names=columns_data, encoding='latin1', usecols=columns_data)
  country_name = "Country Code" #Columna de codigos de paises para filtro
  gender_name = "Gender" #Columna de codigos de genero para filtro
  profession_name = "Professions" #Columna de profesiones para filtro

  gender = "unknown"
  male = "male"
  female = "female"

  añadir_Pestaña(nombre_pestana)
  pestana = documento.worksheet(nombre_pestana) #Nombre de pestana en donde se va a escribir info
  time_sleep = 0.25

  pestana.update("A1", [["Pais", country], ["Profesion", profession]])
  time.sleep(time_sleep)

  if fecha_inicio != "-" and fecha_fin != "-":
      df = (df.loc[(df["Date"] >= fecha_inicio) & (df["Date"] <= fecha_fin)])

    #Para cualquier profesion
  if profession == "ALL":
    if country == "ALL":
      country_gender = df
    else:
      country_gender = df.loc[df[country_name] == country]
    filter_gender = country_gender.loc[df[gender_name] == gender]
    print(len(filter_gender))
    filter_ALL_genders = [country_gender.loc[df[gender_name] == gender], country_gender.loc[df[gender_name] == male], country_gender.loc[df[gender_name] == female]]
    total_rows = filter_ALL_genders[1].count() + filter_ALL_genders[2].count() + filter_ALL_genders[0].count()
    men_rows = filter_ALL_genders[1].count()
    women_rows = filter_ALL_genders[2].count()
    unknown_rows = filter_ALL_genders[0].count()
  else:
    #Para profesion en particular
    if country == "ALL":
      country_gender = df
    else:
      country_gender = df.loc[df[country_name] == country]
    profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
    filter_gender = profession_gender.loc[df[gender_name] == gender]
    filter_ALL_genders = [profession_gender.loc[df[gender_name] == gender], profession_gender.loc[df[gender_name] == male], profession_gender.loc[df[gender_name] == female]]
    total_rows = profession_gender.loc[df[gender_name] == female].count() + profession_gender.loc[df[gender_name] == male].count()  + profession_gender.loc[df[gender_name] == gender].count()
    men_rows = profession_gender.loc[df[gender_name] == male].count()
    women_rows = profession_gender.loc[df[gender_name] == female].count()
    #TODO agregar unknown

  pestana.update("A3", [["Hombres", int(men_rows["Full Text"])], ["Mujeres", int(women_rows["Full Text"])], ["No especificado", int(unknown_rows["Full Text"])], ["Total Filas", int(total_rows["Full Text"])]])
  time.sleep(time_sleep)
  pestana.update("B7", [["Hombres", "Mujeres", "No especificado", "% Hombres", "% Mujeres", "% No especificado", "Sentimiento Hombres", "Sentimiento Mujeres", "Sentimiento No especificados", "Total", "%Total"]])
  time.sleep(time_sleep)
  pestana.update("D1", [["Fecha Inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])

  origin_cell = "A8"
  cell_number = 8
  origin_cell_men = "B9"
  origin_cell_women = "C9"
  origin_cell_unknown = "D9"

  #Formato de procentaje para celda de Excel
  cell_decimal_format = gsf.cellFormat(
    numberFormat = gsf.numberFormat("NUMBER", pattern = "##.###%") 
  ) 

  barra_progreso = progressbar.ProgressBar(max_value = len(column_dictonary))
  progreso = 0

  bandera = 0
  for column in column_dictonary:
    progreso += 1
    barra_progreso.update(progreso)
    for word in column:
      time.sleep(time_sleep)
      if type(word) != float:
        men_counts = filter_ALL_genders[1]["Full Text"].str.contains(word, case=False).value_counts()
        female_counts = filter_ALL_genders[2]["Full Text"].str.contains(word, case=False).value_counts()
        unknown_counts = filter_ALL_genders[0]["Full Text"].str.contains(word, case=False).value_counts()
        try:
          men_counts = men_counts.iloc[1]
        except:
          men_counts = 0
        try:
          female_counts = female_counts.iloc[1]
        except:
          female_counts = 0
        try:
          unknown_counts = unknown_counts.iloc[1]
        except:
          unknown_counts = 0
        
        #Calculo de sentimiento por palabra/token para hombres, mujeres y desconocidos
        conteos = [unknown_counts, men_counts, female_counts]
        acumulado_sentimiento = []
        index_sentiments = 0
        for gender in filter_ALL_genders:
          sentimiento = 0
          for tweet in gender["Full Text"]:
            str_twwt = str(tweet).lower()
            if word.lower() in str_twwt:
              sentimiento += sentiment.sentiment(str_twwt)
          if conteos[index_sentiments] > 0:
            sentimiento = sentimiento / conteos[index_sentiments]
            if sentimiento > 1:
              sentimiento = 1
          else:
             sentimiento = "-"
          acumulado_sentimiento.append(sentimiento)
          index_sentiments +=1
        
        if word in columns[0]:
          pestana.update(origin_cell, [[" ", " ", " ", " ", " ", " ", " ", " "]])
          cell_number += 1
          origin_cell = "A" + str(cell_number)
          origin_cell_men = "B" + str(cell_number)
          origin_cell_women = "C" + str(cell_number)
          origin_cell_unknown = "D" + str(cell_number)
          origin_cell_total = "K" + str(cell_number)

          gsf.format_cell_range(pestana, "E:G", cell_decimal_format)
          gsf.format_cell_range(pestana, "L:M", cell_decimal_format)
          time.sleep(time_sleep)

          pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$3' % origin_cell_men), ('=%s/$B$4' % origin_cell_women), ('=%s/$B$5' % origin_cell_unknown), (acumulado_sentimiento[1]), (acumulado_sentimiento[2]), (acumulado_sentimiento[0]), int(men_counts+female_counts+unknown_counts), ('=%s/$B$6' % origin_cell_total)]], value_input_option='USER_ENTERED')
          time.sleep(time_sleep)  
        else:
          if bandera == 0:
            pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$3' % ("B" + str(cell_number))), ('=%s/$B$4' % ("C" + str(cell_number))), ('=%s/$B$5' % ("D" + str(cell_number))), (acumulado_sentimiento[1]), (acumulado_sentimiento[2]), (acumulado_sentimiento[0]), int(men_counts+female_counts+unknown_counts), ('=%s/$B$6' % ("K" + str(cell_number)))]], value_input_option='USER_ENTERED')  
            bandera = 1
          else:
            gsf.format_cell_range(pestana, "E:G", cell_decimal_format)
            gsf.format_cell_range(pestana, "L:M", cell_decimal_format)
            time.sleep(time_sleep)
            pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$3' % origin_cell_men), ('=%s/$B$4' % origin_cell_women), ('=%s/$B$5' % origin_cell_unknown), (acumulado_sentimiento[1]), (acumulado_sentimiento[2]), (acumulado_sentimiento[0]), int(men_counts+female_counts+unknown_counts), ('=%s/$B$6' % origin_cell_total)]], value_input_option='USER_ENTERED')
            time.sleep(time_sleep)
    
        cell_number += 1
        origin_cell = "A" + str(cell_number)
        origin_cell_men = "B" + str(cell_number)
        origin_cell_women = "C" + str(cell_number)
        origin_cell_unknown = "D" + str(cell_number)
        origin_cell_total = "K" + str(cell_number)
  del df

#Filtro por categoria de Intereses, clasificado por genero y con sentimiento incluido
def aplicacion_Filtro_Demograficos_Condensado(nombre_archivo, nombre_pestana, country, profession, fecha_inicio, fecha_fin):
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(nombre_archivo, names=columns_data, encoding='latin1', usecols=columns_data)
  country_name = "Country Code" #Columna de codigos de paises para filtro
  gender_name = "Gender" #Columna de codigos de genero para filtro
  profession_name = "Professions" #Columna de profesiones para filtro

  gender = "unknown"
  male = "male"
  female = "female"

  añadir_Pestaña(nombre_pestana)
  pestana = documento.worksheet(nombre_pestana) #Nombre de pestana en donde se va a escribir info
  time_sleep = 0.25

  pestana.update("A1", [["Pais", country], ["Profesion", profession]])
  time.sleep(time_sleep)

  if fecha_inicio != "-" and fecha_fin != "-":
      df = (df.loc[(df["Date"] >= fecha_inicio) & (df["Date"] <= fecha_fin)])
  
  if profession == "ALL":
    if country == "ALL":
      country_gender = df
    else:
      country_gender = df.loc[df[country_name] == country]
    filter_gender = country_gender.loc[df[gender_name] == gender]
    print(len(filter_gender))
    filter_ALL_genders = [country_gender.loc[df[gender_name] == gender], country_gender.loc[df[gender_name] == male], country_gender.loc[df[gender_name] == female]]
    total_rows = filter_ALL_genders[1].count() + filter_ALL_genders[2].count() + filter_ALL_genders[0].count()
    men_rows = filter_ALL_genders[1].count()
    women_rows = filter_ALL_genders[2].count()
    unknown_rows = filter_ALL_genders[0].count()
  else:
    #Para profesion en particular
    if country == "ALL":
      country_gender = df
    else:
      country_gender = df.loc[df[country_name] == country]
    profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
    filter_gender = profession_gender.loc[df[gender_name] == gender]
    filter_ALL_genders = [profession_gender.loc[df[gender_name] == gender], profession_gender.loc[df[gender_name] == male], profession_gender.loc[df[gender_name] == female]]
    total_rows = profession_gender.loc[df[gender_name] == female].count() + profession_gender.loc[df[gender_name] == male].count()  + profession_gender.loc[df[gender_name] == gender].count()
    men_rows = profession_gender.loc[df[gender_name] == male].count()
    women_rows = profession_gender.loc[df[gender_name] == female].count()
    #TODO agregar unknown

  pestana.update("A3", [["Hombres", int(men_rows["Full Text"])], ["Mujeres", int(women_rows["Full Text"])], ["No especificado", int(unknown_rows["Full Text"])], ["Total Filas", int(total_rows["Full Text"])]])
  time.sleep(time_sleep)
  pestana.update("B7", [["Hombres", "Mujeres", "No especificado", "% Hombres", "% Mujeres", "% No especificado", "Sentimiento Hombres", "Sentimiento Mujeres", "Sentimiento No especificados", "Total", "%Total"]])
  time.sleep(time_sleep)
  pestana.update("D1", [["Fecha Inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])

  origin_cell = "A8"
  cell_number = 8
  origin_cell_men = "B9"
  origin_cell_women = "C9"
  origin_cell_unknown = "D9"

  #Formato de procentaje para celda de Excel
  cell_decimal_format = gsf.cellFormat(
    numberFormat = gsf.numberFormat("NUMBER", pattern = "##.###%") 
  )

  print(columns[0])
  index = 8
  for categoria in columns[0]:
    pestana.update(("A" + str(index)), [[categoria]])
    time.sleep(time_sleep)
    index += 1 

  barra_progreso = progressbar.ProgressBar(max_value = len(column_dictonary))
  progreso = 0

  index = 8
  for column in column_dictonary:
    progreso += 1
    toWriteCategoria = [0,0,0,0,0,0] # Numero apariciones unknown, hombres, mujeres, sentimiento y  porcentajes en ese orden 
    barra_progreso.update(progreso)
    for word in column:
      if type(word) != float:
        if type(word) != float:
          men_counts = filter_ALL_genders[1]["Full Text"].str.contains(word, case=False).value_counts()
          female_counts = filter_ALL_genders[2]["Full Text"].str.contains(word, case=False).value_counts()
          unknown_counts = filter_ALL_genders[0]["Full Text"].str.contains(word, case=False).value_counts()
          try:
            toWriteCategoria[1] += men_counts.iloc[1]
          except:
            toWriteCategoria[1] += 0
          try:
            toWriteCategoria[2] += female_counts.iloc[1]
          except:
            toWriteCategoria[2] += 0
          try:
            toWriteCategoria[0] += unknown_counts.iloc[1]
          except:
            toWriteCategoria[0] += 0

          index_sentimiento = 0
          for gender in filter_ALL_genders:
            sentimiento = 0
            for tweet in gender["Full Text"]:
              str_twwt = str(tweet).lower()
              if word.lower() in str_twwt:
                sentimiento += sentiment.sentiment(str_twwt)
            if sentimiento > 0:
              toWriteCategoria[(index_sentimiento + 3)] += sentimiento
            index_sentimiento += 1
    
    for sentimiento in [0,1,2]:
      if toWriteCategoria[sentimiento] > 0:
        toWriteCategoria[(sentimiento + 3)] = toWriteCategoria[(sentimiento + 3)] / toWriteCategoria[sentimiento]
      else:
        toWriteCategoria[(sentimiento + 3)] = "-"

    origin_cell_men = str(("B" + str(index)))
    origin_cell_women = str(("C" + str(index)))
    origin_cell_unknown = str(("D" + str(index)))
    origin_cell_total = str(("K" + str(index)))
    pestana.update(str("B"+ str(index)), [[int(toWriteCategoria[1]), int(toWriteCategoria[2]), int(toWriteCategoria[0]), ('=%s/$B$3' % origin_cell_men), ('=%s/$B$4' % origin_cell_women), ('=%s/$B$5' % origin_cell_unknown), toWriteCategoria[4], toWriteCategoria[5], toWriteCategoria[3], int(toWriteCategoria[1] + toWriteCategoria[2] + toWriteCategoria[0]), ('=%s/$B$6' % origin_cell_total)]], value_input_option='USER_ENTERED')
    
    gsf.format_cell_range(pestana, "E:G", cell_decimal_format)
    gsf.format_cell_range(pestana, "L:M", cell_decimal_format)
    time.sleep(time_sleep)
    
    index += 1

#Filtro para cada una de los participantes de una categoria de stakeholders, por cada una de las categorias de token/hashtags
def aplicacion_Filtro_Stakeholders_Condensado(archivo_interacciones, nombre_pestana, country, profession, categoria, columna_analisis, fecha_inicio, fecha_fin):
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(archivo_interacciones, names=columns_data, encoding='latin1', usecols=columns_data)
  stakeholder_name = columna_analisis
  country_name = "Country Code" #Columna de codigos de paises para filtro
  profession_name = "Professions" #Columna de profesiones para filtro
  stakeholders_filtrados = []

  indice_stakeholders = stakeholders[0].index(categoria)
  for stakeholder in stakeholders_dictonary[indice_stakeholders]:
    if type(stakeholder) != float and stakeholder != stakeholders_dictonary[indice_stakeholders][0]:
      if stakeholder_name == "Mentioned Authors":
        stakeholders_filtrados.append(("@" + stakeholder.lower()))
      else:
        stakeholders_filtrados.append(stakeholder)

  print(stakeholders_filtrados)
  añadir_Pestaña(nombre_pestana)
  pestana = documento.worksheet(nombre_pestana) #Nombre de pestana en donde se va a escribir info
  time_sleep = 0.25

  pestana.update("A1", [["Pais", country], ["Profesion", profession], ["Categoria Stakeholders", categoria]])
  time.sleep(time_sleep)

  numero_total_filas = 0
  for stakeholder in stakeholders_filtrados:
    if profession == "ALL":
      if country == "ALL": #En caso de que no se requiera un pais en particular
        country_gender = df
      else: #En caso de requerir un pais en particular
          country_gender = df.loc[df[country_name] == country]
      interacciones_stakeholder = (country_gender.loc[country_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])
    else:
      #Para profesion en particular
      if country == "ALL":
          country_gender = df
      else:
        country_gender = df.loc[df[country_name] == country]
      profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
      interacciones_stakeholder = (profession_gender.loc[profession_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])

    if fecha_inicio != "-" and fecha_fin != "-":
      interacciones_stakeholder = (interacciones_stakeholder.loc[(interacciones_stakeholder["Date"] >= fecha_inicio) & (interacciones_stakeholder["Date"] <= fecha_fin)])
        
    numero_total_filas += len(interacciones_stakeholder) #int(filtrado["Full Text"])

  time.sleep(time_sleep)
  pestana.update("A4", [["Total Filas", numero_total_filas]])
  time.sleep(time_sleep)
  pestana.update("D1", [["Fecha Inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])
  time.sleep(time_sleep)

  index = 7
  columna_pestana_ascii = 66
  segunda_columna_pestana_ascii = 65
  dos_columnas = 0

  for stakeholderr in stakeholders_filtrados:
    if columna_pestana_ascii > 90 and dos_columnas < 1:
      columna_pestana_ascii = 65
      dos_columnas = 1
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    elif columna_pestana_ascii > 90 and dos_columnas > 0:
      segunda_columna_pestana_ascii += 1
      columna_pestana_ascii = 65
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    elif columna_pestana_ascii < 90 and dos_columnas > 0:
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    else:
      columna = chr(columna_pestana_ascii)
    
    pestana.update((columna + str(index)), [[stakeholderr, ("Sentimiento " + stakeholderr)]])
    time.sleep(time_sleep)
    columna_pestana_ascii += 2

  index = 8
  for categoria in columns[0]:
    pestana.update(("A" + str(index)), [[categoria]])
    time.sleep(time_sleep)
    index += 1
  
  barra_progreso = progressbar.ProgressBar(max_value = len(column_dictonary))
  progreso = 0

  index = 8
  for column in column_dictonary:
    progreso += 1
    barra_progreso.update(progreso)
    columna_pestana_ascii = 66
    segunda_columna_pestana_ascii = 65
    dos_columnas = 0
    for stakeholder in stakeholders_filtrados:
      numero_interacciones = 0
      sentimiento = 0
      #Para cualquier profesion
      if profession == "ALL":
        if country == "ALL": #En caso de que no se requiera un pais en particular
          country_gender = df
        else: #En caso de requerir un pais en particular
          country_gender = df.loc[df[country_name] == country]
        interacciones_stakeholder = (country_gender.loc[country_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])
      else:
        #Para profesion en particular
        if country == "ALL":
          country_gender = df
        else:
          country_gender = df.loc[df[country_name] == country]
        profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
        interacciones_stakeholder = (profession_gender.loc[profession_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])

      if fecha_inicio != "-" and fecha_fin != "-":
        interacciones_stakeholder = (interacciones_stakeholder.loc[(interacciones_stakeholder["Date"] >= fecha_inicio) & (interacciones_stakeholder["Date"] <= fecha_fin)])

      for word in column:
        if type(word) != float:
          contador = interacciones_stakeholder["Full Text"].str.contains(word, case=False).value_counts()
          try:
            numero_interacciones += contador.iloc[1]
          except:
            numero_interacciones += 0
          for tweet in interacciones_stakeholder["Full Text"]:
            str_twwt = str(tweet).lower()
            if word.lower() in str_twwt:
              sentimiento += sentiment.sentiment(str_twwt)
      
      if columna_pestana_ascii > 90 and dos_columnas < 1:
        columna_pestana_ascii = 65
        dos_columnas = 1
        columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
        columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
      elif  columna_pestana_ascii > 90 and dos_columnas > 0:
        segunda_columna_pestana_ascii += 1
        columna_pestana_ascii = 65
        columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
        columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
      elif columna_pestana_ascii < 90 and dos_columnas > 0:
        columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
        columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
      else:
        columna = chr(columna_pestana_ascii)
        columna_siguiente = chr(columna_pestana_ascii + 1)

      if int(numero_interacciones) > 0:
        sentimiento = sentimiento / int(numero_interacciones)
        if sentimiento > 1:
              sentimiento = 1
      else:
        sentimiento = "-"
      pestana.update((columna + str(index)), [[int(numero_interacciones), sentimiento]])
      time.sleep(time_sleep)
      columna_pestana_ascii += 2
    index += 1
  del df

def aplicacion_Filtro_Stakeholders_Expandido(archivo_interacciones, nombre_pestana, country, profession, categoria, columna_analisis, fecha_inicio, fecha_fin):
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(archivo_interacciones, names=columns_data, encoding='latin1', usecols=columns_data)
  stakeholder_name = columna_analisis #Columna con nombres de autores de interacciones
  country_name = "Country Code" #Columna de codigos de paises para filtro
  profession_name = "Professions" #Columna de profesiones para filtro
  stakeholders_filtrados = []

  #try:
  indice_stakeholders = stakeholders[0].index(categoria)
  for stakeholder in stakeholders_dictonary[indice_stakeholders]:
    if type(stakeholder) != float and stakeholder != stakeholders_dictonary[indice_stakeholders][0]:
      if stakeholder_name == "Mentioned Authors":
        stakeholders_filtrados.append(("@" + stakeholder.lower()))
      else:
        stakeholders_filtrados.append(stakeholder)
  print(stakeholders_filtrados)
  
  añadir_Pestaña(nombre_pestana)
  pestana = documento.worksheet(nombre_pestana) #Nombre de pestana en donde se va a escribir info
  time_sleep = 0.25

  pestana.update("A1", [["Pais", country], ["Profesion", profession], ["Categoria Stakeholders", categoria]])
  time.sleep(time_sleep)

  numero_total_filas = 0
  for stakeholder in stakeholders_filtrados:
    if profession == "ALL":
      if country == "ALL": #En caso de que no se requiera un pais en particular
        country_gender = df
      else: #En caso de requerir un pais en particular
        country_gender = df.loc[df[country_name] == country]
      interacciones_stakeholder = (country_gender.loc[country_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])
    else:
      #Para profesion en particular
      if country == "ALL":
        country_gender = df
      else:
        country_gender = df.loc[df[country_name] == country]
      profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
      interacciones_stakeholder = (profession_gender.loc[profession_gender[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False)])

    if fecha_inicio != "-" and fecha_fin != "-":
      interacciones_stakeholder = (interacciones_stakeholder.loc[(interacciones_stakeholder["Date"] >= fecha_inicio) & (interacciones_stakeholder["Date"] <= fecha_fin)])
        
    numero_total_filas += len(interacciones_stakeholder) #int(filtrado["Full Text"])

  time.sleep(time_sleep)
  pestana.update("A4", [["Total Filas", numero_total_filas]])
  time.sleep(time_sleep)
  pestana.update("D1", [["Fecha Inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])
  time.sleep(time_sleep)
  index = 7
  columna_pestana_ascii = 66
  segunda_columna_pestana_ascii = 65
  dos_columnas = 0

  for stakeholderr in stakeholders_filtrados:
    if columna_pestana_ascii > 90 and dos_columnas < 1:
      columna_pestana_ascii = 67
      dos_columnas = 1
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    elif columna_pestana_ascii > 90 and dos_columnas > 0:
      segunda_columna_pestana_ascii += 1
      columna_pestana_ascii = 65
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    elif columna_pestana_ascii < 90 and dos_columnas > 0:
      columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
    else:
      columna = chr(columna_pestana_ascii)

    pestana.update((columna + str(index)), [[stakeholderr, (stakeholderr + "%"), (stakeholderr + "sentiment")]])
    time.sleep(time_sleep)
    columna_pestana_ascii += 3

  #Formato de procentaje para celda de Excel
  cell_decimal_format = gsf.cellFormat(
  numberFormat = gsf.numberFormat("NUMBER", pattern = "##.###%") 
  ) 

  barra_progreso = progressbar.ProgressBar(max_value = len(column_dictonary))
  progreso = 0

  index = 8
  for column in column_dictonary:
    progreso += 1
    barra_progreso.update(progreso)
    for word in column:
      if type(word) != float:
        if word in columns[0]:
          index += 1
        columna_pestana_ascii = 66
        segunda_columna_pestana_ascii = 65
        dos_columnas = 0
        pestana.update(("A" + str(index)), [[word]])
        print(stakeholders_filtrados)
        for stakeholder in stakeholders_filtrados:
          print(stakeholder)
          if fecha_inicio != "-" and fecha_fin != "-": #Filtro fechas
            filtrado = (df.loc[(df["Date"] >= fecha_inicio) & (df["Date"] <= fecha_fin)])
          else:
            filtrado = df
          if profession == "ALL": #Filtro profesiones
            if country == "ALL": #Filtro Pais
              continue
            else:
              filtrado = filtrado.loc[filtrado[country_name] == country]
            filtrado_textos = filtrado.loc[filtrado[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False) & filtrado["Full Text"].str.contains(word, regex=False, na=False, case=False)]
            filtrado = (len(filtrado.loc[filtrado[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False) & filtrado["Full Text"].str.contains(word, regex=False, na=False, case=False)]))
          else:
            if country == "ALL":
              continue
            else:
              filtrado = filtrado.loc[filtrado[country_name] == country]
              filtrado = filtrado.loc[filtrado[profession_name] == profession]
            filtrado_textos = filtrado.loc[filtrado[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False) & filtrado["Full Text"].str.contains(word, regex=False, na=False, case=False)]
            filtrado = (len(filtrado.loc[filtrado[stakeholder_name].str.contains(str(stakeholder), regex=False, na=False, case=False) & filtrado["Full Text"].str.contains(word, regex=False, na=False, case=False)]))
          
          print(filtrado_textos)
          sentimiento = 0
          for tweet in filtrado_textos["Full Text"]:
            sentimiento += sentiment.sentiment(str(tweet))

          if columna_pestana_ascii > 90 and dos_columnas < 1:
            columna_pestana_ascii = 67
            dos_columnas = 1
            columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
            columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
          elif  columna_pestana_ascii > 90 and dos_columnas > 0:
            segunda_columna_pestana_ascii += 1
            columna_pestana_ascii = 65
            columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
            columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
          elif columna_pestana_ascii < 90 and dos_columnas > 0:
            columna = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii))
            columna_siguiente = str(chr(segunda_columna_pestana_ascii) + chr(columna_pestana_ascii + 1))
          else:
            columna = chr(columna_pestana_ascii)
            columna_siguiente = chr(columna_pestana_ascii + 1)

          if columna_siguiente is not "[":
            gsf.format_cell_range(pestana, columna_siguiente, cell_decimal_format)

          porcentaje = "="+ (columna) + str(index) + "/$B$4"
          if filtrado > 0:
            sentimiento = sentimiento / float(filtrado)
            if sentimiento > 1:
              sentimiento = 1
          else:
            sentimiento = "-"
          pestana.update((columna + str(index)), [[filtrado, (porcentaje), sentimiento]], value_input_option='USER_ENTERED')
          columna_pestana_ascii += 3
        index += 1
        time.sleep(2)
  del df

#Similar a la aplicacion de filtros demograficos, con la diferencia de que se puede excluir a una categoria de stakeholders en particular
def aplicacion_Filtro_Excluir_Stakeholders(archivo_interacciones, nombre_pestana, country, profession, categoria, columna_analisis, fecha_inicio, fecha_fin):
  gc.enable()
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(archivo_interacciones, names=columns_data, encoding='latin1', usecols=columns_data)
  stakeholder_name = columna_analisis
  stakeholders_filtrados = []

  indice_stakeholders = stakeholders[0].index(categoria)
  for stakeholder in stakeholders_dictonary[indice_stakeholders]:
    if type(stakeholder) != float and stakeholder != stakeholders_dictonary[indice_stakeholders][0]:
      if stakeholder_name == "Mentioned Authors":
        stakeholders_filtrados.append(("@" + stakeholder.lower()))
      else:
        stakeholders_filtrados.append(stakeholder)

  country_name = "Country Code" #Columna de codigos de paises para filtro
  gender_name = "Gender" #Columna de codigos de genero para filtro
  profession_name = "Professions" #Columna de profesiones para filtro

  gender = "unknown"
  male = "male"
  female = "female"

  añadir_Pestaña(nombre_pestana)
  pestana = documento.worksheet(nombre_pestana) #Nombre de pestana en donde se va a escribir info
  time_sleep = 0.25

  pestana.update("A1", [["Pais", country, ""], ["Profesion", profession], ["Excluidos", categoria]])
  time.sleep(time_sleep)
  pestana.update("D1", [["Fecha Inicio", fecha_inicio], ["Fecha Fin", fecha_fin]])
  time.sleep(time_sleep)

  numero_filas_incluidas_totales = 0
  numero_filas_incluidas_hombres = 0
  numero_filas_incluidas_mujeres = 0
  numero_filas_incluidas_unknown = 0

  if fecha_inicio != "-" and fecha_fin != "-": #Filtro fechas
      df = (df.loc[(df["Date"] >= fecha_inicio) & (df["Date"] <= fecha_fin)])

    #Para cualquier profesion
  if profession == "ALL":
    if country == "ALL":
      country_gender = df #En caso de que el país no sea un elemento para generar un filtro
    else:
      country_gender = df.loc[df[country_name] == country]
    if stakeholder_name == "Author":
      contador_incluidos = country_gender[~country_gender.Author.isin(stakeholders_filtrados)]
    else:
      contador_incluidos = country_gender[~country_gender[stakeholder_name].str.contains('|'.join(stakeholders_filtrados), na=False, case=False)]  
  else:
    #Para profesion en particular
    if country == "ALL":
      country_gender = df
    else:
      country_gender = df.loc[df[country_name] == country]
    profession_gender = country_gender.loc[country_gender[profession_name].str.contains(profession, regex=False, na=False, case=False)]
    if stakeholder_name == "Author":
      contador_incluidos = profession_gender.loc[~profession_gender.Author.isin(stakeholders_filtrados)]
    else:
      contador_incluidos = profession_gender.loc[~profession_gender[stakeholder_name].str.contains('|'.join(stakeholders_filtrados), na=False, case=False)]   

  contador_hombres = (contador_incluidos.loc[df[gender_name] == male].count())
  contador_hombres_int = (int(contador_hombres["Full Text"]))
  contador_mujeres = (contador_incluidos.loc[df[gender_name] == female].count())
  contador_mujeres_int = (int(contador_mujeres["Full Text"]))
  contador_unknown = (contador_incluidos.loc[df[gender_name] == gender].count())
  contador_unknown_int = (int(contador_unknown["Full Text"]))
  contador_totales = contador_hombres_int + contador_mujeres_int + contador_unknown_int

  pestana.update("A4", [["Hombres", contador_hombres_int], ["Mujeres", contador_mujeres_int], ["No especificado", contador_unknown_int], ["Total Filas", contador_totales]])
  time.sleep(time_sleep)
  pestana.update("B8", [["Hombres", "Mujeres", "No especificado", "% Hombres", "% Mujeres", "% No especificado","Sentimiento Hombres", "Sentimiento Mujeres", "Sentimiento No especificados", "Total", "%Total"]])
  time.sleep(time_sleep)

  origin_cell = "A9"
  cell_number = 9
  origin_cell_men = "B10"
  origin_cell_women = "C10"
  origin_cell_unknown = "D10"

  #Formato de procentaje para celda de Excel
  cell_decimal_format = gsf.cellFormat(
    numberFormat = gsf.numberFormat("NUMBER", pattern = "##.###%") 
  )

  barra_progreso = progressbar.ProgressBar(max_value = len(column_dictonary))
  progreso = 0

  bandera = 0
  for column in column_dictonary:
    progreso += 1
    barra_progreso.update(progreso)
    for word in column:
      #gc.collect()
      time.sleep(time_sleep)
      if type(word) != float:
        men_counts = contador_incluidos.loc[df[gender_name] == male]["Full Text"].str.contains(word, case=False).value_counts()
        female_counts = contador_incluidos.loc[df[gender_name] == female]["Full Text"].str.contains(word, case=False).value_counts()
        unknown_counts = contador_incluidos.loc[df[gender_name] == gender]["Full Text"].str.contains(word, case=False).value_counts()
        try:
          men_counts = men_counts.iloc[1]
        except:
          men_counts = 0
        try:
          female_counts = female_counts.iloc[1]
        except:
          female_counts = 0
        try:
          unknown_counts = unknown_counts.iloc[1]
        except:
          unknown_counts = 0
        
        sentimiento_hombres = 0
        hombres = contador_incluidos.loc[df[gender_name] == male]
        hombres = hombres[hombres["Full Text"].str.contains(word, case=False)]
        for tweet in hombres["Full Text"]:
          if (sentiment.sentiment(str(tweet)) <= 1):
            sentimiento_hombres += sentiment.sentiment(str(tweet))
        if men_counts > 0:
          sentimiento_hombres = sentimiento_hombres/float(men_counts)
          if sentimiento_hombres > 1:
            sentimiento_hombres = "Por definir"
        else:
          sentimiento_hombres = "-"
        
        mujeres = contador_incluidos.loc[df[gender_name] == female]
        mujeres = mujeres[mujeres["Full Text"].str.contains(word, case=False)]
        sentimiento_mujeres = 0
        for tweet in mujeres["Full Text"]:
          if (sentiment.sentiment(str(tweet)) <= 1):
           sentimiento_mujeres += sentiment.sentiment(str(tweet))
        if female_counts > 0:
          sentimiento_mujeres = sentimiento_mujeres/float(female_counts)
          if sentimiento_mujeres > 1:
            sentimiento_mujeres = "Por definir"
        else:
          sentimiento_mujeres = "-"
        
        desconocidos = contador_incluidos.loc[df[gender_name] == gender]
        desconocidos = desconocidos[desconocidos["Full Text"].str.contains(word, case=False)]
        sentimiento_desconocido = 0
        for tweet in desconocidos["Full Text"]:
          if (sentiment.sentiment(str(tweet)) <= 1):
            sentimiento_desconocido += sentiment.sentiment(str(tweet))
        if unknown_counts> 0:
          sentimiento_desconocido = sentimiento_desconocido/float(unknown_counts)
          if sentimiento_desconocido > 1:
            sentimiento_desconocido = "Por definir"
        else:
          sentimiento_desconocido = "-"

        if word in columns[0]:
          pestana.update(origin_cell, [[" ", " ", " ", " ", " ", " ", " ", " "]])
          cell_number += 1
          origin_cell = "A" + str(cell_number)
          origin_cell_men = "B" + str(cell_number)
          origin_cell_women = "C" + str(cell_number)
          origin_cell_unknown = "D" + str(cell_number)
          origin_cell_total = "K" + str(cell_number)

          gsf.format_cell_range(pestana, "E:G", cell_decimal_format)
          gsf.format_cell_range(pestana, "L:M", cell_decimal_format)
          time.sleep(time_sleep)

          pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$4' % origin_cell_men), ('=%s/$B$5' % origin_cell_women), ('=%s/$B$6' % origin_cell_unknown), (sentimiento_hombres), (sentimiento_mujeres), (sentimiento_desconocido), int(men_counts+female_counts+unknown_counts), ('=%s/$B$7' % origin_cell_total)]], value_input_option='USER_ENTERED')
          time.sleep(time_sleep)  
        else:
          if bandera == 0:
            pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$4' % ("B" + str(cell_number))), ('=%s/$B$5' % ("C" + str(cell_number))), ('=%s/$B$6' % ("D" + str(cell_number))), (sentimiento_hombres), (sentimiento_mujeres), (sentimiento_desconocido), int(men_counts+female_counts+unknown_counts), ('=%s/$B$7' % ("K" + str(cell_number)))]], value_input_option='USER_ENTERED')  
            bandera = 1
          else:
            gsf.format_cell_range(pestana, "E:G", cell_decimal_format)
            gsf.format_cell_range(pestana, "L:M", cell_decimal_format)
            time.sleep(time_sleep)
            pestana.update(origin_cell, [[word, int(men_counts), int(female_counts), int(unknown_counts), ('=%s/$B$4' % origin_cell_men), ('=%s/$B$5' % origin_cell_women), ('=%s/$B$6' % origin_cell_unknown), (sentimiento_hombres), (sentimiento_mujeres), (sentimiento_desconocido), int(men_counts+female_counts+unknown_counts), ('=%s/$B$7' % origin_cell_total)]], value_input_option='USER_ENTERED')
            time.sleep(time_sleep)
    
        cell_number += 1
        origin_cell = "A" + str(cell_number)
        origin_cell_men = "B" + str(cell_number)
        origin_cell_women = "C" + str(cell_number)
        origin_cell_unknown = "D" + str(cell_number)
        origin_cell_total = "K" + str(cell_number)

  del df

#Se agrega el sentimiento, numerico y en palabras en csv de interacciones
def agregarSentimientoCSV(archivo_interacciones):
  sentiment = sentiment_analysis.SentimentAnalysisSpanish()
  df = pd.read_csv(archivo_interacciones, names=columns_data, encoding='latin1', usecols=columns_data)
  sentimiento_numerico = []
  sentimiento_NPS = []
  tweets = df["Full Text"]
  for tweet in tweets:
    if tweet == tweets[0]:
      sentimiento_numerico.append("Numerical Sentiment [0-1]")
      sentimiento_NPS.append("NPS Sentiment")
    else:
      sentimiento = sentiment.sentiment(str(tweet))
      sentimiento_numerico.append(round(sentimiento, 5))
      sentimiento_escrito = ""
      if sentimiento >= 0 and sentimiento < 0.3:
        sentimiento_escrito = "Negativo"
      elif sentimiento >= 0.3 and sentimiento < 0.8:
        sentimiento_escrito = "Neutral"
      else:
        sentimiento_escrito = "Positivo"
      sentimiento_NPS.append(sentimiento_escrito)
  df["Sentimiento Numerico"] = sentimiento_numerico
  df["Sentimiento Escrito"] = sentimiento_NPS
  print(df)

  with open("/content/drive/Shared drives/(50 PROJ-CORP) DATA ANALYTICS/Proyectos/Clientes/Grupo Los Pueblos/py_cgp_criptomonedas_Sentimientos.csv", 'w', encoding='latin1') as f:
    df.to_csv(f, index=False, header=False)

#Ejecutar, de acuerdo a la pestana de configuracion del Sheets de Drive, las diferentes funciones
def ejecucionScripts(archivo_interacciones):
  pestana = documento.worksheet("Parametros_Ejecucion")
  informarcion = pestana.get_all_values()
  print(informarcion)
  for ejecucion in informarcion:
    if ejecucion == informarcion[0]:
      continue
    else:
      if ejecucion[1] == "Demograficos":
        globals()["aplicacion_filtros_demograficos"](archivo_interacciones, ejecucion[0], ejecucion[2], ejecucion[3], ejecucion[4], ejecucion[5])
      elif ejecucion[1] == "Stakeholders_Condensado":
        globals()["aplicacion_Filtro_Stakeholders_Condensado"](archivo_interacciones, ejecucion[0], ejecucion[2], ejecucion[3], ejecucion[6], ejecucion[8], ejecucion[4], ejecucion[5])
      elif ejecucion[1] == "Stakeholders_Expandido":
        globals()["aplicacion_Filtro_Stakeholders_Expandido"](archivo_interacciones, ejecucion[0], ejecucion[2], ejecucion[3], ejecucion[6], ejecucion[8], ejecucion[4], ejecucion[5])
      elif ejecucion[1] == "Excluir_Stakeholders":
        globals()["aplicacion_Filtro_Excluir_Stakeholders"](archivo_interacciones, ejecucion[0], ejecucion[2], ejecucion[3], ejecucion[7], ejecucion[8], ejecucion[4], ejecucion[5])
      elif ejecucion[1] == "Demograficos_Condensado":
        globals()["aplicacion_Filtro_Demograficos_Condensado"](archivo_interacciones, ejecucion[0], ejecucion[2], ejecucion[3], ejecucion[4], ejecucion[5])


## TO DO ##
# Sentimiento IBM Watson
# Modificar la funcion de agregarSentimientosCSV, la parte de las rutas
# Aplicar redondeo a columna de sentimientos
# Preguntar Adán como se va a abordar lo del video (archivos de entrenamiento principalmente)
# Creacion de nueva funcion Expandido
# Fuente de datos (twitter, news, etc.)
# Quitar impresiones
# Pensar intereses cruzados
# Ver como evitar error 500