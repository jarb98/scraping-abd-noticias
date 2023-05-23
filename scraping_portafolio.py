import requests
from bs4 import BeautifulSoup
import re
import random
import time
import pymongo
import os
from bson.objectid import ObjectId


#Arreglar dos cosas de manera inicial

#2. Subir la data a mi cluster de Mongo




def recoleccion_portafolio( no_inicial, no_final, termino):

    lista_portafolio = []

    conexion_mongo = os.environ.get('CONEXION_MONGO')

    #ciclo en el rango que se quiere iterar
    for i in range (no_inicial, no_final+1):

        url = 'https://www.portafolio.co/buscar?q={}&page={}'.format(termino,i+1)
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")    
        else:
            break

        #Irse a dormir 
        tiempo_sueno = random.randint(3,12)
        print(f"voy a dormir: {tiempo_sueno} minutos")
        #time.sleep(tiempo_sueno*60)

        #Scrape la data de la pagina de ese tema y ese numero de pagina

        fechas_eltiempo = soup.find_all("div", class_="time")

        titulo_eltiempo = soup.find_all("a", class_="news-title")

        categoria_eltiempo = soup.find_all("div", class_="listing-category")

        texto_pequeño_eltiempo = soup.find_all("div", class_="listing-epigraph")

        #Crear un diccionario para subir la data a Mongo
        for element in range(len(fechas_eltiempo)):
            unique_id = ObjectId()


            diccionario_actual = {'_id':unique_id,
                                'fecha':fechas_eltiempo[element].get_text(), 
                                'titulo':titulo_eltiempo[element].get_text(), 
                                'categoria':categoria_eltiempo[element].get_text(),
                                'texto':texto_pequeño_eltiempo[element].get_text()
            }

            #Unir resultados
            lista_portafolio.append(diccionario_actual)
            

        if lista_portafolio: print(lista_portafolio)
        #Cada vez que lea una pagina se sube a una colección de Mongo
        client = pymongo.MongoClient(conexion_mongo)
        #Crear base de datos
        #Cambiar nombre a Taller cuando lo este poniendo de verdad
        #base_datos_prueba es un objeto tipo Database
        base_datos = client.Taller02_Portafolio

        #Crear una colección 
        if termino not in base_datos.list_collection_names():
            base_datos.create_collection(termino)

        #Crear una colección 
        coleccion_Portafolio = base_datos[termino]
        #Insertar el la data a la colección de la base de datos 
        #Es necesario que sea un diccionario
        for document in lista_portafolio:
            try:
                coleccion_Portafolio.insert_one(document)
            except pymongo.errors.DuplicateKeyError:
                print("Llave duplicada")






#Llamar a la funcion e imprimir los resultados
lista_temas = ['inflacion','desempleo','reforma_tributaria']

for termino in lista_temas: 
    recoleccion_portafolio(1,200,termino)


