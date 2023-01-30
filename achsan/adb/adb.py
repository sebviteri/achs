### PARA DATABRICKS ######################
# Funciones para Azure Databricks
# Made by NachoV
##########################################
 
import pytz
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame
from IPython.display import HTML
import dbutils

class DescripcionTablas:
    """
    El objetivo de la clase es identificar si al momento de ejecución del notebook la data ya pertenece a este dia. 

    tablas_ejemplo = ['db_trusted_soporte_casos_de_uso.reporte_pacientes_activos_formatohistorico_pbi','db_trusted_siniestros.bt_siniestros']
    
    # DESCRIPCION DE LA TABLA CON TODAS LAS COLUMNAS
    get_descripcion_tablas(tablas_ejemplo).crear_tabla()      

    # VERSIÓN CORTA DE LA ANTERIOR
    get_descripcion_tablas(tablas_ejemplo).resumen()          

    # ORDENA DETENCIÓN DE NOTEBOOK Y FLUJO EN DATAFACTORY (FUNCIONA AL SER INVOCADO DESDE UN PIPELINE)
    get_descripcion_tablas(tablas_ejemplo).abortar_notebook() 


    # Forma de uso: 
    casos = [i.replace('temp__','').replace('__','.').replace('''"''','').strip() for i in json.dumps(temp_views)[2:-2].split(',')]
    get_descripcion_tablas(casos).resumen()
    """
    def __init__(self, lista):
        self.lista = lista


    # AJUSTAR A HORARIO DE SANTIAGO
    def ajustar_hora(self, utc_time):   
        respuesta = utc_time.replace(tzinfo = pytz.utc).astimezone(pytz.timezone('Chile/Continental'))
        return respuesta
    
    
    # CREAR DATAFRAME 
    def crear_tabla(self):
        country_time = datetime.now(pytz.timezone('Chile/Continental')) # HORARIO SANTIAGO
        filas=[]
        for i in self.lista:
          filas.append(spark.sql(f'DESCRIBE DETAIL {i}'))
        df = reduce(DataFrame.union, filas).toPandas()
        df['lastModified_Chile'] = df.lastModified.apply(lambda i: self.ajustar_hora(i))
        df['executed_on_time?']  = df.lastModified_Chile.apply(lambda i: True if i.date() == country_time.date() and country_time.time() > i.time() else False)
        df['now_Chile']          = country_time.isoformat(' ', 'seconds')
        return df.sort_values(by = ['lastModified_Chile'], axis = 0, ascending = False)
    

    # FITLRAR DATAFRAME
    def resumen(self):
        return HTML(self.crear_tabla().filter(['name','lastModified_Chile','now_Chile','executed_on_time?','location']).to_html())
    
    
    # FUNCION QUE PERMITE ABORTAR LA EJECUCIÓN DE UN PIPELINE EN CASO DE QUE HAYA UNA TABLA SIN DATA ACTUAL
    def abortar_notebook(self):
        casos=['db_trusted_soporte_casos_de_uso.reporte_pacientes_activos_formatohistorico_pbi','db_trusted_siniestros.bt_siniestros']
        if False in self.crear_tabla()['executed_on_time?'].to_list():
            dbutils.notebook.exit("Data desactualizada")
        else:
            print('Data actualizada')

