# Nacho funcs


class DescripcionTablas:
    """
    El objetivo de la clase es identificar si al momento de ejecución del notebook la data ya pertenece a este dia. 
    tablas_ejemplo=['db_trusted_soporte_casos_de_uso.reporte_pacientes_activos_formatohistorico_pbi','db_trusted_siniestros.bt_siniestros']
    get_descripcion_tablas(tablas_ejemplo).crear_tabla()      # DESCRIPCION DE LA TABLA CON TODAS LAS COLUMNAS
    get_descripcion_tablas(tablas_ejemplo).resumen()          # VERSIÓN CORTA DE LA ANTERIOR
    get_descripcion_tablas(tablas_ejemplo).abortar_notebook() # ORDENA DETENCIÓN DE NOTEBOOK Y FLUJO EN DATAFACTORY (FUNCIONA AL SER INVOCADO DESDE UN     PIPELINE )
    """
    def __init__(self,lista):
        self.lista = lista
    # AJUSTAR A HORARIO DE SANTIAGO
    def ajustar_hora(self,utc_time):    
        import pytz
        respuesta=utc_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Chile/Continental'))
        return respuesta
    # CREAR DATAFRAME 
    def crear_tabla(self):
        from datetime import datetime
        import pytz
        from functools import reduce
        from pyspark.sql import DataFrame
        country_time = datetime.now(pytz.timezone('Chile/Continental')) # HORARIO SANTIAGO
        filas=[]
        for i in self.lista:
          filas.append(spark.sql(f'DESCRIBE DETAIL {i}'))
        df = reduce(DataFrame.union, filas).toPandas()
        df['lastModified_Chile']=df.lastModified.apply(lambda i: self.ajustar_hora(i))
        df['executed_on_time?']=df.lastModified_Chile.apply(lambda i: '✔' if i.date()==country_time.date() and country_time.time()>i.time() else '❌')
        df['now_Chile']=country_time.isoformat(' ', 'seconds')
        return df.sort_values(by=['lastModified_Chile'], axis=0, ascending=False)
    
    # FITLRAR DATAFRAME
    def resumen(self):
        from IPython.display import HTML
        return HTML(self.crear_tabla().filter(['name','lastModified_Chile','now_Chile','executed_on_time?','location']).to_html())
    # FUNCION QUE PERMITE ABORTAR LA EJECUCIÓN DE UN PIPELINE EN CASO DE QUE HAYA UNA TABLA SIN DATA ACTUAL
    def abortar_notebook(self):
        casos=['db_trusted_soporte_casos_de_uso.reporte_pacientes_activos_formatohistorico_pbi','db_trusted_siniestros.bt_siniestros']
        if '❌' in self.crear_tabla()['executed_on_time?'].to_list():
            dbutils.notebook.exit("Data desactualizada")
        else:
            print('Data actualizada')



# # Forma de uso: 
# casos=[i.replace('temp__','').replace('__','.').replace('''"''','').strip() for i in json.dumps(temp_views)[2:-2].split(',')]
# get_descripcion_tablas(casos).resumen()