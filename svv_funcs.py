############ PARA LIMPIEZAS ############
import pandas as pd

def limpia_nombre_cols(df):
    """
    Limpia nombre de columnas, dejando todo en minúsculas, con guión bajo en vez de espacios, y sin tildes ni caracteres no ASCII
    """
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_").str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df


############ PARA SQL ############
import pandas as pd
import pyodbc
import typing

def carga_query(filepath:str) -> str:
    """Carga una query desde un archivo .sql a un objeto string de python

    Args:
        filepath (str): Path en donde se encuentra el archivo .sql

    Returns:
        str: string con la query sql
    """
    with open(f"{filepath}", encoding='UTF-8') as f:
        query_sql_lines = f.readlines()
        query_sql = [''.join(query_sql_lines)]
    print(f"OK: Carga de queries")
    return query_sql



def descarga_de_sql_server(query:str, server_url:str='ACHS-AUTMDBAZ.achs.cl', database:str='az-cdg', username=None, password=None) -> typing.Union[None, pd.DataFrame]:
    """Conecta a un servidor SQL de ACHS

    Args:
        query (str)     : Query que se utilizará para descargar información
        server_url (str): URL en donde se encuentra el servidor
        database (str)  : Base de datos en donde se aplicará la query

    Returns:
        pd.DataFrame: Dataframe con la tabla resultante de la query
    """
    sql_conn_str = f"""
        Driver={{SQL Server}};
        Server={server_url};
        Database={database};
        Trusted_Connection=yes;
        """
    if (username is not None) & (password is not None):
        sql_conn_str = f"""
            Driver={{SQL Server}};
            Server={server_url};
            Database={database};
            UID={username};
            PWD={password}
            """

    ini_ahora = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
    print(f"Inicio descarga desde SQL = {ini_ahora}")
    try:
        
        sql_conn = pyodbc.connect(sql_conn_str) 

        print(f"OK: Conexión a SQL Server")
    except Exception as e:
        print(f"ERROR: Falla conexión a SQL Server por: {e}")
        return None

    # Carga desde query hasta dataframe
    try:
        df = pd.read_sql(sql=query, con=sql_conn)
        fin_ahora = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
        print(f"OK: Carga de tablas SQL a Dataframe {df.shape}")
        print(f"Fin descarga desde SQL = {fin_ahora}")

        return df
    except Exception as e:
        print(f"ERROR: Falla descarga de datos SQL por: {e}")
        return None
        



def load_csvs_into_df(path, prefix='', file_ext='csv', encoding='utf-8'):
    """ 
    Carga todos los archivos .csv que existan en una carpeta y los convierte en un dataframe
    """
    csv_files = glob.glob(os.path.join(path, f"*.{file_ext}"))
    print(f"Cantidad de archivos a compilar = {len(csv_files)}")
    df = pd.DataFrame()
    for f in csv_files:
        filename = f.split("/")[-1].replace(f'.{file_ext}', '')
        this_df = pd.read_csv(f, encoding=encoding)
        this_df['filename'] = filename
        df = pd.concat([df, this_df])
    
    df = df.reset_index(drop=True)
    print(f"{df.shape = }")

    return df

    