############ PARA LIMPIEZAS ############
import pandas as pd

def limpia_nombre_cols(df):
    """
    Limpia nombre de columnas, dejando todo en minúsculas, con guión bajo en vez de espacios, y sin tildes ni caracteres no ASCII
    """
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_").str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df