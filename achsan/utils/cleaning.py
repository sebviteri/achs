### PARA LIMPIEZAS #######################
# Funciones para limpiar datos de pandas
# o PySpark
##########################################

import pandas as pd
import typing

def limpia_nombre_cols(df: pd.DataFrame) -> typing.Union[None, pd.DataFrame]:
    """
    Limpia nombre de columnas, dejando todo en minúsculas, con guión bajo en vez de espacios, y sin tildes ni caracteres no ASCII
    """
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_").str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df