import gspread
import pandas as pd

def credenciales ():
    """
    ## Descripción:
    Genera conexión con documento google sheets
    Returns:
        hoja_excel: URL de documento google sheets 
    """
    gc = gspread.service_account(filename='C:\proyectos\sos\credenciales\credentials.json')
    hoja_excel = gc.open_by_url('https://docs.google.com/spreadsheets/d/1vhKqsudLV-iyIbL8GReUOqi02tj16LUZmvGc3o0C5lE/edit#gid=0')
    return hoja_excel

