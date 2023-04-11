import gspread
import gspread.utils as gsutils
import pandas as pd
import os

def credenciales ():
    """
    ## Descripción:
    Genera conexión con documento google sheets
    Returns:
        hoja_excel: URL de documento google sheets 
    """
    path = os.path.join(os.path.expanduser('~'), ".auth", "wof_googlesheets.json")
    print(path)
    gc = gspread.service_account(filename=path)
    hoja_excel = gc.open_by_url('https://docs.google.com/spreadsheets/d/1vhKqsudLV-iyIbL8GReUOqi02tj16LUZmvGc3o0C5lE/edit#gid=0')
    return hoja_excel

def consultar_id (hoja:int):
    """
    ## Descripción:
    Extrae ID de fichas registradas en google sheets
    ### Args:
        hoja (int): Index de hoja de donde extraer IDs

    ### Returns:
        ids: Lista de IDs ya registrados en google sheets
    """
    hoja_excel = credenciales()
    hoja1 = hoja_excel.get_worksheet(hoja)
    ids = hoja1.col_values(1) #extrae columna de ID
    if len(ids) != 0:
        ids.pop(0) # elimina la etiqueta de la lista
    return ids

def QueryInitialRow(worksheetPage:gspread.worksheet, row:int)->bool:
    """Consulta si existe la primera fila de la hoja de trabajo"""
    data = len(worksheetPage.row_values(row))
    flag = True
    if data != 0:
        flag = False
    return flag

def ExportDate(date, page, excelSheet):
    """Inserta fecha en la hoja de trabajo"""
    #selecciona hoja de trabajo
    worksheetPage = excelSheet.get_worksheet(page)
    col = len(worksheetPage.row_values('A1'))
    if page == 0:
        rowCategory = gsutils.rowcol_to_a1(1, col + 3)
        worksheetPage.update(rowCategory , date)
    elif page == 1:
        rowCategory = gsutils.rowcol_to_a1(1, col+2)
        worksheetPage.update(rowCategory , date)
    else:
        rowCategory = gsutils.rowcol_to_a1(1, col+1)
        worksheetPage.update(rowCategory , date)

def ExportHeaders(data, page:int, row:int, excelSheet):
    """Exporta las cabeceras en la hoja de trabajo"""
    #selecciona hoja de trabajo
    worksheetPage = excelSheet.get_worksheet(page)
    #selecciona la posicion en la que se insertara la cabecera
    rowCategory = gsutils.rowcol_to_a1(row, 2)
    col = len(worksheetPage.row_values(rowCategory))
    # si no hay nada ingresa al inicio de la hoja de trabajo
    if col == 0:
        for i in range(len(data)):
            rowCategory = gsutils.rowcol_to_a1(row, i+2)
            worksheetPage.update(rowCategory , data[i])
    else:
        #si hay datos en la hoja de trabajo, inserta los datos a partir de la ultima columna
        col = col + 2
        for i in range(len(data)):
            rowCategory = gsutils.rowcol_to_a1(row, col+i)
            worksheetPage.update(rowCategory , data[i])

def InsertUniqueData(worksheetPage:gspread.worksheet, data, row):
    """inserta datos únicos en la hoja de trabajo"""
    col = len(worksheetPage.row_values('A1'))
    rowCategory = gsutils.rowcol_to_a1(row, col-1)
    worksheetPage.update(rowCategory , data[0])

def Export(dataID, page:int, row:int,excelSheet, data):
    """recive un dataframe y clasifica la información para enviarla a la hoja de trabajo"""
    #selecciona hoja de trabajo
    worksheetPage = excelSheet.get_worksheet(page)
    #consulta si la hoja de trabajo esta vacía
    flag = QueryInitialRow(worksheetPage, row)
    #verifica si el tipo de dato es dataframe
    if isinstance(dataID, pd.DataFrame) and isinstance(data, pd.DataFrame):
        #si no hay nada ingresa al inicio de la hoja de trabajo
        if flag:
            #si no hay nada ingresa al inicio de la hoja de trabajo
            worksheetPage.update('A3', dataID.values.tolist())
            worksheetPage.update('B2', data.values.tolist())
        else:
            # si hay datos en la hoja de trabajo, inserta los datos a partir de la ultima columna
            col = len(worksheetPage.row_values(3)) + 1
            rowCategory = gsutils.rowcol_to_a1(row, col)
            worksheetPage.update(rowCategory , data.values.tolist())
    else: #si no es dataframe repite los mismos pasos pero para datos únicos
        if flag:
            position = gsutils.rowcol_to_a1(row, 1)
            worksheetPage.update(position, dataID)
            for i in range(len(data)):
                position2 = gsutils.rowcol_to_a1(row,i+2)
                worksheetPage.update(position2, data[i])
        else:
            if len(data) == 1:
                InsertUniqueData(worksheetPage, data, row)
            else:
                col = len(worksheetPage.row_values(row)) + 1
                for i in range(len(data)):
                    rowCategory = gsutils.rowcol_to_a1(row, col+i)
                    worksheetPage.update(rowCategory , data[i])