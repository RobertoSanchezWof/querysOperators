import gspread
import gspread.utils as gsutils
import pandas as pd

def credenciales ():
    """
    ## Descripción:
    Genera conexión con documento google sheets
    Returns:
        hoja_excel: URL de documento google sheets 
    """
    gc = gspread.service_account(filename='C:\proyectos\querysOperators\credenciales\credentials.json')
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

def consultar_data(hoja:int, inicio, fin):
    """Consulta si existe la primera fila de la hoja de trabajo"""
    hoja_excel = credenciales()
    pagina = hoja_excel.get_worksheet(hoja)
    data= pagina.range(f'{inicio}:{fin}')
    flag = True
    for cell in data:
        if cell.value != '':
            flag = False
    return flag

def sendPage1(pagina:gspread.worksheet, hoja:int, dftext:pd.DataFrame, dfData:pd.DataFrame, date:str):
    description = consultar_data(hoja, 'A3', 'A10')
    dfData.fillna('', inplace=True)
    if description:
        pagina.merge_cells('B1:D1')
        #fuciona celdas de google sheets B1,C1 y D1
        pagina.update('B1:D1', date)
        #ingresa dftext en columna A desde la fila 3
        pagina.update('A3', dftext.values.tolist())
        pagina.update('B2', dfData.astype(str).values.tolist())
    else:
        col = len(pagina.row_values(3)) + 1
        dateInit = gsutils.rowcol_to_a1(1, col)
        dateEnd = gsutils.rowcol_to_a1(1, col+2)
        pagina.merge_cells(f'{dateInit}:{dateEnd}')
        # incerta fecha en celdas B1, C1 y D1 centrando el texto
        pagina.update(f'{dateInit}:{dateEnd}', date)
        #selecciona la fila de google sheets en base a row
        rowCategory = gsutils.rowcol_to_a1(2, col)
        #ingresa nombre de categorias en la fila 2 de dfdata   
        pagina.update(rowCategory , dfData.astype(str).values.tolist())

def sendPage2(pagina, hoja, dfText, dfData, date):
    description = consultar_data(hoja, 'A3','A14')
    if description:
        pagina.merge_cells('B1:C1')
        pagina.update('B1:C1', date)
        #ingresa dftext en columna A desde la fila 3
        pagina.update('A3', dfText.values.tolist())
        pagina.update('B2', dfData.astype(str).values.tolist())
    else:
        col = len(pagina.row_values(3)) + 1
        dateInit = gsutils.rowcol_to_a1(1, col)
        dateEnd = gsutils.rowcol_to_a1(1, col+1)
        # incerta fecha en celdas B1, C1 y D1 centrando el texto
        pagina.merge_cells(f'{dateInit}:{dateEnd}')
        pagina.update(f'{dateInit}:{dateEnd}', date)
        #selecciona la fila de google sheets en base a row
        rowCategory = gsutils.rowcol_to_a1(2, col)
        #ingresa nombre de categorias en la fila 2 de dfdata   
        pagina.update(rowCategory , dfData.astype(str).values.tolist())

def sendPage3(pagina, hoja, dftext, dfData, date):
    description = consultar_data(hoja, 'A3','A52')
    if description:
        pagina.update('B1', date)
        #ingresa dftext en columna A desde la fila 3
        pagina.update('A3', dftext.values.tolist())
        pagina.update('B2', dfData.astype(str).values.tolist())
    else:
        row = len(pagina.row_values(3)) + 1
        #selecciona la fila de google sheets en base a row
        rowCategory = gsutils.rowcol_to_a1(2, row)
        rowDate = gsutils.rowcol_to_a1(1, row)
        pagina.update(rowDate, date)
        #ingresa nombre de categorias en la fila 2 de dfdata   
        pagina.update(rowCategory , dfData.astype(str).values.tolist())

def sendPage4(pagina, hoja, dftext, dfData, date):
    description = consultar_data(hoja, 'A3','A52')
    if description:
        pagina.update('B1', date)
        #ingresa dftext en columna A desde la fila 3
        pagina.update('A3', dftext.values.tolist())
        pagina.update('B2', dfData.astype(str).values.tolist())
    else:
        row = len(pagina.row_values(3)) + 1
        #selecciona la fila de google sheets en base a row
        rowCategory = gsutils.rowcol_to_a1(2, row)
        rowDate = gsutils.rowcol_to_a1(1, row)
        pagina.update(rowDate, date)
        #ingresa nombre de categorias en la fila 2 de dfdata   
        pagina.update(rowCategory , dfData.astype(str).values.tolist())

def exportar(dftext:pd.DataFrame, dfData:pd.DataFrame, hoja:int, date:str):
    """recive un dataframe y clasifica la información para enviarla a la hoja de trabajo"""
    hoja_excel = credenciales()
    pagina = hoja_excel.get_worksheet(hoja)#selecciona hoja de trabajo
    if (hoja == 1):
        sendPage1(pagina, hoja, dftext, dfData, date)
    elif(hoja == 2):
        sendPage2(pagina, hoja, dftext, dfData , date)
    elif(hoja == 3):
        sendPage3(pagina, hoja, dftext, dfData, date)
    elif(hoja == 4):
        sendPage4(pagina, hoja, dftext, dfData, date)