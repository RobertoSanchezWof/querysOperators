import re
import pandas as pd
import json
from datetime import datetime
from Uploader import exportar

dfDescription = pd.DataFrame(columns=['Description'])
dfData = pd.DataFrame(columns=['Valor', 'Total', 'Porcentaje'],
                    data=[['Valor', 'Total','porcentaje']])

def LeftMerge(dfFires, dfReports) -> pd.DataFrame:
    '''Une la data de Fire y History por el ID del incendio, lo guarda como CSV'''
    return pd.merge(dfFires, dfReports, how='left', left_on='id', right_on='fire', indicator=True)

def FiresReportsMerge(dfFires,dfReports) -> tuple[pd.DataFrame, int, pd.DataFrame, pd.DataFrame]:
    """ Ejecuta un left join entre los datos de fuegos y reportes

        Returns:
            df: DataFrame con los datos de fuegos y reportes
            per: Porcentaje de fuegos con reporte
            dfOnlyLeft: DataFrame con los datos de fuegos sin reporte
            dfOnlyRight: DataFrame con los datos de reportes sin fuego
    """
    # Mescla los datos de fuegos y reportes
    df = LeftMerge(dfFires, dfReports)
    # Obtenemos los fuegos con reporte
    dfBoth = df[df['_merge'] == "both"]
    # Obtenemos los fuegos sin reporte
    dfOnlyLeft = df[df['_merge'] == "left_only"]
    # Obtenemos los reportes sin fuego, caso especial. Esto quiere decir que el fuego quedo fuera del rango de tiempo.
    dfOnlyRight = df[df['_merge'] == "right_only"]
    
    total = dfBoth.shape[0] + dfOnlyLeft.shape[0]
    per = (dfBoth.shape[0]/(total))*100
    string = '{:.2f}%'.format(per)
    # dfData.loc[len(dfData)] = [casosPresentados, total, string]
    # dfDescription.loc[len(dfDescription)] = ["Fuegos con Reporte"]
    return dfBoth, per, dfOnlyLeft, dfOnlyRight

def ReportBank(firesInHistory: pd.DataFrame, firesNotInHistory: pd.DataFrame):
    '''Entrega la información de los incendios y reportes agrupados por banco'''
    listData = []
    
    withReport = firesInHistory
    noReport = firesNotInHistory
    
    systemList = [x.replace("'", "\"") for x in withReport["system"].tolist()]
    diccionario = {i: json.loads(x) for i, x in enumerate(systemList)}
    dfNombres = pd.DataFrame.from_dict(diccionario, orient="index")
    
    systemListNoReport = [x.replace("'", "\"") for x in noReport["system"].tolist()]
    diccionarioNoReport = {i: json.loads(x) for i, x in enumerate(systemListNoReport)}
    dfNombresNoReport = pd.DataFrame.from_dict(diccionarioNoReport, orient="index")
    
    countsIn = dfNombres.groupby('system_name').count().drop(columns=['version', 
                                                                    'utc_minute_offset',
                                                                    'utc_offset']).rename(columns={'api_key': 'Reportados'})
    countsNotIn = dfNombresNoReport.groupby('system_name').count().drop(columns=['version', 
                                                                                'utc_minute_offset',
                                                                                'utc_offset']).rename(columns={'api_key': 'No Reportados'})
    
    df = pd.merge(countsIn, countsNotIn, left_on='system_name', right_on='system_name', how='outer')    
    #elimina los decimales de los valores
    df = df.fillna(0).astype(int)
    dfText = pd.DataFrame(df.index)
    nuevo_dato = pd.DataFrame({'Reportados': ['Reportados'], 'No Reportados': ['No Reportados']})
    df = pd.concat([nuevo_dato,df], ignore_index=True)
    exportar(dfText, df, 2, date)

def msg(string):
    result = re.findall(r"(?:(?<!\{)\b.*?(?='?\},|$))", string)
    print(string)
    print(result)
    #'msg': ''

def msjPromedio(firesInHistory: pd.DataFrame):
    '''Promedia el numero de mensajes por reporte.'''
    numeroMsg = []
    df = firesInHistory
    #df["events"].head(1).apply(lambda x: msg(x))
    for element in df["events"].head(10):
        count = element.count("state")
        numeroMsg.append(count)
    promedio = sum(numeroMsg)/len(numeroMsg)
    dfDescription.loc[len(dfDescription)] = ["Mensajes promedio por reporte"]
    dfData.loc[len(dfData)] = [promedio, None, None]

def time(x):
    results = re.findall(r"'time': DatetimeWithNanoseconds\((.*?), tzinfo=datetime.timezone.utc\)", x)
    # results = [datetime.strptime(x, '%Y, %m, %d, %H, %M, %S') for x in results]
    fecha = results[0]
    try:
        # Intenta convertir la fecha con segundos
        fechaConvertida = datetime.strptime(fecha, '%Y, %m, %d, %H, %M, %S')
    except ValueError:
        # Si falla, intenta convertir la fecha sin segundos
        fechaConvertida = datetime.strptime(fecha, '%Y, %m, %d, %H, %M')
    return fechaConvertida

def PromReportTime(df: pd.DataFrame):
    """Promedia el tiempo de reacción entre detección y apertura """
    promTimes = []
    for index, row in df.iterrows():
        dateFire = time (row["times"])
        openTime = pd.to_datetime(row['timestamp_open']).to_pydatetime().replace(tzinfo=None)
        times = openTime - dateFire
        promTimes.append(times)
    df.insert(len(df.columns), "reaction_time", promTimes)
    promTimes = pd.Series(promTimes)
    promedio = promTimes.mean()
    horas, rem = divmod (promedio.total_seconds(), 3600)
    minutos, segundos = divmod(rem, 60)
    time_str = '{:02.0f}:{:02.0f}:{:02.0f}'.format(horas, minutos, segundos)
    df['reaction_time'] = df['reaction_time'].astype(str)
    
    #dfData.loc[len(dfData)] = [time_str, None, None]
    #dfDescription.loc[len(dfDescription)] = ["Tiempo promedio de reacción a incendios"]

def promOpenClose(firesInHistory: pd.DataFrame):
    '''Promedia el tiempo entre que se abre y cierra un reporte'''
    cicloDeVida = []
    df = firesInHistory
    df["timestamp_open"] = pd.to_datetime(df["timestamp_open"])
    df["timestamp_close"] = pd.to_datetime(df["timestamp_close"])
    for open, close in zip(df["timestamp_open"], df["timestamp_close"]):
        time = close - open
        cicloDeVida.append(time)
    cicloDeVida = pd.Series(cicloDeVida)
    promedio = cicloDeVida.mean()
    horas, rem = divmod (promedio.total_seconds(), 3600)
    minutos, segundos = divmod(rem, 60)
    time_str = '{:02.0f}:{:02.0f}:{:02.0f}'.format(horas, minutos, segundos)
    dfData.loc[len(dfData)] = [time_str, None, None]
    dfDescription.loc[len(dfDescription)] = ["Tiempo promedio apertura y cierre"]

def reportDispatcher():
    '''Muestra los reportes que han sido despachados (JSON Uruguay)'''
    df = pd.read_csv("data/merge.csv", sep="_")
    dfUruguay = df.loc[(df["client_x"] == "SPF") & (df["json_sended"].notnull())]
    dfDescription.loc[len(dfDescription)] = ["Reportes despachados para Uruguay"]
    dfData.loc[len(dfData)] = [len(dfUruguay), None, None]

def reportForOperator():
    '''Reportes agrupados por operador.'''
    df = pd.read_csv("data/merge.csv", sep="_")
    newDataFrame = df.groupby("operator").count()
    newDataFrame = newDataFrame[['fire_x']]
    newDataFrame.rename(columns={"fire_x": "N° Reportes"}, inplace=True)
    dfText = pd.DataFrame(newDataFrame.index)
    newData = pd.DataFrame({'N° Reportes': ['N° Reportes']})
    newDataFrame = pd.concat([newData, newDataFrame], ignore_index=True)
    exportar(dfText, newDataFrame, 3, date) 

def CountReportsWithoutInternalID(df : pd.DataFrame) -> tuple[int, int, float]:
    '''Numero de reportes sin ID interno'''
    total = df.size[0]
    count = df["internal_id"].isna().sum()
    porcentaje = (count/total)*100
    return count, total, porcentaje    
    #dfData.loc[len(dfData)] = [count, total, string]
    #dfDescription.loc[len(dfDescription)] = ["Reportes sin Id interno"]

def poolForDispatcher(firesInHistory : pd.DataFrame):
    '''Numero de reportes por despachador.'''
    df = firesInHistory
    DataFrameDispachers = df.groupby("dispatcher").count()
    DataFrameDispachers = DataFrameDispachers[['id']]
    DataFrameDispachers.rename(columns={"id": "N° Reportes"}, inplace=True)
    dfText = pd.DataFrame(DataFrameDispachers.index)
    newData = pd.DataFrame({'N° Reportes': ['N° Reportes']})
    DataFrameDispachers = pd.concat([newData, DataFrameDispachers], ignore_index=True)
    exportar(dfText, DataFrameDispachers, 4, date) 

def takeTime():
    '''Toma la hora de inicio y final de la data'''
    df = pd.read_csv("data/merge.csv", sep="_")
    open = pd.to_datetime(df["timestamp_open"])
    close = pd.to_datetime(df["timestamp_close"])
    open = open.min().date().strftime("%d/%m")
    close = close.max().date().strftime("%d/%m")
    date = "Periodos: " + open + " - " + close
    return date

#monitor()
date = takeTime()

