import re
import pandas as pd
import json
from datetime import datetime
from Uploader import Export

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
    all = df.shape[0]
    # Obtenemos los fuegos con reporte
    dfBoth = df[df['_merge'] == "both"]
    # Obtenemos los fuegos sin reporte
    dfOnlyLeft = df[df['_merge'] == "left_only"]
    # Obtenemos los reportes sin fuego, caso especial. Esto quiere decir que el fuego quedo fuera del rango de tiempo.
    dfOnlyRight = df[df['_merge'] == "right_only"]
    # Calculamos el porcentaje de fuegos con reporte
    total = dfBoth.shape[0] + dfOnlyLeft.shape[0]
    perBoth = (dfBoth.shape[0]/total)*100
    perBoth = round(perBoth, 2)
    
    perLeft = (dfOnlyLeft.shape[0]/total)*100
    perLeft = round(perLeft, 2)
    
    perRight = (dfOnlyRight.shape[0]/total)*100
    perRight = round(perRight, 2)
    
    return dfBoth, perBoth, dfOnlyLeft, perLeft, dfOnlyRight, perRight, all

def GroupByBank(firesInHistory: pd.DataFrame, firesNotInHistory: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''Entrega la información de los incendios y reportes agrupados por banco'''
    withReport = firesInHistory
    noReport = firesNotInHistory
    
    systemList = [x.replace("'", "\"") for x in withReport["system"].tolist()]
    dictionary = {i: json.loads(x) for i, x in enumerate(systemList)}
    dfName = pd.DataFrame.from_dict(dictionary, orient="index")
    
    systemListNoReport = [x.replace("'", "\"") for x in noReport["system"].tolist()]
    dictionaryNotReport = {i: json.loads(x) for i, x in enumerate(systemListNoReport)}
    dfNameNotReport = pd.DataFrame.from_dict(dictionaryNotReport, orient="index")
    
    countsIn = dfName.groupby('system_name').count().drop(columns=['version', 
                                                                    'utc_minute_offset',
                                                                    'utc_offset']).rename(columns={'api_key': 'Reportados'})
    countsNotIn = dfNameNotReport.groupby('system_name').count().drop(columns=['version', 
                                                                                'utc_minute_offset',
                                                                                'utc_offset']).rename(columns={'api_key': 'No Reportados'})
    
    df = pd.merge(countsIn, countsNotIn, left_on='system_name', right_on='system_name', how='outer')   
    #elimina los decimales de los valores
    df = df.fillna(0).astype(int)
    #extrae valores index para crear un dataframe
    dfText = pd.DataFrame(df.index)
    #agrega los valores de reportados y no reportados al inicio del dataframe
    nuevo_dato = pd.DataFrame({'Reportados': ['Reportados'], 'No Reportados': ['No Reportados']})
    df = pd.concat([nuevo_dato,df], ignore_index=True)
    #extrae el periodo de tiempo que se esta analizando
    #envía dataframe a la función exportar increpando el los bancos, los informes, la pagina donde se alojara la información y la fecha
    #exportar(dfText, df, 2, date)
    return dfText, df
    
def msg(string):
    result = re.findall(r"(?:(?<!\{)\b.*?(?='?\},|$))", string)
    print(string)
    print(result)
    #'msg': ''

def AverageMessageCount(df: pd.DataFrame) -> float:
    '''Promedia el numero de mensajes por reporte.'''
    numMsg = []
    #df["events"].head(1).apply(lambda x: msg(x))
    for element in df["events"]:
        count = element.count("state")
        numMsg.append(count)
    average = sum(numMsg)/len(numMsg)
    average = round(average, 2)
    return average

def time(x):
    results = re.findall(r"'time': DatetimeWithNanoseconds\((.*?), tzinfo=datetime.timezone.utc\)", x)
    # results = [datetime.strptime(x, '%Y, %m, %d, %H, %M, %S') for x in results]
    date = results[0]
    try:
        # Intenta convertir la fecha con segundos
        dateTransform = datetime.strptime(date, '%Y, %m, %d, %H, %M, %S')
    except ValueError:
        # Si falla, intenta convertir la fecha sin segundos
        dateTransform = datetime.strptime(date, '%Y, %m, %d, %H, %M')
    return dateTransform

def AverageReportActionTimes(df: pd.DataFrame) -> str:
    """Promedia el tiempo de reacción entre detección y apertura """
    promTimes = []
    for index, row in df.iterrows():
        dateFire = time (row["times"])
        openTime = pd.to_datetime(row['timestamp_open']).to_pydatetime().replace(tzinfo=None)
        times = openTime - dateFire
        promTimes.append(times)
    df.insert(len(df.columns), "reaction_time", promTimes)
    promTimes = pd.Series(promTimes)
    average = promTimes.mean()
    hour, rem = divmod (average.total_seconds(), 3600)
    min, sec = divmod(rem, 60)
    time_str = '{:02.0f}:{:02.0f}:{:02.0f}'.format(hour, min, sec)
    df['reaction_time'] = df['reaction_time'].astype(str)
    return time_str
    #dfData.loc[len(dfData)] = [time_str, None, None]
    #dfDescription.loc[len(dfDescription)] = ["Tiempo promedio de reacción a incendios"]

def AverageOpenCloseTimes(df: pd.DataFrame) -> str:
    '''Promedia el tiempo entre que se abre y cierra un reporte'''
    timeOfLife = []
    df["timestamp_open"] = pd.to_datetime(df["timestamp_open"])
    df["timestamp_close"] = pd.to_datetime(df["timestamp_close"])
    for open, close in zip(df["timestamp_open"], df["timestamp_close"]):
        time = close - open
        timeOfLife.append(time)
    timeOfLife = pd.Series(timeOfLife)
    average = timeOfLife.mean()
    hour, rem = divmod (average.total_seconds(), 3600)
    min, seg = divmod(rem, 60)
    time_str = '{:02.0f}:{:02.0f}:{:02.0f}'.format(hour, min, seg)
    return time_str

def UruguayDispatchedReports(df: pd.DataFrame) -> int:
    '''Muestra los reportes que han sido despachados (JSON Uruguay)'''
    dfUruguay = df.loc[(df["client_x"] == "SPF") & (df["json_sended"].notnull())]
    count = len(dfUruguay)
    return count
    dfDescription.loc[len(dfDescription)] = ["Reportes despachados para Uruguay"]
    dfData.loc[len(dfData)] = [len(dfUruguay), None, None]

def GroupByOperators(df:pd.DataFrame) ->pd.DataFrame:
    '''Reportes agrupados por operador.'''
    newDataFrame = df.groupby("operator").count()
    newDataFrame = newDataFrame[['fire_x']]
    newDataFrame.rename(columns={"fire_x": "N° Reportes"}, inplace=True)
    dfText = pd.DataFrame(newDataFrame.index)
    newData = pd.DataFrame({'N° Reportes': ['N° Reportes']})
    newDataFrame = pd.concat([newData, newDataFrame], ignore_index=True)
    return dfText, newDataFrame
    #exportar(dfText, newDataFrame, 3, date) 

def CountReportsWithoutInternalID(df : pd.DataFrame) -> tuple[int, int, float]:
    '''Numero de reportes sin ID interno'''
    total = df.shape[0]
    count = df["internal_id"].isna().sum()
    percent = (count/total)*100
    percent = round(percent, 2)
    count = int(count)
    return count, total, percent    
    #dfData.loc[len(dfData)] = [count, total, string]
    #dfDescription.loc[len(dfDescription)] = ["Reportes sin Id interno"]

def GroupbyDispatchers(df : pd.DataFrame):
    '''Numero de reportes por despachador.'''
    DataFrameDispachers = df.groupby("dispatcher").count()
    DataFrameDispachers = DataFrameDispachers[['id']]
    DataFrameDispachers.rename(columns={"id": "N° Reportes"}, inplace=True)
    dfText = pd.DataFrame(DataFrameDispachers.index)
    newData = pd.DataFrame({'N° Reportes': ['N° Reportes']})
    DataFrameDispachers = pd.concat([newData, DataFrameDispachers], ignore_index=True)
    return dfText, DataFrameDispachers
    #exportar(dfText, DataFrameDispachers, 4, date) 

def ExtractStartEndDate(df: pd.DataFrame) -> str:
    '''Toma la hora de inicio y final de la data'''
    open = pd.to_datetime(df["timestamp_open"])
    close = pd.to_datetime(df["timestamp_close"])
    open = open.min().date().strftime("%d/%m")
    close = close.max().date().strftime("%d/%m")
    date = "Periodos: " + open + " - " + close
    return date

#monitor()

