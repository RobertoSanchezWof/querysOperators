import pandas as pd
import json
import ast
from datetime import datetime


def monitor():
    '''Imprime 20 filas del merge'''
    df = pd.read_csv("data/merge.csv", sep="_")
    for x in list(df.columns):
        print(df[x].head(20))

def merge() :
    '''Une la data de Fire y History por el ID del incendio, lo guarda como CSV'''
    fire = pd.read_csv("fires.csv", sep="_")
    history = pd.read_csv("history.csv", sep="_")
    dfMerge = pd.merge(fire, history, how='left', left_on='id', right_on='fire', indicator=True)
    dfMerge.to_csv("data/merge.csv", sep="_")
    print(dfMerge)

def firesNotInHistory():
    '''Devuelve los datos que no posean registro. Y la cantidad de estos'''
    df = pd.read_csv("data/merge.csv", sep="_")
    df = df[df['_merge'] == 'left_only']
    casosPresentados = df.shape[0]
    return df, casosPresentados

def historyNotInFires():
    '''Devuelve los datos que no poseen incendios. Y su cantidad.'''
    df = pd.read_csv("data/merge.csv", sep="_")
    df = df[df['_merge'] == 'right_only']
    casosPresentados = df.shape[0]
    return df, casosPresentados

def fireInHistory():
    '''Devuelve los incendios que si poseen reporte, y su cantidad.'''
    df = pd.read_csv("data/merge.csv", sep="_")
    df = df[df['_merge'] == "both"]
    casosPresentados = df.shape[0]
    return df, casosPresentados

def reportBank(firesInHistory: pd.DataFrame, firesNotInHistory: pd.DataFrame):
    '''Entrega la información de los incendios y reportes agrupados por banco'''
    withReport = firesInHistory
    noReport = firesNotInHistory
    
    systemList = [x.replace("'", "\"") for x in withReport["system"].tolist()]
    diccionario = {i: json.loads(x) for i, x in enumerate(systemList)}
    dfNombres = pd.DataFrame.from_dict(diccionario, orient="index")
    
    systemListNoReport = [x.replace("'", "\"") for x in noReport["system"].tolist()]
    diccionarioNoReport = {i: json.loads(x) for i, x in enumerate(systemListNoReport)}
    dfNombresNoReport = pd.DataFrame.from_dict(diccionarioNoReport, orient="index")
    
    countsIn = dfNombres['system_name'].value_counts()
    countsNotIn = dfNombresNoReport['system_name'].value_counts()
    
    print(f"Lista de incendios con historial por cada banco:\n{countsIn}\n")
    print(f"Lista de incendios sin historial por cada banco:\n{countsNotIn}\n")

def msjPromedio(firesInHistory: pd.DataFrame):
    '''Promedia el numero de mensajes por reporte.'''
    numeroMsg = []
    df = firesInHistory
    for element in df["events"]:
        count = element.count("state")
        numeroMsg.append(count)
    promedio = sum(numeroMsg)/len(numeroMsg)
    print(f"El promedio de mensajes por reporte es de {promedio:.2f}\n")

# Funcion para extraer los numeros de la variable "time" usando regex.
# Ej:
# {'system_detection': False, 'ti2023, 2, 1, 0, 41, 'time': DatetimeWithNanoseconds(2023, 2, 1, 0, 41, 48, tzinfo=datetime.timezone.utc), 'log_time': DatetimeWithNanoseconds(2023, 2, 1, 0, 42, 4, tzinfo=datetime.timezone.utc), 'online_time': DatetimeWithNanoseconds(2023, 2, 1, 0, 42, 11, 887204, tzinfo=datetime.timezone.utc), 'atime': DatetimeWithNanoseconds(2023, 2, 1, 0, 40, 8, tzinfo=datetime.timezone.utc)}
def time(x):
    import re
    results = re.findall(r"'time': DatetimeWithNanoseconds\((.*?), tzinfo=datetime.timezone.utc\)", x)
    # results = [datetime.strptime(x, '%Y, %m, %d, %H, %M, %S') for x in results]
    print(results[0])
    return results


def funcion(x:str)->datetime:
    print(x)
    data = ''
    return data

def promReactionOpen(firesInHistory: pd.DataFrame):
    """promedia el tiempo de reacción entre detección y apertura """
    df = firesInHistory
    datetime(2023, 2, 1, 0, 41, 48)
    
    df["times"].apply(lambda x: time (x))
    #diccionario = df["times"].to_dict()
    #df['times'] = df['times'].apply(lambda x: ast.literal_eval(x))
    #df['times'] = df['times'].apply(lambda x: pd.Series(json.loads(x)).to_dict())
    #systemList = [x.replace("'", "\"") for x in df["times"].tolist()]
    #diccionario = json.dumps(systemList)
    #dfTimes = pd.read_json(diccionario)
    #df["times"].to_csv("time.csv",sep="_") 
    #print(diccionario.keys())
    #print(df["times"].to_dict())

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

    print(f"El tiempo promedio entre apertura y cierre es de {time_str}\n")

def reportDispatcher():
    '''Muestra los reportes que han sido despachados (JSON Uruguay)'''
    resultados = []
    df = pd.read_csv("data/merge.csv", sep="_")
    resultados.append(df["json_sended"].value_counts(dropna=False))
    for diccionario in resultados:
        for clave, valor in diccionario.items():
            if clave == True:
                print(f"Reportes de despacho: {valor}.\n")
            elif clave == False:
                print(f"Reportes que no son de despacho: {valor}.")
            else :
                print(f"Reportes sin información: {valor}.")
    #print (df.json_sended)

def reportForOperator():
    '''Reportes agrupados por operador.'''
    df = pd.read_csv("data/merge.csv", sep="_")
    lista = df.groupby("operator").count()
    print(f"Lista de reportes por operador\n{lista.fire_x}\n")

def reportNotInternalId():
    '''Numero de reportes sin ID interno'''
    df = pd.read_csv("data/merge.csv", sep="_")
    count = df["internal_id"].isna().sum()
    #print(df["internal_id"].value_counts(dropna=False))
    print(f"la cantidad de reportes sin Id interno es de {count}\n")

def poolForDispatcher(firesInHistory : pd.DataFrame):
    '''Numero de reportes por despachador.'''
    df = firesInHistory
    agrupado = df.groupby("dispatcher").count()
    print(f"Lista de historial por despachador\n{agrupado.id}\n")


#monitor()
dfFireInHistory, countFireInHistory = fireInHistory()
dfFireNotInHistory, countFireNotInHistory = firesNotInHistory()
dfHistoryNotInFire, countHistoryNotInFire = historyNotInFires()

# print (f"La cantidad de fuegos con historial es de {countFireInHistory}.")
# print(f"La cantidad de fuegos sin historial es de {countFireNotInHistory}.")
# print(f"La cantidad de Historial sin fuegos es de {countHistoryNotInFire}.\n")
# reportNotInternalId()
promReactionOpen(dfFireInHistory)
# promOpenClose(dfFireInHistory)
#reportDispatcher()
# msjPromedio(dfFireInHistory)

# reportBank(dfFireInHistory, dfFireNotInHistory)
# reportForOperator()
# poolForDispatcher(dfFireInHistory)