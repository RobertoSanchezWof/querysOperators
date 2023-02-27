import pandas as pd
import json
#from datetime import datetime

def monitor():
    df = pd.read_csv("merge.csv", sep="_")
    for x in list(df.columns):
        print(df[x].head(20))

def merge():
    fire = pd.read_csv("fires.csv", sep="_")
    history = pd.read_csv("history.csv", sep="_")
    dfMerge = pd.merge(fire, history, how='left', left_on='id', right_on='fire', indicator=True)
    dfMerge.to_csv("merge.csv", sep="_")
    print(dfMerge)

def firesNotInHistory():
    df = pd.read_csv("merge.csv", sep="_")
    df = df[df['_merge'] == 'left_only']
    casosPresentados = df.shape[0]
    print(f"se han presentado un total de {casosPresentados} casos de fuegos sin historial")
    df.to_csv("NotInHistory.csv", sep="_")
    return df

def historyNotInFires():
    df = pd.read_csv("merge.csv", sep="_")
    df = df[df['_merge'] == 'right_only']
    casosPresentados = df.shape[0]
    return df
    #print(f"se han presentado un total de {casosPresentados} casos de historial sin fuegos")

def fireInHistory():
    df = pd.read_csv("merge.csv", sep="_")
    df = df[df['_merge'] == "both"]
    casosPresentados = df.shape[0]
    return df
    #print(f"la cantidad de fuegos con historial es de {casosPresentados}")

def reportBank():
    df = pd.read_csv("merge.csv", sep="_")
    withReport = fireInHistory()
    noReport = firesNotInHistory()
    
    systemList = [x.replace("'", "\"") for x in df["system"].tolist()]
    diccionario = {i: json.loads(x) for i, x in enumerate(systemList)}
    dfNombres = pd.DataFrame.from_dict(diccionario, orient="index")
    uniqueKeys = dfNombres.drop_duplicates(subset=["api_key"])[["api_key", "system_name"]]
    
    mergeInHistory = withReport.merge(uniqueKeys, left_on='bank', right_on='api_key', how='left')
    mergeNotInHistory = noReport.merge(uniqueKeys, left_on='bank', right_on='api_key', how='left')
    
    countsIn = mergeInHistory['system_name'].value_counts()
    countsNotIn = mergeNotInHistory['system_name'].value_counts()
    
    print(f"Lista de incendios con reporte por cada banco:\n {countsIn}")
    print(f"Lista de incendios sin reporte por cada banco:\n {countsNotIn}")
    print(noReport.bank)

def msjPromedio():
    numeroMsg = []
    df = fireInHistory()
    for element in df["events"]:
        count = element.count("state")
        numeroMsg.append(count)
    promedio = sum(numeroMsg)/len(numeroMsg)
    print(f"el promedio de mensajes por reporte es de {promedio:.2f}")

def promOpenClose():
    tiempoProm = []
    df = fireInHistory()
    df["timestamp_open"]=pd.to_datetime(df["timestamp_open"])
    df["timestamp_close"]=pd.to_datetime(df["timestamp_close"])
    for open, close in zip(df["timestamp_open"], df["timestamp_close"]):
        time = close - open
        tiempoProm.append(time)
    tiempoProm = pd.Series(tiempoProm)
    promedio = tiempoProm.mean()
    strPromedio = str(promedio)
    strPromedio = strPromedio.split(".")
    print(f"el tiempo promedio entre apertura y cierre es de {strPromedio[0]}")

def reportDispatcher():
    resultados = []
    df = pd.read_csv("merge.csv", sep="_")
    resultados.append(df["json_sended"].value_counts(dropna=False))
    for diccionario in resultados:
        for clave, valor in diccionario.items():
            if clave == True:
                print(f"Reportes de despacho son: {valor}")
            elif clave == False:
                print(f"Reportes que no son de despacho son: {valor}")
            else :
                print(f"Reportes sin información son: {valor}")
    
def reportForOperator():
    df = pd.read_csv("merge.csv", sep="_")
    print(df["operator"].value_counts())
    
def reportNotInternalId():
    df = pd.read_csv("merge.csv", sep="_")
    count = df["internal_id"].isna().sum()
    print(df["internal_id"].value_counts(dropna=False))
    print(F"la cantidad de reportes sin Id interno es de {count}")

def poolForDispatcher():
    df = fireInHistory()
    agrupado = df.groupby("dispatcher").count()
    print(agrupado)


#monitor()
#reportBank()
#fireInHistory()
#firesNotInHistory()
#historyNotInFires()
#msjPromedio()
#promOpenClose()
#reportDispatcher()
#reportForOperator()
#reportNotInternalId()
#poolForDispatcher()