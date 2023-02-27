import pandas as pd
import json

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
    return df, casosPresentados

def historyNotInFires():
    df = pd.read_csv("merge.csv", sep="_")
    df = df[df['_merge'] == 'right_only']
    casosPresentados = df.shape[0]
    return df, casosPresentados

def fireInHistory():
    df = pd.read_csv("merge.csv", sep="_")
    df = df[df['_merge'] == "both"]
    casosPresentados = df.shape[0]
    return df, casosPresentados

def reportBank(firesInHistory, firesNotInHistory):
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

def msjPromedio(firesInHistory):
    numeroMsg = []
    df = firesInHistory
    for element in df["events"]:
        count = element.count("state")
        numeroMsg.append(count)
    promedio = sum(numeroMsg)/len(numeroMsg)
    print(f"El promedio de mensajes por reporte es de {promedio:.2f}\n")

def promOpenClose(firesInHistory):
    tiempoProm = []
    df = firesInHistory
    df["timestamp_open"]=pd.to_datetime(df["timestamp_open"])
    df["timestamp_close"]=pd.to_datetime(df["timestamp_close"])
    for open, close in zip(df["timestamp_open"], df["timestamp_close"]):
        time = close - open
        tiempoProm.append(time)
    tiempoProm = pd.Series(tiempoProm)
    promedio = tiempoProm.mean()
    strPromedio = str(promedio)
    strPromedio = strPromedio.split(".")
    print(f"El tiempo promedio entre apertura y cierre es de {strPromedio[0]}\n")

def reportDispatcher():
    resultados = []
    df = pd.read_csv("merge.csv", sep="_")
    resultados.append(df["json_sended"].value_counts(dropna=False))
    for diccionario in resultados:
        for clave, valor in diccionario.items():
            if clave == True:
                print(f"Reportes de despacho: {valor}.\n")
            elif clave == False:
                print(f"Reportes que no son de despacho: {valor}.")
            else :
                print(f"Reportes sin información: {valor}.")

def reportForOperator():
    df = pd.read_csv("merge.csv", sep="_")
    lista = df.groupby("operator").count()
    print(f"Lista de reportes por operador\n{lista.fire_x}\n")

def reportNotInternalId():
    df = pd.read_csv("merge.csv", sep="_")
    count = df["internal_id"].isna().sum()
    #print(df["internal_id"].value_counts(dropna=False))
    print(f"la cantidad de reportes sin Id interno es de {count}\n")

def poolForDispatcher(firesInHistory):
    df = firesInHistory
    agrupado = df.groupby("dispatcher").count()
    print(f"Lista de historial por despachador\n{agrupado.id}\n")


# monitor()
dfFireInHistory, countFireInHistory = fireInHistory()
dfFireNotInHistory, countFireNotInHistory = firesNotInHistory()
dfHistoryNotInFire, countHistoryNotInFire = historyNotInFires()

print (f"La cantidad de fuegos con historial es de {countFireInHistory}.")
print(f"La cantidad de fuegos sin historial es de {countFireNotInHistory}.")
print(f"La cantidad de Historial sin fuegos es de {countHistoryNotInFire}.\n")
reportNotInternalId()
promOpenClose(dfFireInHistory)
reportDispatcher()
msjPromedio(dfFireInHistory)

reportBank(dfFireInHistory, dfFireNotInHistory)
reportForOperator()
poolForDispatcher(dfFireInHistory)