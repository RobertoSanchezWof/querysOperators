####################################################################################
# Script para la obtención de datos de la base de datos de Firebase.
# Se obtienen los datos de los fuegos y de la historia de los fuegos.
# Se guardan en dos ficheros csv.q
# Se debe tener instalado el paquete de firebase-admin y pandas.
# Se debe tener el fichero de credenciales de Firebase.
####################################################################################

# Importamos las librerías necesarias
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import pandas as pd

def DownloadData(save: bool = False):
    # Inicializamos la conexión con la base de datos
    cred = credentials.Certificate('./credenciales/firebase.json')
    firebase_admin.initialize_app (cred)
    db : firestore.firestore.Client = firestore.client()

    # Definimos la fecha de inicio y termino de la consulta
    startDate = datetime(2023, 2, 1)
    endDate = datetime(2023, 2, 28)

    # Realizamos la consulta a la base de datos de fires
    fires = db.collection("fires")
    query = fires.order_by("times.time", direction=firestore.Query.DESCENDING)
    query = query.where("times.time", ">=", startDate)
    query = query.where("times.time", "<=", endDate)
    # Ejecutamos la consulta
    fichasFuegos = query.stream()
    dictFire = [docFire.to_dict() for docFire in fichasFuegos]

    # Realizamos la consulta a la base de datos de history
    history = db.collection("history")
    query2 = history.order_by("timestamp_open", direction=firestore.Query.DESCENDING)
    query2 = query2.where("timestamp_open", ">=", startDate)

    # Ejecutamos la consulta
    fichasHistory = query2.stream()
    dictHistory = [docHistory.to_dict() for docHistory in fichasHistory]

    # Pasamos los datos a un dataframe
    dfFires = pd.DataFrame(dictFire)
    dfHistory = pd.DataFrame(dictHistory)

    # Guardamos los datos en dos ficheros csv
    if save:
        dfFires.to_csv("fires.csv", sep="|")
        dfHistory.to_csv("history.csv", sep="|")
    
    return dfFires, dfHistory
