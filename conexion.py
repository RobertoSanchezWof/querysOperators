import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import pandas as pd

cred = credentials.Certificate('./credenciales/firebase.json')
firebase_admin.initialize_app (cred)
db = firestore.client()

fecha = datetime(2023, 2, 1)

fires = db.collection("fires")
query = fires.where("times.time", ">=", fecha).order_by("times.time", direction=firestore.Query.DESCENDING)
fichasFuegos = query.stream()
dictFire = [docFire.to_dict() for docFire in fichasFuegos]

history = db.collection("history")
query2 = history.where("timestamp_open", ">=", fecha).order_by("timestamp_open", direction=firestore.Query.DESCENDING)
fichasHistory = query2.stream()
dictHistory = [docHistory.to_dict() for docHistory in fichasHistory]

dfFires = pd.DataFrame(dictFire)
dfHistory = pd.DataFrame(dictHistory)

dfFires.to_csv("fires.csv", sep="_")
dfHistory.to_csv("history.csv", sep="_")
    
