print("Ejecutando Queryoperators!")
from Downloader import DownloadData
from ReportAnalizer import *
from Uploader import exportar
def on_message(ws, message):
    print(message)
def on_error(ws, error):
    print(error)
def on_close(ws):
    print("### closed ###")

def main():
    # Descarga los datos    
    download = input("¿Desea descargar los datos?(y/n)[n]:")
    if download == "y":
        DownloadData(save=True)
        print("Datos descargados correctamente.")
        #Desea analizar los datos
    else:
        print("Datos no descargados.")
    analize = input("¿Desea analizar los datos?(y/n)[y]: ")
    if analize == "y" or analize == "":
        # Analiza los datos
        dfFireInHistory = fireInHistory()
        dfFireNotInHistory = firesNotInHistory()
        dfHistoryNotInFire = historyNotInFires()

        reportNotInternalId()
        promReactionOpen(dfFireInHistory)
        promOpenClose(dfFireInHistory)
        reportDispatcher()
        msjPromedio(dfFireInHistory)

        reportBank(dfFireInHistory, dfFireNotInHistory)
        reportForOperator()
        poolForDispatcher(dfFireInHistory)

        exportar(dfDescription, dfData, 1, date)
        print("Datos analizados correctamente.")
    else:
        print("Datos no analizados.")
    #Desea exportar los datos
    export = input("¿Desea exportar los datos?(y/n)[n]: ")
    if export == "y":
        exportar()
        print("Datos exportados correctamente.")

if __name__ == "__main__":
    main()