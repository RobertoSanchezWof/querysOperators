print("Ejecutando Queryoperators!")

from Downloader import DownloadData
from ReportAnalizer import *
from Uploader import exportar

def main():
    # Consulta si desea descargar los datos
    download = input("¿Desea descargar los datos?(y/n)[n]:")
    if download == "y":
        DownloadData(save=True)
        print("Datos descargados correctamente.")
    else:
        print("Datos no descargados.")
    # Consulta si desea analizar los datos
    analize = input("¿Desea analizar los datos?(y/n)[y]: ")
    if analize == "y" or analize == "":
        dfFires = pd.read_csv("data/fires.csv",sep="_")
        dfHistory = pd.read_csv("data/history.csv",sep="_")
        # Analisis unitarios
        [dfFiresWithReport,perFiresWithReport,dfFiresWithoutReport,dfReportsWithoutFire] = FiresReportsMerge(dfFires, dfHistory)
        print("Fuegos con reporte: ", dfFiresWithReport.shape[0])
        print("En porcentaje: ", perFiresWithReport)
        print("Fuegos sin reporte: ", dfFiresWithoutReport.shape[0])
        # dfFireNotInHistory = firesNotInHistory()
        # dfHistoryNotInFire = historyNotInFires()
        
        # Analisis de reportes
        [countRWOID,totalCountRWOID,perCountRWOID] =  CountReportsWithoutInternalID(dfFiresWithReport)
        PromReportTime(dfFiresWithoutReport)
        # promOpenClose(dfFireInHistory)
        # reportDispatcher()
        # msjPromedio(dfFireInHistory)

        # # Analisis de personal.
        # reportBank(dfFireInHistory, dfFireNotInHistory)
        # reportForOperator()
        # poolForDispatcher(dfFireInHistory)
        # print("Datos analizados correctamente.")
    else:
        print("Datos no analizados.")
    #Desea exportar los datos
    export = input("¿Desea exportar los datos?(y/n)[n]: ")
    if export == "y":
        exportar(dfDescription, dfData, 1, date)
        print("Datos exportados correctamente.")
    else:
        print("Datos no exportados")

if __name__ == "__main__":
    main()