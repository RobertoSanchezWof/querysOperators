print("Ejecutando Queryoperators!")

from Downloader import DownloadData
from ReportAnalizer import *
from Uploader import Export, ExportDate, ExportHeaders, credenciales

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
        [dfFiresWithReport, perFiresWithReport, dfFiresWithoutReport, perFireswithoutReport, dfReportsWithoutFire, perReportswithoutFire, total] = FiresReportsMerge(dfFires, dfHistory)
        valueDfFiresWithReport = dfFiresWithReport.shape[0]
        valueDfFiresWithoutReport = dfFiresWithoutReport.shape[0]
        valueReportsWithoutFire = dfReportsWithoutFire.shape[0]
        #print("Fuegos con reporte: ", dfFiresWithReport.shape[0])
        #print("En porcentaje: ", perFiresWithReport)
        #print("Fuegos sin reporte: ", dfFiresWithoutReport.shape[0])
        #print("En porcentaje: ", perFireswithoutReport)
        #print("reportes sin fuego: ", dfReportsWithoutFire.shape[0])
        #print("En porcentaje: ", perReportswithoutFire)
        
        # Analisis de reportes
        [countRWOID, totalCountRWOID, perCountRWOID] =  CountReportsWithoutInternalID(dfFiresWithReport)
        timePromReaction = AverageReportActionTimes(dfFiresWithReport)
        averageOPT = AverageOpenCloseTimes(dfFiresWithReport)
        reportUruguay = UruguayDispatchedReports(dfFiresWithReport)
        msgProm = AverageMessageCount(dfFiresWithReport)
        #print("Reportes sin Internal ID: ", countRWOID)
        #print("Total: ", totalCountRWOID)
        #print("Porcentaje: ", perCountRWOID)
        #print("Tiempo promedio de reacción: ", timePromReaction)
        #print("Tiempo promedio de vida: ", averageOPT)
        #print("Reportes despachados para Uruguay: ", reportUruguay)
        #print("Mensaje promedio por informe: ", msgProm)

        # Analisis de personal.
        [categoryBank, dataBank] = GroupByBank(dfFiresWithReport, dfFiresWithoutReport)
        [categoryOperator, dataOperator] = GroupByOperators(dfFiresWithReport)
        [categoryDispatchers, dataDispatchers] = GroupbyDispatchers(dfFiresWithReport)
        print("Datos analizados correctamente.")
    else:
        print("Datos no analizados.")
    #Desea exportar los datos
    export = input("¿Desea exportar los datos?(y/n)[n]: ")
    if export == "y":
        excelSheet = credenciales()
        date = ExtractStartEndDate(dfFiresWithReport)
        #pagina 1
        #exporta la fecha y la hoja a la que se va a exportar
        ExportDate(date, 0, excelSheet)
        #exporta las cabeceras, la pagina y la fila
        ExportHeaders(["Valor", "Total", "Porcentaje"],0 , 2, excelSheet)
        #Export se compone por: nombre, pagina, fila, array de datos unitarios o dataframe
        Export("Fuegos con Reporte" ,0 ,3, excelSheet, [valueDfFiresWithReport, total, perFiresWithReport])
        Export("Fuegos sin reporte" ,0 ,4,excelSheet , [valueDfFiresWithoutReport, total, perFireswithoutReport])
        Export("reportes sin Fuegos",0 ,5 ,excelSheet , [valueReportsWithoutFire, total, perReportswithoutFire])
        Export("Reportes sin Internal ID", 0, 7,excelSheet , [countRWOID, totalCountRWOID, perCountRWOID])

        Export("Tiempo promedio de reacción", 0, 9,excelSheet , [timePromReaction])
        Export("Tiempo promedio de vida", 0, 10,excelSheet , [averageOPT])
        Export("Reportes despachados para Uruguay", 0, 11,excelSheet , [reportUruguay])
        Export("Mensaje promedio por informe", 0, 12,excelSheet , [msgProm])
        
        #Pagina 2
        Export(categoryBank, 1, 2,excelSheet , dataBank)
        ExportDate(date, 1, excelSheet)
        #Pagina 3
        Export(categoryOperator,2, 2,excelSheet , dataOperator)
        ExportDate(date, 2, excelSheet)
        #Pagina 4
        Export(categoryDispatchers,3, 2,excelSheet , dataDispatchers)
        ExportDate(date, 3, excelSheet)
        print("Datos exportados correctamente.")
    else:
        print("Datos no exportados")

if __name__ == "__main__":
    main()