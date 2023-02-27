# instrucciones de uso

Para extraer data de firebase ejecuta en tu compilador de código el archivo conexion.py el cual extraerá información desde el 1 de febrero del 2023 hasta la fecha actual
generando asi 2 archivos fires.csv y history.csv, la data ya esta incluida en el archivo plataforma operadores.zip la cual hay que descomprimir en el mismo lugar donde están  conexion.py y leercsv.py para su inmediata lectura.
Para hacer las consultas a la data extraída ejecutar el archivo leercsv.py, la información entregada esta separada por funciones: 

1. fireInHistory: Entrega los incendios que si tienen historial asociado.
2. firesNotInHistory: Entrega los incendios que no tienen historial asociado.
3. historyNotInFires: Entrega os historial que no tienen asociado un incendio.
4. reportNotInternalId: Entrega los historiales que no tienen un "internal_id".
5. promOpenClose: Entrega el tiempo promedio entre apertura y cierre de un fuego.
6. reportDispatcher: Entrega la cantidad de reportes que son de despacho, los que no son y los que no tienen información.
7. msjPromedio: Entrega la cantidad de mensajes promedio por reporte.
8. reportBank: Entrega una lista con la cantidad de reportes por banco.
9. reportForOperator: Entrega la cantidad de reportes por operador.
10. poolForDispatcher: Entrega la cantidad de reportes por despachador.