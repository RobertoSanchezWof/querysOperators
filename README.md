# Querys Operators

Proyecto destinado a procesar las detecciones de incendio junto a su reporte anexado desde la plataforma operadores.

## Consideraciones:

- La data a tomar empieza entre el 1 de febrero y la fecha actual.
- La data queda almacenada en CSV para no requerir mas consultas en posteriores ejecuciones.
- Entry Point o punto de ejecución: __main__.py
- Se requieren credenciales para extraer la data de Firebase. Un archivo JSON que debe vivir en una carpeta llamada credenciales

## Instrucciones de uso

> 1.  (Opcional) Extraer data de firebase
>
> > Ejecutar archivo conexion.py
>
> 2.  (Opcional) Usar la data por defecto
>
> > Descomprimir operadores.zip
>
> 3.  Ejecutar consulas y analisis
>
> > Ejecutar python queryoperators.py
