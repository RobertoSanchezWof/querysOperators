from datetime import datetime
import locale

locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

fecha = datetime.strptime('1 de febrero de 2023', '%d de %B de %Y')
print(fecha)
