import pandas as pd
import unicodedata
import uuid
from cassandra.cluster import Cluster

#Conectar con el clúster de Cassandra.
cluster = Cluster(['0.0.0.0'], port=9042)
session = cluster.connect('practica2')

#Leer el archivo csv.
df = pd.read_csv('./books_data/books.csv', nrows=100, on_bad_lines='skip', encoding='latin-1', sep=';')
df = df.reset_index()

# Función para eliminar tildes.
def eliminar_tildes(texto):
    texto = str(texto)
    texto = texto.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    return texto

#Reemplazar tildes.
for columna in df.columns:
    df[columna] = df[columna].apply(eliminar_tildes)

#Para cada fila, insertamos los registros utilizando cqlengine.
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

# nsertar datos en las tablas.
insert_libro_query = session.prepare("INSERT INTO libros (isbn, titulo, anio_edicion) VALUES (?, ?, ?)")
insert_autor_query = session.prepare("INSERT INTO autores (id_autor, nombre_autor) VALUES (?, ?)")
insert_autor_isbn_query = session.prepare("INSERT INTO autor_isbn (id_autor, isbn) VALUES (?, ?)")

#Crear un diccionario para mapear autores a IDs únicos.
autor_id_dict = {}
autor_id = 1  #Inicializar el ID único.

# Iterar a través de la columna "Autor" en el DataFrame
for autor in df['Book-Author']:
    if autor not in autor_id_dict:
        # Si el autor no está en el diccionario, asignarle un nuevo ID único
        autor_id_dict[autor] = autor_id
        autor_id += 1

#Crear una nueva columna.
df['Autor_ID'] = df['Book-Author'].map(autor_id_dict)

for row in range(len(df)):
    valores=df.loc[row]
    session.execute(insert_libro_query, (valores['ISBN'], valores['Book-Title'], valores['Year-Of-Publication']))
    session.execute(insert_autor_query, (valores['Autor_ID'], valores['Book-Author']))
    session.execute(insert_autor_isbn_query, (valores['Autor_ID'], valores['ISBN']))

#Cerrar la conexión con el clúster de Cassandra.
cluster.shutdown()
