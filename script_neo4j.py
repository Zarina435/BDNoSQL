from neo4j import GraphDatabase
import pandas as pd

#Conectar.
uri = "bolt://localhost:7687" 
username = "neo4j"
password = "master22"

df = pd.read_csv('./books_data/books.csv',nrows=100, on_bad_lines='skip', encoding='latin-1', sep=';') 

#Función para eliminar tildes.
def eliminar_tildes(texto):
    texto = str(texto)
    texto = texto.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    return texto

#Reemplazar tildes.
for columna in df.columns:
    df[columna] = df[columna].apply(eliminar_tildes)

#Cargar datos en neo4j.
def cargar_datos(tx, isbn, titulo, anio_edicion, id_autor, nombre_autor):
    tx.run("""
        MERGE (libro:Libro {isbn: $isbn})
        ON CREATE SET libro.titulo = $titulo, libro.anio_edicion = $anio_edicion

        MERGE (autor:Autor {id_autor: $id_autor})
        ON CREATE SET autor.nombre_autor = $nombre_autor

        MERGE (autor)-[:ESCRIBIO]->(libro)
        MERGE (autor_isbn:IsbnAutor {isbn: $isbn, id_autor: $id_autor})
    """, isbn=isbn, titulo=titulo, anio_edicion=anio_edicion, id_autor=id_autor, nombre_autor=nombre_autor)

#Crear un diccionario para mapear autores a IDs únicos.
autor_id_dict = {}
autor_id = 1  #Inicializar el ID único.

#Para añadir IDs únicos.
for autor in df['Book-Author']:
    if autor not in autor_id_dict:
        # Si el autor no está en el diccionario, asignarle un nuevo ID único
        autor_id_dict[autor] = autor_id
        autor_id += 1

#Crear una nueva columna.
df['Autor_ID'] = df['Book-Author'].map(autor_id_dict)

#Crear restricciones de unicidad.
with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session() as session:
        session.run("CREATE CONSTRAINT ON (libro:Libro) ASSERT libro.isbn IS UNIQUE")
        session.run("CREATE CONSTRAINT ON (autor:Autor) ASSERT autor.id_autor IS UNIQUE")

#Cargar datos en Neo4j.
with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session() as session:
        for row in range(len(df)):
            valores=df.loc[row]
            session.write_transaction(cargar_datos, valores['ISBN'], valores['Book-Title'], valores['Year-Of-Publication'], valores['Autor_ID'], valores['Book-Author'])