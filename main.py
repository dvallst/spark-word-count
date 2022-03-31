from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == '__main__':
    # Crea un SparkContext local con 2 hilos
    sc = SparkContext("local[2]", "CuentaPalabras")
    # Crea un StreamingContext a partir del SparkContext con un tiempo de batch de 1 segundo
    ssc = StreamingContext(sc, 1)

    # Crea un DStream que se conectará a “localhost” y puerto 9999 con una conexión de tipo socket (nc -lk 9999)
    lineas = ssc.socketTextStream("localhost", 9999)

    # Separa cada línea en palabras
    palabras = lineas.flatMap(lambda linea: linea.split(" "))

    # Transforma el DStream "palabras" en un DStream "tuplas", con un valor de 1 para cada palabra
    tuplas = palabras.map(lambda palabra: (palabra, 1))

    # Aquí se contarán el número de palabras en modo StateLess
    contadorPalabras = tuplas.reduceByKey(lambda x, y: x + y)

    # Muestra por pantalla el número de veces que aparece cada palabra en el RDD
    contadorPalabras.pprint()

    ssc.start()  # Comienza el procesado
    ssc.awaitTermination()  # espera el fin del procesado
