# Big Data · Tarea 3: análisis de hurtos con Spark y Kafka

Implementación académica en Python para explorar registros históricos de hurtos en Colombia y demostrar el procesamiento continuo de eventos simulados.

**Autor:** Rodolfo Parra · **Repositorio:** [rodolfop54/big-data-tarea-3-spark-kafka](https://github.com/rodolfop54/big-data-tarea-3-spark-kafka)

## Arquitectura

```text
BATCH
API de Datos Abiertos Colombia
  → descarga de hurtos.csv
  → HDFS: /Tarea3/hurtos.csv
  → Spark DataFrame
  → limpieza, transformación y análisis en consola

STREAMING
Productor Python: un evento JSON cada 2 segundos
  → Apache Kafka: hurto_eventos
  → Spark Structured Streaming
  → conteo acumulado por tipo_hurto en consola
```

La descarga y carga a HDFS se realizan con los comandos de esta guía. El programa batch comienza leyendo el archivo que ya está en HDFS. El productor genera eventos artificiales independientes del CSV histórico.

## Archivos

| Archivo | Función |
| --- | --- |
| [`analisis_hurtos.py`](analisis_hurtos.py) | Lectura desde HDFS, limpieza y ocho análisis con PySpark. |
| [`hurto_producer.py`](hurto_producer.py) | Generación aleatoria de eventos y envío a Kafka. |
| [`hurto_streaming_consumer.py`](hurto_streaming_consumer.py) | Consumo de JSON desde Kafka y agregación continua con Spark. |
| `README.md` | Preparación, ejecución, configuración y alcance. |

## Datos históricos

Fuente utilizada en la actividad: **Reporte Hurto por Modalidades Policía Nacional**, en Datos Abiertos Colombia, identificador `d4fr-sbn2`.

- [Ficha del conjunto de datos](https://www.datos.gov.co/d/d4fr-sbn2).
- [API de descarga CSV usada en el ejercicio](https://www.datos.gov.co/api/v3/views/d4fr-sbn2/query.csv).

El script espera estos encabezados exactos:

```text
departamento, municipio, armas_medios, fecha_hecho,
genero, grupo_etario, tipo_de_hurto, cantidad
```

`cantidad` debe ser numérica y `fecha_hecho` debe tener formato `dd/MM/yyyy`. Antes de ejecutar, revisa los encabezados del CSV descargado; si la fuente cambia sus nombres o formatos, será necesario adaptar el script. La fuente puede actualizarse, por lo que los totales dependen de la fecha de descarga.

## Requisitos y preparación

Los comandos están pensados para una máquina Linux o VM con Hadoop, Spark y Kafka previamente instalados. Se usa un entorno local de laboratorio con un broker Kafka.

| Componente | Configuración para estos ejemplos |
| --- | --- |
| Java | JDK 11 y `JAVA_HOME` configurado. |
| Python | Python 3.8–3.11, con `pip` y soporte para `venv`. |
| Apache Hadoop | HDFS configurado; NameNode y DataNode activos. URI usada: `hdfs://localhost:9000`. |
| Apache Spark | Spark 3.5.3 con Scala 2.12; `spark-submit` disponible en `PATH`. |
| Apache Kafka | Instalación 3.x con ZooKeeper para los comandos de arranque siguientes; broker en `localhost:9092`. |
| Paquete Python | `kafka-python` para el productor. PySpark lo proporciona la instalación de Spark al usar `spark-submit`. |
| Conector Spark–Kafka | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`, descargado por `--packages`. |
| Otras herramientas | Git, `curl` y acceso a Internet para descargar datos y dependencias. |

Comprueba las instalaciones:

```bash
java -version
python3 --version
hadoop version
spark-submit --version
```

Obtén el código y prepara el entorno del productor:

```bash
git clone https://github.com/rodolfop54/big-data-tarea-3-spark-kafka.git
cd big-data-tarea-3-spark-kafka
python3 -m venv .venv
.venv/bin/python -m pip install kafka-python
export KAFKA_HOME=/opt/Kafka
```

`/opt/Kafka` es la ruta utilizada en la actividad; ajusta `KAFKA_HOME` si tu instalación está en otra carpeta. Repite esa variable en cada terminal donde uses las herramientas de Kafka. Para Spark se utiliza el Python configurado en su instalación; no es necesario activar el entorno virtual del productor.

El ejemplo de envío con `--packages` requiere que la versión de Spark y el sufijo de Scala coincidan con el conector. Si usas otra distribución, ajusta esas coordenadas según tu versión instalada.

## Ejecución batch: API → HDFS → Spark

### 1. Comprobar HDFS

Ejecuta como el usuario de tu instalación Hadoop, con permiso de escritura en `/Tarea3`:

```bash
jps
hdfs getconf -confKey fs.defaultFS
hdfs dfs -ls hdfs://localhost:9000/
```

Si HDFS está detenido y tu instalación dispone de sus scripts estándar, inícialo con `start-dfs.sh`. No es necesario formatear el NameNode. El comando de Spark mostrado más adelante usa ejecución local y no requiere YARN.

### 2. Descargar y cargar los datos

Desde la carpeta del repositorio:

```bash
curl --fail --location \
  'https://www.datos.gov.co/api/v3/views/d4fr-sbn2/query.csv' \
  --output hurtos.csv
ls -lh hurtos.csv
head -n 5 hurtos.csv

hdfs dfs -mkdir -p hdfs://localhost:9000/Tarea3
hdfs dfs -put hurtos.csv hdfs://localhost:9000/Tarea3/
hdfs dfs -ls hdfs://localhost:9000/Tarea3
hdfs dfs -du -h hdfs://localhost:9000/Tarea3/hurtos.csv
```

`-put` conserva un archivo existente y falla si ya está cargado. Si deseas reemplazarlo con una nueva descarga, usa explícitamente `hdfs dfs -put -f hurtos.csv hdfs://localhost:9000/Tarea3/`.

### 3. Ejecutar el análisis

```bash
spark-submit --master 'local[*]' analisis_hurtos.py
```

El programa muestra el esquema, el número de filas y una muestra inicial. Después elimina duplicados, descarta nulos en campos esenciales, convierte la fecha y obtiene el año. Finalmente suma `cantidad` por:

1. Departamento: los diez valores más altos.
2. Municipio: los diez valores más altos.
3. Tipo de hurto.
4. Género.
5. Grupo etario.
6. Arma o medio utilizado.
7. Año del hecho.
8. Municipio dentro de `ATLÁNTICO`.

El número de filas (`count`) y la suma de `cantidad` son medidas diferentes. Los resultados se imprimen en consola; el script no exporta tablas ni gráficos. Las fechas que no se conviertan correctamente deben revisarse antes de interpretar la evolución anual. La limpieza no normaliza cadenas como `"NULL"` ni variantes de nombres geográficos.

Al terminar, espera en **«Presione ENTER para finalizar y cerrar Spark...»**. Mantén esa terminal abierta para inspeccionar Spark UI y presiona Enter cuando finalices.

## Ejecución streaming: Python → Kafka → Spark

### 1. Iniciar los servicios

Si los servicios ya están activos, continúa con la creación del tópico. Para la instalación Kafka 3.x con ZooKeeper utilizada en el laboratorio, inicia cada proceso en una terminal independiente:

```bash
# Terminal de ZooKeeper
/opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
```

```bash
# Terminal del broker, después de iniciar ZooKeeper
/opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
```

Estos comandos corresponden al modo ZooKeeper. Si tu broker ya funciona con KRaft, utiliza el arranque de tu instalación y continúa con el mismo tópico.

### 2. Crear y verificar el tópico

En otra terminal:

```bash
export KAFKA_HOME=/opt/Kafka
"$KAFKA_HOME/bin/kafka-topics.sh" --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic hurto_eventos

"$KAFKA_HOME/bin/kafka-topics.sh" --describe \
  --bootstrap-server localhost:9092 --topic hurto_eventos
```

### 3. Iniciar el consumidor de Spark

Desde la carpeta del repositorio, inicia el consumidor y espera a que la consulta esté activa:

```bash
spark-submit --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  hurto_streaming_consumer.py
```

El consumidor usa `startingOffsets=latest`: una consulta nueva comienza con los mensajes que lleguen a partir de su inicio. Por eso conviene dejarlo listo antes de lanzar el productor.

### 4. Iniciar el productor

En una terminal adicional, desde la misma carpeta:

```bash
.venv/bin/python hurto_producer.py
```

Cada dos segundos se envía un objeto con esta estructura; los valores son ilustrativos:

```json
{
  "departamento": "ATLÁNTICO",
  "municipio": "Barranquilla",
  "tipo_hurto": "HURTO ABIGEATO",
  "timestamp": 1792713600
}
```

`timestamp` contiene segundos Unix. Departamento y municipio se eligen de manera independiente, de modo que pueden aparecer combinaciones geográficas incoherentes. Los eventos sirven para demostrar el flujo técnico y no representan denuncias reales.

### 5. Observar y detener

El productor muestra `Enviado: {...}`. Spark interpreta el JSON y actualiza una tabla con `tipo_hurto` y `count`, usando modo `complete`. El conteo es acumulado durante la consulta; no utiliza ventanas temporales. Los tres tipos generados son abigeato, piratería terrestre y entidades financieras.

Detén primero el productor y luego el consumidor con `Ctrl+C` en sus terminales. El consumidor no configura un checkpoint persistente; no se garantiza conservar sus conteos al reiniciar. Kafka conserva los mensajes según la retención configurada en el broker.

## Verificación y solución de problemas

| Síntoma | Revisión |
| --- | --- |
| HDFS rechaza la conexión | Confirma NameNode/DataNode y que `file_path` en `analisis_hurtos.py` coincida con la URI real del NameNode. |
| Archivo o columna inexistente | Verifica `/Tarea3/hurtos.csv`, encabezados, separador CSV y formato de fecha. |
| `ModuleNotFoundError: kafka` | Instala `kafka-python` con `.venv/bin/python -m pip install kafka-python` y usa ese mismo intérprete para el productor. |
| Kafka no responde | Revisa ZooKeeper si aplica, el broker y el puerto con `ss -ltnp`; comprueba `localhost:9092` y los listeners del broker. |
| Spark no encuentra la fuente `kafka` | Ejecuta con `--packages` y revisa la descarga del conector y su coincidencia con Spark/Scala. |
| No aparecen eventos en Spark | Mantén el productor activo después de iniciar el consumidor; revisa que ambos usen `hurto_eventos`. |

Mientras la aplicación Spark esté activa, revisa Jobs y Stages en `http://localhost:4040`. Si hay otra aplicación, consulta el puerto anunciado por Spark, por ejemplo `4041`. Para acceder desde el equipo anfitrión, sustituye `localhost` por la IP de la VM y verifica la conectividad. La interfaz del NameNode suele estar en `http://localhost:9870` en Hadoop 3.

Las direcciones `localhost` de los scripts suponen que Spark, HDFS y Kafka se alcanzan desde la misma máquina del laboratorio. En un clúster distribuido se deben ajustar las direcciones y la configuración de ejecución.

## Referencias

- [Datos Abiertos Colombia: conjunto utilizado](https://www.datos.gov.co/d/d4fr-sbn2).
- [Apache Spark 3.5: integración de Structured Streaming con Kafka](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html).
- [Apache Kafka 3.5: inicio de servicios y tópicos](https://kafka.apache.org/35/getting-started/quickstart/).

Los scripts corresponden a los archivos originales de la actividad. La demostración completa requiere los servicios y datos descritos; publicar el código no sustituye la evidencia de ejecución en el laboratorio.
