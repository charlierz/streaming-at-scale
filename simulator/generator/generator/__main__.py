import os
import time
import datetime
import uuid
import json

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

executors = int(os.environ.get('EXECUTORS') or 1)
# rowsPerSecond = int(os.environ.get('EVENTS_PER_SECOND') or 1000)
# numberOfDevices = int(os.environ.get('NUMBER_OF_DEVICES') or 1000)
rowsPerSecond = 200
numberOfDevices = rowsPerSecond
temperatureDataCount = int(os.environ.get("TEMPERATURE_DATA_COUNT") or 66)
cellVoltageDataCount = int(os.environ.get("CELL_VOLTAGE_DATA_COUNT") or 80)
duplicateEveryNEvents = int(os.environ.get("DUPLICATE_EVERY_N_EVENTS") or 0)

outputFormat = os.environ.get('OUTPUT_FORMAT') or "kafka"
outputOptions = json.loads(os.environ.get('OUTPUT_OPTIONS') or "{}")
secureOutputOptions = json.loads(os.environ.get('SECURE_OUTPUT_OPTIONS') or "{}")

generate_uuid = F.udf(lambda : str(uuid.uuid4()), StringType())

spark = (SparkSession
  .builder
  .master("local[%d]" % executors)
  .appName("DataGenerator")
  .getOrCreate()
  )

stream = (spark
  .readStream
  .format("rate")
  .option("rowsPerSecond", rowsPerSecond)
  .load()
   )
# Rate stream has columns "timestamp" and "value"

stream = (stream
  .withColumn("dev", F.concat(F.lit("XYZ"), F.expr("mod(value, %d)" % numberOfDevices)))
  .withColumn("dsn", F.expr("value div %d" % numberOfDevices))
  .withColumn("mod", F.array(
    F.lit("REKAN"),
    F.lit("MIMIE"),
    F.lit("YUTON"),
    F.lit("RETWI"),
    F.lit("REZOE"),
    F.lit("NILEA"),
    F.lit("ORANG"),
    F.lit("BYDE5"),
    F.lit("BMWI3"),
    F.lit("AUETR"),
  ).getItem(
    (F.rand()*10).cast("int")
  ))
  .withColumn("partitionKey", F.col("dev"))
  .withColumn("eid", generate_uuid())
  # current_timestamp is later than rate stream timestamp, therefore more accurate to measure end-to-end latency
  .withColumn("ts", F.current_timestamp())
  .withColumn("cnt", F.round(F.rand()*10000000, 0))
  .withColumn("vol", F.round(F.rand()*400, 0))
  .withColumn("cur", F.round(F.rand()*40-20, 2))
  .withColumn("spe", F.round(F.rand()*200, 0))
  .withColumn("mas", F.round(F.rand()*200, 0))
  .withColumn("odo", F.round(F.rand()*500000, 0))
  .withColumn("soc", F.round(F.rand()*100, 1))
  .withColumn("map", F.round(F.rand()*100, 1))
  .withColumn("cap", F.round(F.rand()*100, 0))
  .withColumn("lat", F.round(F.rand()*100, 6))
  .withColumn("lon", F.round(F.rand()*-100, 6))
  .withColumn("acc", F.round(F.rand()*100, 0))
  .withColumn("bra", F.round(F.rand()*100, 0))
  .withColumn("miv", F.round(F.rand()*10, 2))
  .withColumn("mit", F.round(F.rand()*50, 2))
  .withColumn("mav", F.round(F.rand()*10, 2))
  .withColumn("mat", F.round(F.rand()*50, 2))
  .withColumn("sdf", F.expr("CASE WHEN rand()<0.5 THEN 0 ELSE 1 END"))
  .withColumn("sig", F.round(F.rand()*100, 0))
  .withColumn("gps", F.expr("CASE WHEN rand()<0.5 THEN 0 ELSE 1 END"))
  .withColumn("sat", F.round(F.rand()*20, 0))
  .withColumn("blf", F.expr("CASE WHEN rand()<0.5 THEN 0 ELSE 1 END"))
  .withColumn("sta", F.expr("CASE WHEN rand()<0.5 THEN 'OFF' ELSE (CASE WHEN rand()<0.5 THEN 'ON' ELSE 'CHA' END) END"))
  .withColumn("jou", F.current_timestamp())
  )

stream = stream.withColumn("cv", F.array([F.lit(F.round(F.rand()*10, 2)) for i in range(cellVoltageDataCount)]))
stream = stream.withColumn("ct", F.array([F.lit(F.round(F.rand()*50, 2)) for i in range(temperatureDataCount)]))

if duplicateEveryNEvents > 0:
 stream = stream.withColumn("repeated", F.expr("CASE WHEN rand() < {} THEN array(1,2) ELSE array(1) END".format(1/duplicateEveryNEvents)))
 stream = stream.withColumn("repeated", F.explode("repeated"))

if outputFormat == "eventhubs":
  bodyColumn = "body"
else: #Kafka format
  bodyColumn = "value"

query = (stream
  .selectExpr("to_json(struct(eid, dev, mod, dsn, ts, cv, ct, cnt, vol, cur, spe, mas, odo, soc, map, cap, lat, lon, acc, bra, miv, mit, mav, mat, sdf, sig, gps, sat, blf, sta, jou)) AS %s" % bodyColumn, "partitionKey")
  .writeStream
  .partitionBy("partitionKey")
  .format(outputFormat)
  .options(**outputOptions)
  .options(**secureOutputOptions)
  .option("checkpointLocation", "/tmp/checkpoint")
  .start()
  )

lastTimestamp = ""
nextPrintedTimestamp = time.monotonic()
lastPrintedTimestamp = 0
lastPrintedTimestampRows = 0
totalRows = 0
while (query.isActive):
  now = time.monotonic()
  for rp in query.recentProgress:
    if rp['timestamp'] > lastTimestamp:
      lastTimestamp = rp['timestamp']
      totalRows += rp['numInputRows']
  rps = (totalRows - lastPrintedTimestampRows) / (now - lastPrintedTimestamp)
  lastPrintedTimestamp = now
  nextPrintedTimestamp += 60
  if lastPrintedTimestamp > 0:
    print("%s %10.1f events/s" % (datetime.datetime.now().isoformat(), rps))
  lastPrintedTimestampRows = totalRows
  time.sleep(nextPrintedTimestamp - now)

print(query.exception())
