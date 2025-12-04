#!/usr/bin/env python3

import sys

from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f
import time

schema = StructType([
    StructField("urlid", StringType(), True),
    StructField("urlchildren", StringType(), True)
])

def PageRank_DataFrame(nombre_iteration:int, input_path:str, output_dir:str, project_id:str, bucket_name:str, time_path:str):
    start_time = time.time()
    spark = SparkSession.builder.appName("pagerank_df").getOrCreate()
    sc = spark.sparkContext

    # Lit le fichier CSV en utilisant le schema specifie 
    df = spark.read.option("header", False).option("delimiter", ";").option("quote", '"').schema(schema).csv(input_path)
    df = df.withColumn("urlchildren", f.split(df["urlchildren"], ' '))

    # Eclatement de la liste des enfants
    exploded_df = df.select("urlid", f.array_size(df.urlchildren).alias("count"), f.explode("urlchildren").alias("urlchildren"))
    exploded_df = exploded_df.repartition(sc.defaultParallelism, "urlid")

    # Initialisation des ranks
    ranks = exploded_df.select(f.col("urlid")).union(exploded_df.select(f.col("urlchildren").alias("urlid"))).distinct()
    ranks = ranks.withColumn("rank", f.lit(1.0))

    ranks = ranks.repartition(sc.defaultParallelism, "urlid")

    # Calcul du pagerank
    for _ in range(nombre_iteration):
        contrib = exploded_df.join(ranks,"urlid").withColumn("rank", f.col("rank") / f.col("count"))

        ranks = contrib.groupBy("urlchildren").agg(f.sum("rank").alias("rankCount"))\
                    .withColumn("rank",f.col("rankCount") * 0.85 + 0.15)\
                    .select("urlchildren","rank")\
                    .withColumnRenamed("urlchildren","urlid")
    
    # Calculer les résultats
    ranks = ranks.sort(f.desc("rank"))
    ranks = ranks.coalesce(1)
    end_time = time.time()
    
    # Sauvegarde du temps d'exécution
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(time_path)
    blob.upload_from_string("time,iterations\n{},{}".format(end_time - start_time, nombre_iteration))
    
    # Sauvegarde des résultats
    ranks.write.mode('overwrite').option('header', True).csv(output_dir)
    
    print('Pagerank written to', output_dir)
    spark.stop()
    
if __name__ == '__main__':
    if len(sys.argv) < 7 and not sys.argv[1].isdigit():
        print("Usage: df_pagerank.py <number_iterations:int> <input_path:str> <output_dir:str> <project_id:str> <bucket_name:str> <time_path:str>")
        sys.exit(2)
    
    number_iterations = int(sys.argv[1])
    input_path = sys.argv[2]
    output_dir = sys.argv[3]
    project_id = sys.argv[4]
    bucket_name = sys.argv[5]
    time_path = sys.argv[6]
    
    start_time = time.time()
    PageRank_DataFrame(number_iterations, input_path, output_dir, project_id, bucket_name, time_path)
    end_time = time.time()
