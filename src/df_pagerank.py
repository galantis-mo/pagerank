#!/usr/bin/env python3

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f
import time

schema = StructType([
    StructField("urlid", StringType(), True),
    StructField("urlchildren", StringType(), True)
])

def PageRank_DataFrame(nombre_iteration:int, input_path:str, output_dir:str, output_dir_time:str):
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

    start_time = time.time()
    ranks = ranks.repartition(sc.defaultParallelism, "urlid")

    # Calcul du pagerank
    for _ in range(nombre_iteration):
        contrib = exploded_df.join(ranks,"urlid").withColumn("rank", f.col("rank") / f.col("count"))

        ranks = contrib.groupBy("urlchildren").agg(f.sum("rank").alias("rankCount"))\
                    .withColumn("rank",f.col("rankCount") * 0.85 + 0.15)\
                    .select("urlchildren","rank")\
                    .withColumnRenamed("urlchildren","urlid")
        
    end_time = time.time()
    with open(output_dir_time, 'w') as f:
        f.write(f"time : {end_time - start_time} seconds for {nombre_iteration} iterations")

    # Sauvegarde des résultats
    ranks = ranks.sort(f.desc("rank"))
    ranks.coalesce(1).write.mode('overwrite').option('header', True).csv(output_dir)
    
    print('Pagerank written to', output_dir)
    spark.stop()
    
if __name__ == '__main__':
    if len(sys.argv) < 4 and not sys.argv[1].isdigit():
        print("Usage: df_pagerank.py <number_iterations:int> <input_path:str> <output_dir:str>")
        sys.exit(2)
    
    number_iterations = int(sys.argv[1])
    input_path = sys.argv[2]
    output_dir = sys.argv[3]
    output_dir_time = sys.argv[4]

    PageRank_DataFrame(number_iterations, input_path, output_dir, output_dir_time)