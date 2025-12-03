#!/usr/bin/env python3

import sys

from google.cloud import storage

from operator import add
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import time

def compute_contrib(children, rank):
    number_children = len(children)
    for c in children:
        yield c, rank / number_children

def csv_reader(s):
    elements = s.split(';')
    return (elements[0].strip('"'), elements[1].strip('"').split(' '))

def to_csv(data):
    return ','.join(str(d) for d in data)

def PageRank_RDD(nombre_iteration:int, input_path:str, output_dir:str, project_id:str, bucket_name:str, time_path:str):
    # Creation de l'application
    spark = SparkSession.builder.appName("pagerank_rdd").getOrCreate()
    sc = spark.sparkContext
    
    data = sc.textFile(input_path).map(csv_reader)

    start_time = time.time()
    # Partitionnement
    data = data.partitionBy(sc.defaultParallelism).persist()

    # Initial ranks: 1.0 pour chaque node présent dans adj keys ou values
    link_src = data.keys()
    link_dst = data.flatMap(lambda row: row[1])
    ranks = link_src.union(link_dst).distinct().map(lambda url:(url, 1.0))

    # Partitionnement
    ranks = ranks.partitionBy(sc.defaultParallelism).persist()
    ranks.count()

    # Calcul du page rank
    for _ in range(nombre_iteration):
        ranks = data.join(ranks)\
            .flatMap(lambda row: compute_contrib(row[1][0], row[1][1]))\
            .reduceByKey(add)\
            .mapValues(lambda rank : rank * 0.85 + 0.15)
    
    # Sauvegarde des résultats
    ranks = spark.createDataFrame(ranks, ['urlid', 'rank'])
    ranks = ranks.sort(f.desc("rank"))

    end_time = time.time()
    
    # Sauvegarde du temps d'exécution
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(time_path)
    blob.upload_from_string("time : {} seconds for {} iterations".format(end_time - start_time, nombre_iteration))

    ranks.coalesce(1).write.mode('overwrite').option('header', True).csv(output_dir)

    print('Pagerank written to', output_dir)
    spark.stop()
    
if __name__ == '__main__':
    if len(sys.argv) < 7 and not sys.argv[1].isdigit():
        print("Usage: rdd_pagerank.py <number_iterations:int> <input_path:str> <output_dir:str> <project_id:str> <bucket_name:str> <time_path:str>")
        sys.exit(2)
    
    number_iterations = int(sys.argv[1])
    input_path = sys.argv[2]
    output_dir = sys.argv[3]
    project_id = sys.argv[4]
    bucket_name = sys.argv[5]
    time_path = sys.argv[6]


    PageRank_RDD(number_iterations, input_path, output_dir, project_id, bucket_name, time_path)