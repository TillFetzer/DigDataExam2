#!/usr/bin/env python
# coding: utf-8



# Initialize SparkContext and SparkSession
import findspark, os
findspark.init('/home/hadoop/spark')
import pyspark
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setMaster("yarn").setAppName("Jupyter PySpark Test")
sc = pyspark.SparkContext(conf = conf)
spark = SparkSession(sc)



json_file = spark.read.json("/Path/To/JSONfile.json")

# Read title.basics.tsv into Spark dataframe
imdb_title_basics_dataframe = spark.read.format('csv').options(    header='true', delimiter='\t', nullValue='null', inferschema='true')    .load('/user/hadoop/imdb/title_basics/title.basics.tsv')





imdb_title_basics_dataframe.printSchema() # Print Schema of title_basics dataframe 





imdb_title_basics_dataframe.show(5) # Show first 5 rows of title_basics dataframe 





imdb_title_basics_dataframe.count() # show number of rows within title_basics dataframe 



# Get column titleTypes values with counts and ordered descending
from pyspark.sql.functions import desc
imdb_title_basics_dataframe.groupBy("titleType").count().orderBy(desc("count")).show()



# Calculate average Movie length in minutes
from pyspark.sql.functions import avg
imdb_title_basics_dataframe.filter(imdb_title_basics_dataframe['titleType']=='movie')    .agg(avg('runtimeMinutes')).show() 





# Read title.ratings.tsv into Spark dataframe
imdb_title_ratings_dataframe = spark.read.format('csv').options(    header='true', delimiter='\t', nullValue='null', inferschema='true')    .load('/user/hadoop/imdb/title_ratings/title.ratings.tsv')




imdb_title_ratings_dataframe.printSchema() # Print Schema of title_ratings dataframe 




imdb_title_ratings_dataframe.show(5) # Show first 5 rows of title_ratings dataframe 



title_basics_and_ratings_df = imdb_title_basics_dataframe.join(imdb_title_ratings_dataframe,                             imdb_title_basics_dataframe.tconst == imdb_title_ratings_dataframe.tconst)



top_tvseries=title_basics_and_ratings_df.filter(title_basics_and_ratings_df['titleType']=='tvSeries')                            .filter(title_basics_and_ratings_df['numVotes'] > 200000)                            .orderBy(desc('averageRating'))                            .select('originalTitle', 'startYear', 'endYear', 'averageRating', 'numVotes')
top_tvseries.show(5)




top_tvseries.write.format('parquet')                    .partitionBy('startYear')                    .mode('overwrite')                    .save('/user/hadoop/imdb/top_tvseries')





good_movies_df = title_basics_and_ratings_df.filter(title_basics_and_ratings_df['titleType']=='movie')                            .filter(title_basics_and_ratings_df['numVotes'] > 200000)                            .filter(title_basics_and_ratings_df['startYear'] > 1990)                            .groupBy('startYear')                            .count()                            .orderBy('startYear')
good_movies_df.show(5)





import matplotlib.pyplot as plt
import pandas
pandas_dataframe = good_movies_df.select('startYear', 'count').toPandas()
pandas_dataframe.plot.bar(x='startYear', y='count')




title_basics_and_ratings_df.select('originalTitle', 'titleType', 'startYear',                                    'endYear', 'numVotes', 'averageRating')                            .write.saveAsTable('movies_and_ratings')



result_df = spark.sql("""SELECT originalTitle, averageRating FROM movies_and_ratings WHERE 
                        numVotes > 200000 AND titleType= 'movie' AND averageRating > 8.5 AND startYear > 2010 
                        ORDER BY averageRating DESC LIMIT 10"""
                     ).show(10)


