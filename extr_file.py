import urllib
import os
import zipfile
from pyspark import SparkContext

complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
small_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'

datasets_path = os.path.join('/home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7', 'datasets')

complete_dataset_path = os.path.join(datasets_path, 'ml-latest.zip')
small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')

small_f = urllib.urlretrieve (small_dataset_url, small_dataset_path)
complete_f = urllib.urlretrieve (complete_dataset_url, complete_dataset_path)

with zipfile.ZipFile(small_dataset_path, "r") as z:
    z.extractall(datasets_path)

with zipfile.ZipFile(complete_dataset_path, "r") as z:
    z.extractall(datasets_path)

sc = SparkContext(appName="temp")

small_ratings_file = os.path.join(datasets_path, 'ml-latest-small', 'ratings.csv')

#small_ratings_raw_data = sc.textFile(small_ratings_file)
small_ratings_raw_data = sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/datasets/ml-latest-small/ratings.csv")
small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]

small_ratings_data = small_ratings_raw_data.filter(lambda line: line!=small_ratings_raw_data_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()

small_movies_file = os.path.join(datasets_path, 'ml-latest-small', 'movies.csv')

#small_movies_raw_data = sc.textFile(small_movies_file)
small_movies_raw_data = sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/datasets/ml-latest-small/movies.csv")
small_movies_raw_data_header = small_movies_raw_data.take(1)[0]

small_movies_data = small_movies_raw_data.filter(lambda line: line!=small_movies_raw_data_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1])).cache()
    
