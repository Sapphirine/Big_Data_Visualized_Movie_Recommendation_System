import pyspark.mllib
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def counts(rating_entry):
    num_ratings = len(rating_entry[1])
    return rating_entry[0], (num_ratings, float(sum(x for x in rating_entry[1]))/num_ratings)


class RecEngine:  
    def target_user_movies_ratings(self, target_user, selected_movies):
        movies_rdd= self.sc.parallelize(selected_movies).map(lambda item: (target_user, item))	
        ratings_final = self.model.predictAll(movies_rdd).map(lambda item: (item.product, item.rating))\
							 .join(self.movies).join(self.ratings_count)\
							 .map(lambda m: (m[1][0][1], m[1][0][0], m[1][1])).collect()
        return ratings_final
    
    def recommend_top_movies(self, target_user, number):
        raw_movies = self.ratings.filter(lambda rating: not rating[0] == target_user)\
                                                 .map(lambda x: (target_user, x[1])).distinct()
	ratings_final = self.model.predictAll(raw_movies).map(lambda item: (item.product, item.rating))\
						   .join(self.movies).join(self.ratings_count)\
						   .map(lambda m: (m[1][0][1], m[1][0][0], m[1][1]))\
						   .filter(lambda r: r[2]>=25).takeOrdered(number, key=lambda x: -x[1])
        return ratings_final

    def __init__(self, spark_content):
	logger.info("Start the Engine:")

	self.sc = spark_content
	logger.info("Start to Read ratings.csv ... ")
	raw_ratings = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/ratings.csv")
	#entry[0]: User ID; entry[1]: Movie ID; entry[2]: ratings
	first_line = raw_ratings.take(1)[0]
	self.ratings = raw_ratings.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), int(entry[1]), float(entry[2]))).cache()

	logger.info("Start to Read movies.csv ... ")
	raw_movies = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/movies.csv")
	#entry[0]: Movie ID; entry[1]: Title; entry[2]: Genere
	first_line = raw_movies.take(1)[0]
	self.movies = raw_movies.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1], entry[2])).cache()

	logger.info("Start to Count Movie ratings ...")
	self.ratings_count = self.ratings.map(lambda entry:(entry[1], entry[2])).groupByKey().map(counts).map(lambda x: (x[0], x[1][0]))
        logger.info("Train the ALS model ...")
        self.model = ALS.train(self.ratings, 8, seed=5L, iterations=10, lambda_=0.1)
        logger.info("Successfully build ALS model!")

	movieVectors = self.model.userFeatures.map(lambda (id, factor): (id, Vectors.dense(factor)))
	userVectors = self.model.productFeatures.map(lambda (id, factor): (id, Vectors.dense(factor)))






