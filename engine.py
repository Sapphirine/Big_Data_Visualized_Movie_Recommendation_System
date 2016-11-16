import os
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def counts(rating_entry):
    num_ratings = len(rating_entry[1])
    return rating_entry[0], (num_ratings, float(sum(x for x in rating_entry[1]))/num_ratings)


class RecommendationEngine:
    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))	
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies).join(self.ratings_count)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        return predicted_rating_title_and_count_RDD
    
    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()
        return ratings
    
    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_rdd = self.ratings.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_rdd).filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])

        return ratings

    def __init__(self, spark_content):
	logger.info("Start the Engine:")


	self.sc = spark_content
	logger.info("Start to Read ratings.csv ... ")
	raw_ratings = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/movie_service/datasets/ml-latest-small/ratings.csv")
	#entry[0]: User ID; entry[1]: Movie ID; entry[2]: ratings
	first_line = raw_ratings.take(1)[0]
	self.ratings = raw_ratings.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), int(entry[1]), float(entry[2]))).cache()

	logger.info("Start to Read movies.csv ... ")
	raw_movies = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/movie_service/datasets/ml-latest-small/movies.csv")
	#entry[0]: Movie ID; entry[1]: Title; entry[2]: Genere
	first_line = raw_movies.take(1)[0]
	self.movies = raw_movies.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1], entry[2])).cache()

	logger.info("Start to Count Movie ratings ...")
	self.ratings_count = self.ratings.map(lambda entry:(entry[1], entry[2])).groupByKey().map(counts).map(lambda x: (x[0], x[1][0]))

        logger.info("Train the ALS model...")
        self.model = ALS.train(self.ratings, 8, seed=5L, iterations=10, lambda_=0.1)
        logger.info("Successfully build ALS model!")




