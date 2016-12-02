from pyspark.mllib.linalg import Vectors
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.clustering import KMeans
import logging
import sys
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def counts(rating_entry):
    num_ratings = len(rating_entry[1])
    return rating_entry[0], (num_ratings, float(sum(x for x in rating_entry[1]))/num_ratings)



class RecEngine:  
    def cluster_ratings(self):
	reload(sys)
	sys.setdefaultencoding("utf8")
	rdd_action = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster/Action")
	rdd_comedy = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster/Comedy")
	rdd_drama = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster/Drama")
	rdd_docmen = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster/Documentary&Adventure")
	rdd_horror = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster/Horror&Thriller")
	rdd_ratings = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/averge_ratings_sorted")
	first_line = rdd_ratings.take(1)[0]
	ratings = rdd_ratings.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), float(entry[2]))).cache()

	first_line = rdd_action.take(1)[0]
	action = rdd_action.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1])).cache()

	first_line = rdd_comedy.take(1)[0]
	comedy = rdd_comedy.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1])).cache()

	first_line = rdd_drama.take(1)[0]
	drama = rdd_drama.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1])).cache()

	first_line = rdd_docmen.take(1)[0]
	docmen = rdd_docmen.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1])).cache()

	first_line = rdd_horror.take(1)[0]
	horror = rdd_horror.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1])).cache()
	
	def cov(rdd):
		id, (title, ratings) = rdd
		return id, title, ratings
	#action_final = list(action.join(ratings).map(cov).sortBy(lambda x:x[2], False).collect())
	action_temp = action.join(ratings).map(cov).sortBy(lambda x:x[2], False)
	action_sum = action_temp.map(lambda x:x[2]).sum()
	action_count =  action_temp.map(lambda x:x[2]).count()
	action_average = action_sum / action_count
	action_final = list(action_temp.collect())
	print(action_average)
	f=file("action_ratings_sort", "w+")
	f.write("movieId,title,ratings\n")
	for x in action_final:
		f.write(str(x[0])+","+str(x[1])+","+str(x[2])+"\n")
	f.close()

	#comedy_final = list(comedy.join(ratings).map(cov).sortBy(lambda x:x[2], False).collect())
	comedy_temp = comedy.join(ratings).map(cov).sortBy(lambda x:x[2], False)
	comedy_sum = comedy_temp.map(lambda x:x[2]).sum()
	comedy_count =  comedy_temp.map(lambda x:x[2]).count()
	comedy_average = comedy_sum / comedy_count
	print(comedy_average)
	comedy_final = list(comedy_temp.collect())
	f=file("comedy_ratings_sort", "w+")
	f.write("movieId,title,ratings\n")
	for x in comedy_final:
		f.write(str(x[0])+","+str(x[1])+","+str(x[2])+"\n")
	f.close()

	#drama_final = list(drama.join(ratings).map(cov).sortBy(lambda x:x[2], False).collect())
	drama_temp = drama.join(ratings).map(cov).sortBy(lambda x:x[2], False)
	drama_sum = drama_temp.map(lambda x:x[2]).sum()
	drama_count =  drama_temp.map(lambda x:x[2]).count()
	drama_average = drama_sum / drama_count
	print(drama_average)
	drama_final = list(drama_temp.collect())
	f=file("drama_ratings_sort", "w+")
	f.write("movieId,title,ratings\n")
	for x in drama_final:
		f.write(str(x[0])+","+str(x[1])+","+str(x[2])+"\n")
	f.close()
	
	#def wired_cov(rdd):
	#	(movid, title, rating) = rdd
	#	return rating
	#docmen_final = list(docmen.join(ratings).map(cov).sortBy(lambda x:x[2], False).collect())
	docmen_temp = docmen.join(ratings).map(cov).sortBy(lambda x:x[2], False)
	docmen_sum = docmen_temp.map(lambda x:x[2]).sum()
	docmen_count =  docmen_temp.map(lambda x:x[2]).count()
	docmen_average = docmen_sum / docmen_count
	print(docmen_average)
	docmen_final = list(docmen_temp.collect())
	f=file("docmen_ratings_sort", "w+")
	f.write("movieId,title,ratings\n")
	for x in docmen_final:
		f.write(str(x[0])+","+str(x[1])+","+str(x[2])+"\n")
	f.close()

	#horror_final = list(horror.join(ratings).map(cov).sortBy(lambda x:x[2], False).collect())
	horror_temp = horror.join(ratings).map(cov).sortBy(lambda x:x[2], False)
	horror_sum = horror_temp.map(lambda x:x[2]).sum()
	horror_count =  horror_temp.map(lambda x:x[2]).count()
	horror_average = horror_sum / horror_count
	print(horror_average)
	horror_final = list(horror_temp.collect())
	f=file("horror_ratings_sort", "w+")
	f.write("movieId,title,ratings\n")
	for x in horror_final:
		f.write(str(x[0])+","+str(x[1])+","+str(x[2])+"\n")
	f.close()

	f=file("averge_ratings","w+")
	f.write("action movies average: " + str(action_average) + "\n")
	f.write("comedy movies average: " + str(comedy_average) + "\n")
	f.write("drama movies average: " + str(drama_average) + "\n")
	f.write("docmenentary&adventure movies average: " + str(docmen_average) + "\n")
	f.write("horror&thiller movies average: " + str(horror_average) + "\n") 
	
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
	
    def ratings_new_user(self, file):
	new_ratings = self.sc.textFile(file)
	new_data = new_ratings.map(lambda temp: temp.split(","))\
		   .map(lambda entry: (int(entry[0]), int(entry[1]), float(entry[2]))).cache()
	self.ratings = self.ratings.union(new_data)
	count_total = self.ratings.map(lambda entry:(entry[1], entry[2])).groupByKey().map(counts).map(lambda x: (x[0], x[1][0]))
	self.model = ALS.train(self.ratings, 8, seed=5L, iterations=10, lambda_=0.1)

    def get_tags(self, final_score, key):
	def func5(rdd):
		tagId, movieId, relevance = rdd
		return tagId, relevance
	def func6(rdd):
		tagId, (movieId, tag) = rdd
		return tagId, tag
	def func7(rdd):
		tagId, (tag, relevance) = rdd
		return tagId, tag, relevance
	def func8(rdd):
		tempk, tempv = rdd
		return tempv
	#for k, v in final_score.collect():
	#	print(k)
	#	print(v)
	#	print("=========================")	
	value = final_score.collect()[key][1]
	#print(value)
	temp=list(value)
	#print(str(temp))
	temp.sort(key=lambda x:x[2], reverse=True)
	temp=temp[:100]
	#tempRdd=spark_content
	tempRdd=self.sc.parallelize(temp)
	kickmovie=tempRdd.map(func5)
	print(kickmovie.first())
	tempRdd=tempRdd.join(self.tags).map(func6)
	tempRdd=tempRdd.join(kickmovie)		
	print(tempRdd.first())
	temp=tempRdd.takeOrdered(100, key=lambda x:-x[1][1])
	#name = "movie_"+str(key)
	#f=file(name+"_test", "w+")
	final_string=''
	for x in temp:
			#print(x)
			#f.write(str(x)+"\n")
		final_string=final_string+str(x[1][0]) + ":"+str(x[1][1])+"\n"
		#print(str(x[1][0]) + ":"+str(x[1][1])+"\n")
	print(final_string)
	
	return final_string

    def movie_cluster_func(self, movie_assigned, key):
	#for k, v in movie_assigned.collect():
	#	print(k)
	#	print(v)
	value = movie_assigned.collect()[key][1]
	#print(value)
	#print("Movie Cluster: "+ str(key))	
	temp = list(value)
	temp.sort(key=lambda x:x[3])
	#write into a file
	name = "Cluster_" + str(key)
	#f=file(name+"_test", "w+")
	#f.write("movieId,title,genres,distance\n")
	final_string="movieId,title,genres,distance\n"
	for x in temp:
		final_string=final_string+str(x[1])+","+str(x[2])+","+str(x[3])+"\n"
		#f.write(str(x[1])+","+str(x[2])+","+str(x[3])+"\n")
	#print(final_string)
	#f.close()
	#print("=====================================================================================================================")
	return final_string

    def user_cluster_func(self, user_assigned, key):
	#for k, v in movie_assigned.collect():
	#	print(k)
	#	print(v)
	value = user_assigned.collect()[key][1]
	#print(value)
	#print("Movie Cluster: "+ str(key))	
	temp = list(value)
	temp.sort(key=lambda x:x[3])
	#write into a file
	name = "Cluster_" + str(key)
	#f=file(name+"_test", "w+")
	#f.write("movieId,title,genres,distance\n")
	final_string="userId,movieId,title,genres,distance\n"
	for x in temp:
		final_string=final_string+str(x[1])+","+str(x[2])+","+str(x[3])+"\n"
		#f.write(str(x[1])+","+str(x[2])+","+str(x[3])+"\n")
	print(final_string)
	#f.close()
	#print("=====================================================================================================================")
	return final_string
		
    def get_average(self, movie_id):
	#average = list(self.aver_ratings.groupBy(lambda x:x[0]).collect())
	#.collect()[movie_id])
	#f=file("test_aver_rating","w+")
	#f.write(str(self.aver_ratings.collect()))
	#f.close()
	#average = list(self.aver_ratings.groupByKey().collect()[3])[0]
	average = self.aver_ratings.collect()
	for x in average:
		if x[0] == movie_id:
			aver = x[1]
			break
	#[movie_id][1][0]
	#average_id = self.aver_ratings.join(self.aver_ratings).collect()[movie_id][0]
	print(aver)
	return aver
	
    def get_movie_sort(self):
	aver_list = self.movies.join(self.aver_ratings).sortBy(lambda x:x[1][1], False).collect()
	f=file("averge_ratings_sorted", "w+")
	f.write("movie_id,title,aver_ratings\n")
	for x in aver_list:
		f.write(str(x[0])+","+str(x[1][0])+","+str(x[1][1])+"\n")
	f.close()
		
    #def get_top_ten(self):
	#top_ten_list = self.aver_ratings.sortBy(lambda x:x[]
    def __init__(self, spark_content):
	reload(sys)
	sys.setdefaultencoding("utf8")
	
	logger.info("Start the Engine:")
	######################################################
	logger.info("Start to read tags information ...")
	self.sc = spark_content
	self.cluster_ratings()
	raw_scores = self.sc.textFile("file:///home/bjt/Downloads/ml-latest/genome-scores.csv")
	#def func4(rdd):
	#	movie_id, tag_id, relevance = rdd
	#	return int(tag_id), int(movie_id), float(relevance)
	first_line = raw_scores.take(1)[0]
	self.scores = raw_scores.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[1]), int(entry[0]), float(entry[2]))).cache()
	
	raw_tags = self.sc.textFile("file:///home/bjt/Downloads/ml-latest/genome-tags.csv")
	first_line = raw_tags.take(1)[0]
	#tages: tagid, tag
	self.tags = raw_tags.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1]))
	#final_list = list(self.scores.toLocalIterator).sort(key=lambda x:x[2])
	#self.scores.takeOrderded(key=lambda x:x[2])
	self.final_score = self.scores.groupBy(lambda x:x[1])
	logger.info("Successfully load tags information!")
	#self.get_tags(self.final_score, 1)
	#print(final_score.collect())
	'''
	for key, value in movie_assigned.collect():
		print("=====================================================================================================================")
		print("Cluster: "+ key)

		print("Top Twenty Movies:\n")
		
		temp = list(value)
		temp.sort(key=lambda x:x[3])
		#write into a file
		name = "Cluster_" + key
		f=file("/home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/movie_cluster/" + name, "w+")
		f.write(str(temp))
		f.close()
		for i in range(20):
			print(temp[i])
	print("=====================================================================================================================")
	'''
	
	def func5(rdd):
		tagId, movieId, relevance = rdd
		return tagId, relevance
	def func6(rdd):
		tagId, (movieId, tag) = rdd
		return tagId, tag
	def func7(rdd):
		tagId, (tag, relevance) = rdd
		return tagId, tag, relevance
	for key, value in self.final_score.collect():	
		temp=list(value)
		temp.sort(key=lambda x:x[2], reverse=True)
		temp=temp[:100]
		tempRdd=spark_content
		tempRdd=tempRdd.parallelize(temp)
		kickmovie=tempRdd.map(func5)
		print(kickmovie.first())
		tempRdd=tempRdd.join(self.tags).map(func6)
		tempRdd=tempRdd.join(kickmovie)
		#temp = list(tempRdd.map(func7).toLocalIterator())
		#temp.sort(key=lambda x:x[2], reverse=True)
		
		print(tempRdd.first())
		temp=tempRdd.takeOrdered(100, key=lambda x:-x[1][1])
		#.toLocalIterator()
		#temp=temp.sort(key=lambda x:x[1][1], reverse=True)
		name = "movie_"+str(key)
		f=file("./tag_classification/"+name, "w+")
		#f.write(str(temp))
		
		for x in temp:
			#print(x)
			#f.write(str(x)+"\n")
			f.write(str(x[1][0]) + ":"+str(x[1][1])+"\n")
		f.close()
		print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	
	
	######################################################################
	

	#self.sc = spark_content
	logger.info("Start to Read ratings.csv ... ")
	#raw_ratings = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/ratings.csv")
	raw_ratings = self.sc.textFile("file:///home/bjt/Downloads/ml-latest/ratings.csv")
	#entry[0]: User ID; entry[1]: Movie ID; entry[2]: ratings
	first_line = raw_ratings.take(1)[0]
	self.ratings = raw_ratings.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), int(entry[1]), float(entry[2]))).cache()
	ratings_cluster = raw_ratings.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[1]), int(entry[0]))).cache()
	logger.info("Start to Read movies.csv ... ")
	#raw_movies = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/movies.csv")
	raw_movies = self.sc.textFile("file:///home/bjt/Downloads/ml-latest/movies.csv")
	#entry[0]: Movie ID; entry[1]: Title; entry[2]: Genere
	first_line = raw_movies.take(1)[0]
	self.movies = raw_movies.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), entry[1], entry[2])).cache()
	#print(self.movies.first())
	moviesForCluster = raw_movies.filter(lambda temp: temp != first_line)\
			.map(lambda temp: temp.split(","))\
			.map(lambda entry: (int(entry[0]), (entry[1]+","+ entry[2]))).cache()
	ratings_cluster = ratings_cluster.join(moviesForCluster)
	#print(ratings_cluster.first())
	#(entry[1:]+", "+entry[2] + ", " + entry[3]))
	#print(moviesForCluster.first())
	logger.info("Start to Count Movie ratings ...")
	self.aver_ratings = self.ratings.map(lambda entry:(entry[1], entry[2])).groupByKey().map(counts)
	self.ratings_count=self.aver_ratings.map(lambda x: (x[0], x[1][0]))
	self.aver_ratings = self.aver_ratings.map(lambda x: (x[0], x[1][1]))
	print(self.aver_ratings.first())
	print("Average For Movie 5: " + str(self.get_average(5)))
	self.get_movie_sort()
        logger.info("Train the ALS model ...")
        self.model = ALS.train(self.ratings, 8, seed=5L, iterations=10, lambda_=0.1)
        logger.info("Successfully build ALS model!")
	movie_factors = self.model.productFeatures().map(lambda (id,factor): (id,Vectors.dense(factor)))
	#print 'movie_factors first data:',movie_factors.first()
	movie_vectors = movie_factors.map(lambda (id,vec):vec)

	user_factors = self.model.userFeatures().map(lambda (id,factor):(id,Vectors.dense(factor)))
	#print 'user_factors first data:',user_factors.first()
	user_vectors = user_factors.map(lambda (id, vec):vec)

	num_clusters = 5
	num_iterations = 20
	num_runs =3
	logger.info("Train the KMeans Movie model ...")
	movie_cluster_model = KMeans.train(movie_vectors,num_clusters, num_iterations, num_runs)
	logger.info("Successfully build the KMeans Movie model ...")
	logger.info("Train the KMeans User model ...")
	#movie_cluster_model_coverged = KMeans.train(movie_vectors,num_clusters,100)
	user_cluster_model = KMeans.train(user_vectors,num_clusters,num_iterations, num_runs)
	logger.info("Successfully build the KMeans User model ...")
	predictions = movie_cluster_model.predict(movie_vectors)
	#print 'Predict the first 10 samples tag with:'+",".join([str(i) for i in predictions.take(10)])

	#temp=movie_vectors.zip(predictions)
	#temp.saveAsTextFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/result")
	#titles_factors = raw_movies.filter(lambda temp: temp != first_line).map(lambda temp: temp.split(",")).map(lambda entry: entry[0], (entry[1]+entry[2])).join(movie_factors)
	titles_factors = moviesForCluster.join(movie_factors)
	#print(titles_factors.first())
	#movies_assigned = titles_factors.map(lambda id, (title, genere)) 

    	#movie_assigned = title_factors.map(lambda (id, (name_generes, vec)): id, name_generes, pred = movie_cluster_model.predict(vec), cluster_center = movie_cluster_model.clusterCenters[pred], dist = vec.squared_distance(cluster_center_vec))
	#movie_assigned = titles_factors.map(func2)
	#print(movie_assigned.first())
	
	def func2(rdd):
   		id,(name_genres,vec) = rdd
   	 	pred = movie_cluster_model.predict(vec)
   	 	cluster_center = movie_cluster_model.clusterCenters[pred]
   	 	cluster_center_vec = Vectors.dense(cluster_center)
  	 	dist = vec.squared_distance(cluster_center_vec)
 	  	return str(pred), str(id), name_genres, dist
	
	#movie_assigned = titles_factors.map(lambda x:x[0], x[1][0], movie_cluster_model.predict(x[1][1]), vec.squared_distance(Vectors.dense(movie_cluster_model.clusterCenters[movie_cluster_model.predict(x[1][1])])))
	self.movie_assigned = titles_factors.map(func2).groupBy(lambda x:x[0])
	#movie_assigned.saveAsTextFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/result")
	#print(movie_assigned.first())
	reload(sys)
	sys.setdefaultencoding("utf8") 
	
	for key, value in self.movie_assigned.collect():
		#print("=====================================================================================================================")
		print("Movie Cluster: "+ key)

		#print("Top Twenty Movies:\n")
		
		temp = list(value)
		temp.sort(key=lambda x:x[3])
		#write into a file
		name = "Cluster_" + key
		f=file("/home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/movie_cluster_current/" + name, "w+")
		#f.write(str(temp))
		#f.close()
		#for i in range(len(temp)):
		#	f.write(str(temp[i])+"\n")
		#f.write(str(temp))
		#f.close()
		f.write("movieId,title,genres,distance\n")
		for x in temp:
			f.write(str(x[1])+","+str(x[2])+","+str(x[3])+"\n")
		f.close()
	print("=====================================================================================================================")
	
	#self.movie_cluster_func(self.movie_assigned, 1)
	#ratings_factors = ratings_cluster.join(user_factors)
	#print(ratings_factors.collect())
	#user_assigned = ratingsForCluster.map(func2).groupBy(lambda x:x[0])
	#temp = movie_factors.join(moviesForCluster)
	#user_assigned = self.ratings.join(temp)
	def func3(rdd):
		movie_id, (user_id, name_genres) = rdd
		return user_id, (str(movie_id) + "," + name_genres)
	self.user_assigned = ratings_cluster.map(func3).join(user_factors) 	
	self.user_assigned = self.user_assigned.map(func2).groupBy(lambda x:x[0])
	#print(user_assigned.first())
	#self.user_cluster_func(self.user_assigned, 1)

	for key, value in self.user_assigned.collect():
		#print("=====================================================================================================================")
		print("User Cluster: "+ str(key))

		#print("Top Twenty Users:\n")
		
		temp = list(value)
		temp.sort(key=lambda x:x[3])
		#write into a file
		name = "Cluster_" + key
		#f=file("/home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/user_cluster/"+ name, "w+")
		#f.write(str(temp))
		#f.close()
		f=file("./user_cluster/"+name, "w+")
		#for i in range(len(temp)):	
		#	f.write(str(temp[i])+"\n")
		#f.write(str(temp))
		#f.close()
		f.write("userId,movieId,title,genres,distance\n")
		count=0
		for x in temp:
			f.write(str(x[1])+","+str(x[2])+","+str(x[3])+"\n")
		f.close()
	print("=====================================================================================================================")
	#tempSc = spark_content
	#tempRdd = tempSc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/user_ratings.file")
	#inputnew = tempRdd.map(lambda temp: temp.split(",")).map(lambda entry:(int(entry[0]), int(entry[1]), float(entry[2])))
	#raw_ratings = self.sc.textFile("file:///home/bjt/BigData/Spark/spark-2.0.1-bin-hadoop2.7/bigData/datasets/user_ratings.csv")
	#first_line = raw_ratings.take(1)[0]
	#new_input = raw_ratings.map(lambda temp: temp.split(","))\
	#	      .map(lambda entry: (int(entry[0]), int(entry[1]), float(entry[2])))
	#self.ratings = self.ratings.union(new_input)
	#self.ratings_count = self.ratings.map(lambda entry:(entry[1], entry[2])).groupByKey().map(counts).map(lambda x: (x[0], x[1][0]))
#	self.model = ALS.train(self.ratings, 8, seed=5L, iterations=10, lambda_=0.1)
	#print(new_input.first())
  	# create a list with the format required by the negine (user_id, movie_id, rating)
  	#inputnew = tempRDD
	#self.__new_ratings(new_input)

	#ratings_list = raw_ratings.map(lambda x: x.split(",")).map(lambda x: (0, int(x[0]), float(x[1])))
   	# create a list with the format required by the negine (user_id, movie_id, rating)
    	#ratings = map(lambda x: (0, int(x[0]), float(x[1])), ratings_list)
	#self.__new_ratings(ratings_list)

	

	#final_list.sort(key=lambda x:x[2])
	#tempRdd = spark_content
	#final_rdd = tempRdd.parallelize(final_list).map(func4).join(self.tags)
	#print(final_entry.first())

	





