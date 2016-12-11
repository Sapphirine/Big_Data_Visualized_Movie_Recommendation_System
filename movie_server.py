import time, sys, cherrypy, os
#from socket import *
from paste.translogger import TransLogger
from movie_app import create_app
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # Init spark context and load libraries
    spark_content = SparkContext(conf=SparkConf().setAppName("movie_server"), pyFiles=['movie_engine.py', 'movie_app.py'])
    application = create_app(spark_content)
    logger = TransLogger(application)  
    cherrypy.tree.graft(logger, '/')
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
    cherrypy.engine.start()
    cherrypy.engine.block()
