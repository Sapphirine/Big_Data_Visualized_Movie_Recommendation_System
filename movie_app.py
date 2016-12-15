import sys
import csv
from flask import Blueprint
from flask import Flask, request, render_template, url_for, redirect
main = Flask(__name__)
main.config['SECRET_KEY'] = 'DontTellAnyone'
from flask_wtf import Form
from wtforms import StringField, SubmitField
from wtforms.validators import InputRequired
import pprint 
import json
import requests
from movie_engine import RecEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pp = pprint.PrettyPrinter(indent = 2)

class topForm(Form):
    user_id = StringField('User Id:', validators=[InputRequired()])
    count = StringField('Top Count:', validators=[InputRequired()])

class indvForm(Form):
    user_id = StringField('User Id:', validators=[InputRequired()])
    movie_id = StringField('Movie Id:', validators=[InputRequired()])
    
class tagForm(Form):
    movie_id = StringField('Movie Id:', validators=[InputRequired()])

class catForm(Form):
    category = StringField('Category:', validators=[InputRequired()])
    count = StringField('Top Count:', validators=[InputRequired()])

# class nuForm(Form):
#     user_id=StringField('User Id:', validators=[InputRequired()])
#     rating=StringField('Rating:', validators=[InputRequired()])

@main.route("/", methods = ["GET", "POST"])
def index():
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    user_rating_file = open('user_no.txt', 'r+')
    reader = user_rating_file.read()
    new_user_id=str(int(reader)+1)
    # formNu = nuForm()
    if form.validate_on_submit():
    #return "successful!"
        tuser_id = form.user_id.data
        tcount = form.count.data
        return redirect(url_for('top_ratings', user_id = tuser_id, count = tcount))
    if formIndv.validate_on_submit():
        indv_user = formIndv.user_id.data
        indv_mov = formIndv.movie_id.data
        return redirect(url_for('movie_ratings', user_id = indv_user, movie_id = indv_mov))
    if formTag.validate_on_submit():
        tag_mov = formTag.movie_id.data
        return redirect(url_for('movie_tags', movie_id = tag_mov))
    if formCat.validate_on_submit():
        tcount = formCat.count.data
        cat_mov = formCat.category.data
        return redirect(url_for('category', category = cat_mov, count=tcount))
    # if formNu.validate_on_submit():
    #     print 'create new user'
    #     user=formNu.user_id.data
    #     rating=formNu.rating.data
    #     print(rating)
    #     return redirect(url_for('new_user'),user=user,rating=rating)
    return render_template('index.html', **locals())
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    # formNu = nuForm()
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.recommend_top_movies(user_id,count)
    list = top_ratings
    return render_template('index.html', **locals())
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    #logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    # formNu = nuForm()
    logger.info("User %s rating requested for movie %s", user_id, movie_id);
    ratings = recommendation_engine.target_user_movies_ratings(user_id, [movie_id])
    single_rating = ratings[0]
    return render_template('index.html', **locals())
    # return render_template('select_movie.html', value=value, user_id=user_id, movie_id=movie_id)

@main.route("/<int:movie_id>", methods=["GET"])
def movie_tags(movie_id):
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    # formNu = nuForm()
    logger.info("Get Visualizaton of the tags of the selected movie")
    path="./spark_content/visualization/tag_classification/movie_"+str(movie_id)    
    f=open(path)
    tag=f.read()
    return render_template('index.html', **locals())
    # return render_template('tag_movie.html', value=value, movie_id=movie_id)


@main.route("/<string:category>/category/top/<int:count>", methods=["GET"])
def category(category,count):
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    # formNu = nuForm()
    pp.pprint(category)
    logger.info("Return top movies in the selected category")
    path="./movie_cluster/"+str(category)  
    f=open(path)
    reader=f.read().splitlines()
    all_category=[]
    for row in reader:   # iterates the rows of the file in orders
        all_category.append(row)
    selected_category=all_category[1:count+1]
    f.close()      # closing
    return render_template('index.html', **locals())


@main.route("/newuser", methods=["POST"])
def newuser():
    form = topForm()
    formIndv = indvForm()
    formTag = tagForm()
    formCat = catForm()
    rating=request.form['user']
    pp.pprint(rating)
    logger.info("user is created")
    user_rating_file = open('user_no.txt', 'r+')
    reader = user_rating_file.read()
    user_id=str(int(reader)+1)
    user_rating_file.seek(0)
    user_rating_file.write(str(int(reader)+1))
    user_rating_file.truncate()
    user_rating_file.close()
    path="./datasets/"
    new_user_file=open(path+'new_user.csv','ab')
    writer=csv.writer(new_user_file)
    new_user_dict={}
    new_user_dict['142138']=request.form['142138']
    new_user_dict['124637']=request.form['124637']
    new_user_dict['132124']=request.form['132124']
    new_user_dict['136992']=request.form['136992']
    new_user_dict['138172']=request.form['138172']
    new_user_dict['164620']=request.form['164620']
    new_user_dict['138620']=request.form['138620']
    new_user_dict['132232']=request.form['132232']
    new_user_dict['143180']=request.form['143180']
    new_user_dict['99450']=request.form['99450']
    new_user_dict['136988']=request.form['136988']

    for item in new_user_dict:
        if new_user_dict[item] != "I don't know":
            writer.writerow((user_id,item,new_user_dict[item]))
    new_user_file.close()
    recommendation_engine.ratings_new_user('./datasets/new_user.csv')
    return render_template('index.html', **locals())




 
def create_app(spark_context):
    global recommendation_engine 

    recommendation_engine = RecEngine(spark_context)    
    
    return main 

