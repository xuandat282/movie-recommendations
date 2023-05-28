from flask import Flask, jsonify, request
from flask_cors import CORS
from py2neo import Graph, Node, NodeMatcher

app = Flask(__name__)
CORS(app)

USER = "neo4j"
PASS = "12345678"
graph = Graph("bolt://" + ":7687", auth=(USER, PASS))


@app.route("/")
def hello_world():
    return "<p>App started!</p>"

####### Movie #######


def projection_exists():
    result = graph.run(
        'CALL gds.graph.exists("movielens_projection")\n'
        "YIELD graphName, exists\n"
        "RETURN exists\n"
    )
    return jsonify(result.data()).get_data("exists")

# Get the available details of a given movie


@app.route('/api/movie/details/<title>')
def getMovieData(title):
    matcher = NodeMatcher(graph)
    movie = matcher.match("Movie", title={title}).first()

    return jsonify(movie)


# Get the genres associated with a given movie
@app.route('/api/movie/genres/<title>')
def getMovieGenres(title):
    genres = graph.run(
        'MATCH (genres)-[:IS_GENRE_OF]->(m:Movie {title: $title}) RETURN genres', title=title)

    return jsonify(list(genres))


# Get the submitted ratings of a given movie
@app.route('/api/movie/ratings/<title>')
def getMovieRatings(title):
    ratings = graph.run('MATCH (u: User)-[r:RATED]->(m:Movie {title: $title}) RETURN u.id AS user, r.rating AS rating',
                        title=title)

    return jsonify(ratings.data())


# Get the submitted tags of a given movie
@app.route('/api/movie/tags/<title>')
def getMovieTags(title):
    tags = graph.run('MATCH (u: User)-[t:TAGGED]->(m:Movie {title: $title}) RETURN u.id AS user, t.tag AS tag',
                     title=title)

    return jsonify(tags.data())


# Get list of movies from a given year
@app.route('/api/movie/year/<year>')
def getMoviesByYear(year):
    movies = graph.run(
        'MATCH (m:Movie) where m.year = $year RETURN m.title AS title, m.year AS year', year=year)

    return jsonify(movies.data())


# Get the average rating for a given movie
@app.route('/api/movie/average-rating/<title>')
def getMovieAverageRating(title):
    avg = graph.run(
        'MATCH (u: User)-[r:RATED]->(m:Movie {title: $title}) RETURN m.title AS title, avg(toFloat(r.rating)) AS averageRating',
        title=title)

    return jsonify(avg.data())


####### Top #######

# Get top N highest rated movies
@app.route('/api/top/movie/top-n/<n>')
def getMovieTopN(n):
    mvs = graph.run(
        'MATCH (u: User )-[r:RATED]->(m:Movie) RETURN m.title AS title, avg(r.rating) AS averageRating, m.img AS img order by averageRating desc limit $n',
        n=int(n))

    return mvs.data()


# Get top N most rated movies
@app.route('/api/top/movie/n-most-rated/<n>')
def getMovieNMostRated(n):
    mvs = graph.run(
        'MATCH (u: User )-[r:RATED]->(m:Movie) RETURN m.title AS title, count(r.rating) as NumberOfRatings order by NumberOfRatings desc limit $n',
        n=int(n))

    return mvs.data()


####### User #######

# Get the submitted ratings by a given user
@app.route('/api/user/ratings/<userId>')
def getUserRatings(userId):
    ratings = graph.run(
        'MATCH (u:User {id: $userId})-[r:RATED ]->(movies) RETURN movies.title AS movie, r.rating AS rating',
        userId=str(userId))

    return jsonify(ratings.data())


# Get the submitted tags by a given user
@app.route('/api/user/tags/<userId>')
def getUserTags(userId):
    tags = graph.run('MATCH (u:User {id: $userId})-[t:TAGGED]->(movies) RETURN movies.title AS title, t.tag AS tag',
                     userId=str(userId))

    return jsonify(tags.data())


# Get the average rating by a given user
@app.route('/api/user/average-rating/<userId>')
def getUserAverageRating(userId):
    avg = graph.run(
        'MATCH (u: User {id: $userId})-[r:RATED]->(m:Movie) RETURN u.id AS user, avg(toFloat(r.rating)) AS averageRating',
        userId=str(userId))

    return jsonify(avg.data())


# Recommender Enginer

# Content based
@app.route('/api/rec_engine/content/<title>/<n>')
def getRecContent(title, n):
    avg = graph.run('MATCH (m:Movie)<-[:IS_GENRE_OF]-(g:Genre)-[:IS_GENRE_OF]->(rec:Movie) '
                    'WHERE m.title = $title'
                    'WITH rec, COLLECT(g.name) AS genres, COUNT(*) AS numberOfSharedGenres '
                    'RETURN rec.title as title, genres, numberOfSharedGenres '
                    'ORDER BY numberOfSharedGenres DESC LIMIT $n', title=str(title), n=int(n))

    return jsonify(avg.data())

# Collaborative Filtering


@app.route('/api/rec_engine/collab/<userid>/<n>')
def getRecCollab(userid, n):
    rec = graph.run('MATCH (u1:User {id: $userid})-[r:RATED]->(m:Movie) '
                    'WITH u1, avg(r.rating) AS u1_mean '
                    'MATCH (u1)-[r1:RATED]->(m:Movie)<-[r2:RATED]-(u2) '
                    'WITH u1, u1_mean, u2, COLLECT({r1: r1, r2: r2}) AS ratings WHERE size(ratings) > 10 '
                    'MATCH (u2)-[r:RATED]->(m:Movie) '
                    'WITH u1, u1_mean, u2, avg(r.rating) AS u2_mean, ratings '
                    'UNWIND ratings AS r '
                    'WITH sum( (r.r1.rating-u1_mean) * (r.r2.rating-u2_mean) ) AS nom, '
                    'sqrt( sum( (r.r1.rating - u1_mean)^2) * sum( (r.r2.rating - u2_mean) ^2)) AS denom, u1, u2 WHERE denom <> 0 '
                    'WITH u1, u2, nom/denom AS pearson '
                    'ORDER BY pearson DESC LIMIT 10 '
                    'MATCH (u2)-[r:RATED]->(m:Movie) WHERE NOT EXISTS( (u1)-[:RATED]->(m) ) '
                    'RETURN m.title AS title, SUM( pearson * r.rating) AS score '
                    'ORDER BY score DESC LIMIT $n', userid=str(userid), n=int(n))

    return jsonify(rec.data())

# Colaborative filtering GDS and KNN - recommend by user


@app.route('/api/rec_engine/cf_gds_knn_user/<userid>/<n>')
def getRecCF_GDS_KNN_user(userid, n):
    query = '''
    MATCH (u:User {id: $userid})
    MATCH (u)-[r1:SIMILAR]->(similarUser)-[r2:RATED]->(m:Movie)
    WHERE NOT EXISTS((u)-[:RATED]->(m))
    RETURN u.id AS userId, m.title AS movieTitle, m.id AS movieId, COLLECT(DISTINCT similarUser.id) AS similarUserIds
    LIMIT $limit
    '''
    rec = graph.run(query, userid=str(userid), limit=int(n))
    return jsonify(rec.data())

# Colaborative filtering GDS and KNN - recommend by movie


@app.route('/api/rec_engine/cf_gds_knn_movie/<movieid>/<n>')
def getRecCF_GDS_KNN_movie(movieid, n):
    query = '''
    MATCH (targetMovie:Movie {id: $movieid})
    MATCH (targetMovie)-[similarity:SIMILAR]-(similarMovie:Movie)
    WHERE similarMovie.id <> targetMovie.id
    RETURN DISTINCT similarMovie.id AS movieId, similarMovie.title AS movieTitle , similarity.score AS score
    ORDER BY similarity.score DESC
    LIMIT $limit
    '''
    rec = graph.run(query, movieid=str(movieid), limit=int(n))
    return jsonify(rec.data())


@app.route('/api/rec_engine/pagerank_collab/<movieid>/<n>')
def getRecPageRankCollab(movieid, n):
    query = '''
    MATCH (src1:Movie {id: $movieid})
    CALL gds.pageRank.stream('pagerank_collab', {
    maxIterations: 20,
    dampingFactor: 0.85,
    sourceNodes: [src1],
    relationshipTypes: ['SIMILAR'],
    relationshipWeightProperty: 'score'
    })
    YIELD nodeId, score
    WHERE gds.util.asNode(nodeId).id <> src1.id
    WITH gds.util.asNode(nodeId) AS movie, score
    RETURN movie.id AS movieId, movie.title AS movieTitle, score
    ORDER BY score DESC
    LIMIT $n;
    '''
    result = graph.run(query, movieid=str(movieid), n=int(n))
    return jsonify(result.data())

# get movie name by id


@app.route('/api/getmoviename/<movieid>')
def getMovieName(movieid):
    query = '''
    MATCH (m:Movie {id: $movieid})
    RETURN m.title AS movieTitle
    '''
    result = graph.run(query, movieid=str(movieid))
    return jsonify(result.data())


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)
