# Link Prediction

Hadoop implementation of five queries:
Used: [https://grouplens.org/datasets/movielens/25m/](https://grouplens.org/datasets/movielens/25m/) 
Files: ratings.csv, movies.csv

* SelectionAndProjection `SELECT movieID, rating FROM ratings WHERE rating>=3.0`
* Intersection `SELECT movieID FROM ratings WHERE rating>=3.0 INTERSECT SELECT movieID FROM movies WHERE genre=comedy*`
* InnerJoin `SELECT movies.titles FROM ratings INNER JOIN movies ON movies.movieID = ratings.movieID WHERE ratings.rating>4.0`
* InnerJoin2 `SELECT movies.titles FROM ratings INNER JOIN movies ON movies.movieID = ratings.movieID WHERE ratings.rating>4.0 OR movies.genre = comedy*`
* GroupAndAggregation `SELECT movieID, AVG (rating), MAX (rating) FROM ratings GROUP BY movieID`

# Requirements

* Java - Maven
This is a Java Maven project so java and maven is required. All other dependencies included in pom.xml

* Hadoop
hadoop HDFS

# Usage

	Usage: <input-dir> <output-dir>

* if output dir is not empty, delete (or change output dir).
* if output dir does not exist it will automatically created.
