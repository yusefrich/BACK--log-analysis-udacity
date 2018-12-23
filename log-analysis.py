#!/usr/bin/env python
import psycopg2

DBNAME = "news"


def article_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = """SELECT articles.title,
                        count(log.path) AS views
                FROM articles
                LEFT JOIN log ON '/article/' || articles.slug LIKE log.path
                GROUP BY articles.title
                ORDER BY views DESC
                LIMIT 3;"""
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nWhat are the most popular three articles of all time?")
    for title, views in values:
        print("{} -- {} views".format(title, views))


def author_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = """SELECT authors.name,
                       count(log.path) AS views
                FROM articles
                JOIN log ON'/article/' || articles.slug LIKE log.path
                JOIN authors ON articles.author = authors.id
                GROUP BY authors.name
                ORDER BY views DESC
                LIMIT 3;"""
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nWho are the most popular three article authors of all time?")
    for author, views in values:
        print("{} -- {} views".format(author, views))


def log_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = """SELECT DAY,
                       perc
                FROM
                  (SELECT DAY,
                          round((sum(requests)/
                                   (SELECT count(*)
                                    FROM log
                                    WHERE
                                    TO_CHAR(log.time, 'Mon-DD,YYYY') = DAY) *
                                    100), 2) AS perc
                   FROM
                     (SELECT TO_CHAR(log.time, 'Mon-DD,YYYY') AS DAY,
                             count(*) AS requests
                      FROM log
                      WHERE status NOT LIKE '%200%'
                      GROUP BY DAY) AS log_percentage
                   GROUP BY DAY
                   ORDER BY perc DESC) AS final_query
                WHERE perc >= 1;"""
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nOn which days more than 1% of requests lead to errors?")
    for day, percent in values:
        print("{} -- {} % errors".format(day, percent))


if __name__ == '__main__':
    article_analysis()
    author_analysis()
    log_analysis()
