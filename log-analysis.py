import psycopg2

DBNAME = "news"


def article_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = "select articles.title, count(log.path) as views " \
            "from articles left join log " \
            "on '/article/' || articles.slug like log.path " \
            "group by articles.title " \
            "order by views desc " \
            "limit 3;"
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nWhat are the most popular three articles of all time? \n")
    for value in values:
        print(
            value[0], "Title",
            " -- ", str(value[1]), "views")


def author_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = "select authors.name, articles.slug, count(log.path) as views from articles join log " \
            "on'/article/' || articles.slug like log.path join authors " \
            "on articles.author = authors.id " \
            "group by authors.name, articles.slug " \
            "order by views desc " \
            "limit 3;"
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nWho are the most popular three article authors of all time? \n")
    for value in values:
        print(
            value[0], "Name",
            " -- ", value[1], "Slug",
            " -- ", str(value[2]), "views")


def log_analysis():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    query = "select day, perc from " \
            "(select day, round((sum(requests)/(select count(*) from log where " \
            "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as " \
            "perc from (select substring(cast(log.time as text), 0, 11) as day, " \
            "count(*) as requests from log where status not like '%200%' group by day) " \
            "as log_percentage group by day order by perc desc) as final_query " \
            "where perc >= 1;"
    c.execute(query)
    values = c.fetchall()
    db.close()
    print("\nOn which days more than 1% of requests lead to errors? \n")
    for value in values:
        print(
            value[0], "Day",
            " -- ", str(value[1]), "Percent")


article_analysis()
author_analysis()
log_analysis()
