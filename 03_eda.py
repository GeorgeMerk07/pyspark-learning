from pyspark.sql import SparkSession
from pyspark.sql import functions as F 

spark = (
    SparkSession.builder
    .appName("Lesson3")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

#Read the cleaned data from Exercise 2 and register it as a SQL table
df = spark.read.parquet("processed_data.parquet")
df.createOrReplaceTempView("titanic")

#1.Group By: survival rate  by sex 
spark.sql("""
    SELECT Sex,
           COUNT(*) AS passengers,
           ROUND(AVG(Survived) * 100, 1) AS survival_rate
    FROM titanic
    GROUP BY Sex
    ORDER BY survival_rate DESC
""").show()

#2. GROUP BY two columns + HAVING
spark.sql("""
    SELECT Pclass, Sex,
           COUNT(*) AS passengers,
           ROUND(AVG(Survived) * 100, 1) AS survival_rate
    FROM titanic
    GROUP BY Pclass, Sex
    HAVING COUNT(*) > 50
    ORDER BY Pclass, Sex
""").show()

#3. CASE WHEN: age groups 
spark.sql("""
    SELECT CASE
              WHEN Age_mean < 13 THEN '1. Child'
              WHEN Age_mean < 20 THEN '2. Teen'
              WHEN Age_mean < 60 THEN '3. Adult'
              ELSE '4. Senior'
            END AS age_group,
            COUNT(*) AS passengers,
            ROUND(AVG(Survived) * 100, 1) AS survival_rate
    FROM titanic
    GROUP BY age_group
    ORDER BY age_group
""").show()

#4. LEFT JOIN with a small lookup table of port names
ports = spark.createDataFrame(
    [("S", "Southampton"), ("C", "Cherbour"), ("Q", "Queenstown")],
    ["code", "port_name"],
)
ports.createOrReplaceTempView("ports")

spark.sql("""
    SELECT p.port_name,
           COUNT(*) AS passengers,
           ROUND(AVG(t.Survived) * 100, 1) AS survival_rate
    FROM titanic t
    LEFT JOIN ports p ON t.Embarked = p.code
    GROUP BY p.port_name
    ORDER BY passengers DESC
""").show()

#5. Window functions: the 3 most expensive tickets in each class
spark.sql("""
    SELECT *
    FROM (
         SELECT Pclass, Name, Fare,
                RANK() OVER (PARTITION BY Pclass ORDER BY Fare DESC) AS fare_rank
         FROM titanic
)
WHERE fare_rank <= 3
ORDER BY Pclass, fare_rank
""").show(truncate=False)

#6 The same question as #1, with the Data Frame API 
(
    df.groupBy("Sex")
    .agg(
        F.count("*").alias("passengers"),
        F.round(F.avg("Survived") * 100, 1).alias("survival_rate"),
    )
    .orderBy(F.desc("survival_rate"))
    .show()
)

spark.stop()


