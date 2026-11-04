from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Exercise 3")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("processed_data.parquet")
df.createOrReplaceTempView("titanic")

#1. Survival by family size we are gonna categorise the family size in 3 groups
#first group Alone(1), second Small(2-4), and third Large(5 or more)

spark.sql("""
    SELECT 
        CASE
            WHEN FamilySize <= 1 THEN '1. Alone'

            WHEN FamilySize <= 4 THEN '2. Small'
            ELSE '3. Large'
        END AS FamilySizeGroup,
        ROUND(AVG(Survived), 3) AS survival_rate,
        COUNT(*) AS total_passengers
    FROM titanic
    GROUP BY FamilySizeGroup
    ORDER BY FamilySizeGroup
""").show()

# Now we must create a small lookup table with Pclass and a class name (1 -> First, 2 -> Second. 3->third),
# join it, and show the average fare per class name, rounded to 2 demicals

spark.sql("""
    WITH lookup AS (
        SELECT 1 AS Pclass, 'First' AS ClassName
        UNION ALL
        SELECT 2 AS Pclass, 'Second' AS ClassName
        UNION ALL
        SELECT 3 AS Pclass, 'Third' AS ClassName
)
    SELECT
        l.ClassName,
        ROUND(AVG(t.Fare), 2) AS Average_Fare
    FROM titanic t
    JOIN lookup l ON t.Pclass = l.Pclass
    GROUP BY l.ClassName
""").show()

#A window function with ROW_NUMBER. Show the 3 youngest passengers of each sex (use Age_mean), with their name and age.

spark.sql("""
    SELECT sex, Name, Age_mean
    FROM (
        SELECT Sex, Name, Age_mean,
               ROW_NUMBER() OVER (PARTITION BY Sex ORDER BY Age_mean ASC) AS row_num
        FROM titanic
    )
    WHERE row_num <= 3
""").show(truncate=False)

#4 We are going to answer agian question one but this time we will use DataFrmae API.

df_groups = df.withColumn(
        "FamilySizeGroup",
        F.when(F.col("FamilySize") <= 1, '1. Alone')
         .when(F.col("FamilySize") <= 4, '2. Small')
         .otherwise('3. Large'),
)
(
  df_groups.groupBy("FamilySizeGroup")
  .agg(
        F.round(F.avg("Survived"), 3).alias("survival_rate"),
        F.count("*").alias("total_passengers"),
  )
  .orderBy("FamilySizeGroup")
  .show()
)

spark.stop()
