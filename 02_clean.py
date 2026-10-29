from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.ml.feature import Imputer

spark = (
    SparkSession.builder
    .appName("Lesson2")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/titanic.csv", header=True, inferSchema=True)

# Count the missing values in the "Embarkes" column
missing_embarked = df.filter(F.col("Embarked").isNull()).count()
print(f"Missing values in embarked: {missing_embarked}")

#Second way to find the missing Embarked values

#2. Find the most common 'Embarked' value (mode)
most_common_row = df.groupBy("Embarked").count().orderBy(F.col("count").desc()).first()
most_commoon_embarked = most_common_row["Embarked"] if most_common_row else None
print(f"Most common Embarked value: {most_commoon_embarked}")

#3. Fill missing ages with the mean into a new column 'Age_Mean'
mean_age = df.select(F.mean("Age")).collect()[0][0]
df = df.withColumn("Age_mean", F.when(F.col("Age").isNull(), mean_age).otherwise(F.col("Age")))
print(f"The mean age is: {mean_age}")

#4. Create FamilySize and the "Alone" column (1 if alone, 0 otherwise)
df = df.withColumn("FamilySize", F.col("SibSp") + F.col("Parch") + 1)
df = df.withColumn("IsAlone", F.when(F.col("FamilySize") == 1, 1).otherwise(0))

#5. Print how many passengers travlled alone
passengers_alone = df.filter(F.col("IsAlone") == 1).count()
print(f"Number of passengers who travelled alone: {passengers_alone}")

#6.Save the results as Parquet file
df.write.mode("overwrite").parquet("processed_data.parquet")
print("Results successfully saved as processed_data.parquet")

df_check = spark.read.parquet("processed_data.parquet")
df_check.printSchema()

spark.stop()
