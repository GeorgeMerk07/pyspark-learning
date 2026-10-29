from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Exercise1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()

)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/titanic.csv", header=True, inferSchema=True)

df.printSchema()

df.show(10)

df.select("Name", "Age", "Fare", "Sex").show(10, truncate=False)

print("Rows:", df.count())
print("Columns:", len(df.columns))


print("Female count:", df.filter(df.Sex == "female").count())
print("Male count:", df.filter(df.Sex == "male").count())
df.describe("Age", "Fare").show()

spark.stop()


