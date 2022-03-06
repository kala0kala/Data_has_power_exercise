import urllib.request
from pyspark.sql import SparkSession

data_file_name = "/opt/spark-data/titanic.csv"
link = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
with urllib.request.urlopen(link) as f:
    html = f.read().decode("utf-8")

with open(data_file_name, "w") as f:
    f.write(html)

# creating spark session
def init_spark():
    spark = (
        SparkSession.builder.config(
            "spark.jars", "/opt/spark-apps/mysql-connector-java-8.0.28.jar"
        )
        .appName("PySpark_MySQL_test")
        .getOrCreate()
    )
    return spark


def main():
    url = "jdbc:mysql://demo-database:3306/alived"
    properties = {
        "user": "root",
        "password": "my-secret-pw",
        "driver": "com.mysql.jdbc.Driver",
    }

    file = "/opt/spark-data/titanic.csv"
    spark = init_spark()

    # reading csv
    reading_file = spark.read.load(
        file, format="csv", inferSchema="true", sep=",", header="true"
    )

    # filtering
    # without classes
    # all
    kids = reading_file.filter((reading_file["Age"] < 18))
    young_adults = reading_file.filter(
        (reading_file["Age"] > 18) & (reading_file["Age"] <= 40)
    )
    young_women = reading_file.filter(
        (reading_file["Age"] > 18)
        & (reading_file["Age"] <= 40)
        & (reading_file["Sex"] == "female")
    )
    young_men = reading_file.filter(
        (reading_file["Age"] > 18)
        & (reading_file["Age"] <= 40)
        & (reading_file["Sex"] == "male")
    )
    old_adults = reading_file.filter(reading_file["Age"] > 40)
    old_women = old_adults.filter(old_adults["Sex"] == "female")
    old_men = old_adults.filter(old_adults["Sex"] == "male")

    # alived
    alived_kids = reading_file.filter(
        (reading_file["Age"] < 18) & (reading_file["Survived"] == 1)
    )
    alived_young_adults = reading_file.filter(
        (reading_file["Age"] > 18)
        & (reading_file["Age"] <= 40)
        & (reading_file["Survived"] == 1)
    )
    alived_young_women = reading_file.filter(
        (reading_file["Age"] > 18)
        & (reading_file["Age"] <= 40)
        & (reading_file["Sex"] == "female")
        & (reading_file["Survived"] == 1)
    )
    alived_young_men = reading_file.filter(
        (reading_file["Age"] > 18)
        & (reading_file["Age"] <= 40)
        & (reading_file["Sex"] == "male")
        & (reading_file["Survived"] == 1)
    )
    alived_old_adults = reading_file.filter(
        (reading_file["Age"] > 40) & (reading_file["Survived"] == 1)
    )
    alived_old_women = alived_old_adults.filter(old_adults["Sex"] == "female")
    alived_old_men = alived_old_adults.filter(old_adults["Sex"] == "male")

    # witch classes
    # all
    first_class = reading_file.filter(reading_file["Pclass"] == 1)
    second_class = reading_file.filter(reading_file["Pclass"] == 2)
    third_class = reading_file.filter(reading_file["Pclass"] == 3)
    first_class_women = first_class.filter(first_class["Sex"] == "female")
    first_class_men = first_class.filter(first_class["Sex"] == "male")
    second_class_women = second_class.filter(second_class["Sex"] == "female")
    second_class_men = second_class.filter(second_class["Sex"] == "male")
    third_class_women = third_class.filter(third_class["Sex"] == "female")
    third_class_men = third_class.filter(third_class["Sex"] == "male")

    # alived
    alived_first_class = reading_file.filter(
        (reading_file["Pclass"] == 1) & (reading_file["Survived"] == 1)
    )
    alived_second_class = reading_file.filter(
        (reading_file["Pclass"] == 2) & (reading_file["Survived"] == 1)
    )
    alived_third_class = reading_file.filter(
        (reading_file["Pclass"] == 3) & (reading_file["Survived"] == 1)
    )
    alived_first_class_women = alived_first_class.filter(
        alived_first_class["Sex"] == "female"
    )
    alived_first_class_men = alived_first_class.filter(
        alived_first_class["Sex"] == "male"
    )
    alived_second_class_women = alived_second_class.filter(
        alived_second_class["Sex"] == "female"
    )
    alived_second_class_men = alived_second_class.filter(
        alived_second_class["Sex"] == "male"
    )
    alived_third_class_women = alived_third_class.filter(
        alived_third_class["Sex"] == "female"
    )
    alived_third_class_men = alived_third_class.filter(
        alived_third_class["Sex"] == "male"
    )

    # calculations
    data = [
        ("kids", (alived_kids.count() / kids.count()) * 100),
        ("young adults", (alived_young_adults.count() / young_adults.count()) * 100),
        ("yound women", (alived_young_women.count() / young_women.count()) * 100),
        ("young men", (alived_young_men.count() / young_men.count()) * 100),
        ("old adults", (alived_old_adults.count() / old_adults.count()) * 100),
        ("old women", (alived_old_women.count() / old_women.count()) * 100),
        ("old men", (alived_old_men.count() / old_men.count()) * 100),
        (
            "first class women",
            (alived_first_class_women.count() / first_class_women.count()) * 100,
        ),
        (
            "first class men",
            (alived_first_class_men.count() / first_class_men.count()) * 100,
        ),
        (
            "secong class women",
            (alived_second_class_women.count() / second_class_women.count()) * 100,
        ),
        (
            "secong class men",
            (alived_second_class_men.count() / second_class_men.count()) * 100,
        ),
        (
            "third class women",
            (alived_third_class_women.count() / third_class_women.count()) * 100,
        ),
        (
            "third class men",
            (alived_third_class_men.count() / third_class_men.count()) * 100,
        ),
    ]

    # creating table
    columns = ["category", "percent"]
    df = spark.createDataFrame(data=data, schema=columns)

    # sending to database
    df.write.jdbc(url=url, table="alived", mode="append", properties=properties)


main()
