from pyspark.sql import SparkSession
from user_agents import parse
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import time
import requests
import pandas as pd
from pyspark.sql.window import Window
import json
from flask import Flask, request


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


app = Flask(__name__)


#Function to load data from the .gz file

def load_data(start_date=None, end_date=None):
    spark = SparkSession.builder.appName("abc").getOrCreate()
    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("time", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("IP", StringType(), True),
            StructField("user_agent_string", StringType(), True),
        ]
    )

    df2 = spark.read.csv("input_data", sep=r"\t", schema=schema)
    split_col = split(df2["IP"], ",")
    df2 = df2.withColumn("IP1", split_col.getItem(0))
    df2 = df2.withColumn("IP2", split_col.getItem(1))

    print("recomended: 2014-10-12")
    # first_day = input("Enter starting day, format:YYY-MM-DD ")
    # last_day = input("Enter last day, format:YYY-MM-DD ")
    # first_day = str(first_day)
    # last_day = str(last_day)
    df2 = df2.filter(col("date").between(start_date, end_date))
    df2 = (
        df2.withColumn("browser_family", br_udf(df2.user_agent_string))
        .withColumn("os_family", os_udf(df2.user_agent_string))
        .withColumn("dev_type", devtype_udf(df2.user_agent_string))
    )
    return df2, spark

# Function to extract operating system family using user_agent library
def os_fam(x):
    user_agent = parse(x)
    os = user_agent.os.family
    return os


os_udf = udf(lambda x: os_fam(x))

# Function to extract browser family using user_agent library
def browser_fam(x):
    user_agent2 = parse(x)
    os = user_agent2.browser.family
    return os


br_udf = udf(lambda x: browser_fam(x))

# Function to extract device type using user_agent library
def device_type(x):
    user_agent2 = parse(x)
    os = user_agent2.device.family
    return os


devtype_udf = udf(lambda x: device_type(x))

#function to extract city and country using ip-api from IP adress
def udf_ip(x):
    #    print(x)
    time.sleep(0.1)
    IP = x
    url = f"http://ip-api.com/json/{IP}"
    payload = ""
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    #    print(response)

    data = response.json()
    if data["status"] != "fail":
        city = data["city"]
        country = data["country"]
        return str(country) + "," + str(city)
    else:
        return None

#functions to return browser,operating system and device percentages into the rest API
def browser(start_date=None, end_date=None):
    df2, spark = load_data(start_date, end_date)
    df2.createOrReplaceGlobalTempView("df2")
    top5_browsers = spark.sql(
        "SELECT browser_family, COUNT(DISTINCT(user_id)) as unique_users FROM global_temp.df2 GROUP BY browser_family ORDER BY unique_users DESC"
    )
    top5_browsers = top5_browsers.withColumn(
        "percent",
        (col("unique_users") / sum("unique_users").over(Window.partitionBy())) * 100,
    )
    new_df = top5_browsers.limit(5)
    return new_df.toJSON().collect()


def os(start_date=None, end_date=None):
    df2, spark = load_data(start_date, end_date)
    df2.createOrReplaceGlobalTempView("df2")
    top5_os = spark.sql(
        "SELECT os_family, COUNT(DISTINCT(user_id)) as unique_users FROM global_temp.df2 GROUP BY os_family ORDER BY unique_users DESC"
    )
    top5_os = top5_os.withColumn(
        "percent",
        (col("unique_users") / sum("unique_users").over(Window.partitionBy())) * 100,
    )
    new_df = top5_os.limit(5)
    return new_df.toJSON().collect()


def device(start_date=None, end_date=None):
    df2, spark = load_data(start_date, end_date)
    df2.createOrReplaceGlobalTempView("df2")
    top5_dev = spark.sql(
        "SELECT dev_type, COUNT(DISTINCT(user_id)) as unique_users FROM global_temp.df2 GROUP BY dev_type ORDER BY unique_users DESC"
    )
    top5_dev = top5_dev.withColumn(
        "percent",
        (col("unique_users") / sum("unique_users").over(Window.partitionBy())) * 100,
    )
    new_df = top5_dev.limit(5)
    return new_df.toJSON().collect()

# Main function for task a, printing the 4 dataframes as in the description
def task_a(start_date=None, end_date=None):
    start_date = str(start_date)
    end_date - str(end_date)
    df2, spark = load_data(start_date, end_date)
    df2.createOrReplaceGlobalTempView("df2")

    top5_browsers = spark.sql(
        "SELECT browser_family, COUNT(DISTINCT(user_id)) as unique_users FROM global_temp.df2 GROUP BY browser_family ORDER BY unique_users DESC LIMIT 5"
    )
    top5_os = spark.sql(
        "SELECT os_family, COUNT(DISTINCT(user_id)) as unique_users FROM global_temp.df2 GROUP BY os_family ORDER BY unique_users DESC LIMIT 5"
    )

    top5_browsers = top5_browsers.toPandas()
    top5_os = top5_os.toPandas()

    pandas_test = df2.limit(45).toPandas()
    pandas_test["test"] = pandas_test["IP1"].apply(lambda x: udf_ip(x))
    pandas_test[["test", "city"]] = pandas_test["test"].str.split(",", expand=True)

    print("\n")
    print("\n")
    print("top 5 browsers based on unique users")
    print("\n")
    print(top5_browsers)

    print("\n")
    print("\n")

    print("top 5 operating systems based on unique users")
    print("\n")
    print(top5_os)

    new = pandas_test["city"].value_counts()
    new = pd.DataFrame(new)
    new.reset_index(level=0, inplace=True)
    new = new.rename(columns={"index": "city", "city": "events_number"})

    new2 = pandas_test["test"].value_counts()
    new2 = pd.DataFrame(new2)
    new2.reset_index(level=0, inplace=True)
    new2 = new2.rename(columns={"index": "country", "test": "events_number"})

    print("\n")
    print("\n")
    print("Top 5 Cities based on number of events")
    print("\n")
    print(new[0:5])
    print("\n")
    print("\n")
    print("Top 5 Countrys based on number of events")
    print("\n")
    print(new2[0:5])



#Building the API's with the endpoints as requested in Task b
@app.route("/stats/os", methods=["GET"])
def get_os_stats():
    start_date = request.args.get("start_date")
    end_date = request.args.get("start_date")
    if start_date and end_date:
        df = os(str(start_date), str(end_date))
        return_results = []
        for os_result in df:
            os_result = json.loads(os_result)
            data = {}
            data["os"] = os_result["os_family"]
            data["percent"] = os_result["percent"]
            return_results.append(data)
        return {"results": return_results}
    return {"message": "didn't find the arguements."}


@app.route("/stats/browser", methods=["GET"])
def get_browser_stats():
    start_date = request.args.get("start_date")
    end_date = request.args.get("start_date")
    if start_date and end_date:
        df = browser(str(start_date), str(end_date))
        return_results = []
        for result in df:
            result = json.loads(result)
            data = {}
            data["browser"] = result["browser_family"]
            data["percent"] = result["percent"]
            return_results.append(data)
        return {"results": return_results}
    return {"message": "didn't find the arguements."}


@app.route("/stats/device", methods=["GET"])
def get_device_stats():
    start_date = request.args.get("start_date")
    end_date = request.args.get("start_date")
    if start_date and end_date:
        df = device(str(start_date), str(end_date))
        return_results = []
        for result in df:
            result = json.loads(result)
            data = {}
            data["browser"] = result["dev_type"]
            data["percent"] = result["percent"]
            return_results.append(data)
        return {"results": return_results}
    return {"message": "didn't find the arguements."}


if __name__ == "__main__":
#   task_a(2014-10-12,2014-10-12)
#uncoment above if you want to run task a function
    app.run(host="0.0.0.0", port=8080)
