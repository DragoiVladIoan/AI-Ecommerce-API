import os

import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

from misc.misc import generate_columns, concat_data_from_list

spark = SparkSession.builder \
        .master("local") \
        .appName("ai_powered_ecommerce_recsys_search_engine") \
        .config('spark.executor.heartbeatInterval', '3600')\
        .config('spark.executor.memory', '6g')\
        .getOrCreate()


def build_and_train_als_model(**kwargs):
    df = kwargs.get('df')
    file = kwargs.get('file_name')
    try:
        if df is not None:
            model_df = spark.createDataFrame(df)
        else:
            print(os.getcwd())
            model_df = spark.read.csv(os.getcwd() + "/data/data_location" + file, header=True, inferSchema=True)

        product_column_name = str(model_df.schema.fields[1].name)

        (training, test) = model_df.randomSplit([0.8, 0.2])
        als = ALS(maxIter=10, regParam=0.01, implicitPrefs=True,
                  userCol=str(model_df.schema.fields[0].name), itemCol=str(model_df.schema.fields[1].name),
                  ratingCol=str(model_df.schema.fields[2].name),
                  coldStartStrategy="drop")

        model = als.fit(training)

        predictions = model.transform(test)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol=str(model_df.schema.fields[2].name),
                                        predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        print(" Root-mean-square error = " + str(rmse))
        return model, product_column_name
    except Exception as e:
        print("Exception while trying to read file", e)


def model_recommend(trained_model, number_of_recommendations, product_column_name):
    user_recs = trained_model.recommendForAllUsers(number_of_recommendations)
    column_names = [product_column_name, "rating"]

    df = user_recs.toPandas()
    aggregated_df = generate_columns(column_names, number_of_recommendations)
    df = pd.concat([df, aggregated_df], axis=1)

    for i in range(0, len(df.index)):
        result_list = df['recommendations'].values[i]
        df = concat_data_from_list(result_list, df, i, column_names)

    df = df.drop(columns=['recommendations'])
    df.to_csv(os.getcwd() + "/data/data_location/" + "user_based_recommendations.csv", index=False)
    return "/data/data_location/user_based_recommendations.csv"
