import os

import pandas as pd
from sklearn.neighbors import NearestNeighbors


def build_and_compute_item_similarity(**kwargs):
    df = kwargs.get('df')
    file = kwargs.get('file_name')
    k = kwargs.get('KNN')
    try:
        if df is not None:
            model_df = df
        else:
            model_df = pd.read_csv(os.getcwd() + "/data/data_location" + file)

        columns = list(model_df.columns.values)
        pivot_df = pd.pivot_table(model_df, values=columns[2], index=columns[0], columns=columns[1]).fillna(0)
        if k is not None:
            k = int(k)
        else:
            k = 3

        rec_df_cosine = compute_similarity_and_recommend("cosine", k, model_df, pivot_df, columns)
        rec_df_pearson = compute_similarity_and_recommend("correlation", k, model_df, pivot_df, columns)
        return save_to_file(rec_df_cosine, "cosine"), save_to_file(rec_df_pearson, "correlation")

    except Exception as e:
        print("Exception while trying to read file", e)


def compute_similarity_and_recommend(similarity_function, k, model_df, pivot_df, columns):
    sim_knn = NearestNeighbors(n_neighbors=k, algorithm="brute", metric=similarity_function)
    sim_knn_fit = sim_knn.fit(pivot_df.T.values)
    item_distances, item_indexes = sim_knn_fit.kneighbors()
    return recommend_items(pivot_df, item_indexes, columns, k)


def recommend_items(pivot_df, item_indexes, columns, k):
    recommendations_df = pd.DataFrame()

    pivot_df = pivot_df.reset_index()

    pivot_df = pivot_df.drop(columns=[columns[0]])

    for i in range(0, len(item_indexes)):
        recommendations_df.at[i, "TARGET_PRODUCT"] = pivot_df.T.index.values.tolist()[i]
        for rec in range(0, k):
            index = pivot_df.T.index.values.tolist()[item_indexes[i][rec]]
            recommendations_df.at[i, "TOP" + str(rec + 1) + "_PRODUCT"] = index
    return recommendations_df


def compute_name_similarity_and_recommend(similarity_function, pivot_df):
    pass


def save_to_file(df, function):
    df.to_csv(os.getcwd() + "/data/data_location/" + "item_based_recommendations_" + function + ".csv", index=False)
    return "/data/data_location/" + "item_based_recommendations_" + function + ".csv"


df = pd.read_csv("/Users/vladdragoi/Documents/ecommerce-data/walmart_full_data.csv")
pivot_df = pd.pivot_table(df, values=0, index="Product Name", columns="Product Name")
print(pivot_df)