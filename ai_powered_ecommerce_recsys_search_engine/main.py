import os

from cloud_config.get_data_from_cloud import get_file_cloud, load_config, upload_file_cloud
from data.data_cleanup.normalize_data import clean_and_save
from data.data_cleanup.remove_persisted_data import remove_data_from_data_location
from recommender_system.item_based_similarity import build_and_compute_item_similarity
from recommender_system.user_based_als_cf_model import build_and_train_als_model, model_recommend
from search_engine.multilabel_classification import train_multilabel_classification, save_pipeline_locally, classify_query


def rec_sys_main(user_based, item_similarity):
    if user_based or item_similarity:
        data_location = load_config()
        print(data_location["location"]["files"]["product_transactions_mock"])
        file = get_file_cloud(load_config(), "product_transactions_mock")
        df, file_name = clean_and_save(file, True, data_location["location"]["files"]["product_transactions_mock"])
        if user_based:
            recsys_user_based_cf(file_name, data_location)
        if item_similarity:
            recsys_item_similarity(file_name, 3, data_location)
        remove_data_from_data_location()


def recsys_user_based_cf(file_name, data_location):
    model, product_column_name = build_and_train_als_model(file_name=file_name)
    recommendations_file_path = model_recommend(model, 5, product_column_name)
    response = upload_file_cloud(data_location, os.getcwd() + recommendations_file_path, "user_recs")
    if response == 0:
        print("SUCCESSFULLY UPLOADED USER RECOMMENDATIONS!")
    else:
        print("FAILED TO UPLOAD USER RECOMMENDATIONS!")


def recsys_item_similarity(file_name, knn, data_location):
    rec_file_cosine, rec_file_pearson = build_and_compute_item_similarity(file_name=file_name, KNN=knn)
    response_cosine = upload_file_cloud(data_location, os.getcwd() + rec_file_cosine, "item_recs_cosine")
    response_pearson = upload_file_cloud(data_location, os.getcwd() + rec_file_pearson, "item_recs_pearson")
    if response_cosine == 0 and response_pearson == 0:
        print("SUCCESSFULLY UPLOADED BOTH TYPES OF ITEM RECOMMENDATIONS!")
    else:
        print("FAILED TO UPLOAD ONE OR MORE ITEM RECOMMENDATIONS!")


def search_engine_main(train, classify, query):
    if train or classify:
        data_location = load_config()
        if train:
            file = get_file_cloud(load_config(), "walmart_data")
            df, file_name = clean_and_save(file, False, data_location["location"]["files"]["walmart_data"])
            multilabel_classification_train(file_name, data_location)
            remove_data_from_data_location()
        if classify:
            if query is not None:
                query_classification(query)


def query_classification(user_query):
    binary_clf, chain_clf, powerset_clf = classify_query(user_query)
    print(powerset_clf)


def multilabel_classification_train(file_name, data_location):
    pipeline_binary, pipeline_chain, pipeline_powerset = train_multilabel_classification(os.getcwd() + "/data/data_location/" + file_name)
    binary_relevance_file = "binary_relevance.joblib"
    classifier_chain_file = "classifier_chain.joblib"
    label_powerset_file = "label_powerset.joblib"
    save_pipeline_locally(pipeline_binary, binary_relevance_file)
    save_pipeline_locally(pipeline_chain, classifier_chain_file)
    save_pipeline_locally(pipeline_powerset, label_powerset_file)
    response1 = upload_file_cloud(data_location, os.getcwd() + "/data/model_location/" + binary_relevance_file, "binary_relevance")
    response2 = upload_file_cloud(data_location, os.getcwd() + "/data/model_location/" + classifier_chain_file, "classifier_chain")
    response3 = upload_file_cloud(data_location, os.getcwd() + "/data/model_location/" + label_powerset_file, "label_powerset")
    if response1 == 0 and response2 == 0 and response3 == 0:
        print("SUCCESSFULLY UPLOADED ALL THREE MODELS")
    else:
        print("FAILED TO UPLOAD ONE OR MORE MODELS")


query = "(10 Pack) FBOMB Macadamia Nut Butter with Sea Salt 1 oz Packs"
search_engine_main(False, True, query)

# rec_sys_main(False, True)
