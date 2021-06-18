import os

import numpy as np
import pandas as pd

from misc.misc import is_int, is_float


def transform_string_into_df(content):
    content_list = content.split("\n")
    columns = content_list[0].split(",")
    rows = []
    for i in range(1, len(content_list)):
        row = content_list[i].split(",")
        for i in range(0, len(row)):
            if is_int(str(row[i])):
                row[i] = int(float(str(row[i])))
            elif is_float(str(row[i])):
                row[i] = float(str(row[i]))
            else:
                row[i] = str(row[i])
        rows.append(row)
    df = pd.DataFrame(data=np.array(rows), columns=columns)
    return df


def min_max_normalization(df, columns_dict):
    df_matrix = pd.pivot_table(df, values=columns_dict['values'], index=columns_dict['index'], columns=columns_dict['columns'])
    df_matrix_norm = (df_matrix - df_matrix.min()) / (df_matrix.max() - df_matrix.min())
    d = df_matrix_norm.reset_index()
    d.index.names = [columns_dict['values']+'_SCALED']
    return pd.melt(d, id_vars=[columns_dict['index']], value_name=columns_dict['values']+'_Scaled').dropna()


def build_dic(df):
    columns = list(df.columns.values)
    return {'values': columns[2], 'index': columns[0], 'columns': columns[1]}


def save_df(df, file_name):
    df.to_csv(os.getcwd() + "/data/data_location/" + file_name, index=False)
    return file_name


def clean_and_save(file_content, min_max, file_name):
    df = transform_string_into_df(file_content)
    columns_dic = build_dic(df)
    if min_max:
        df = min_max_normalization(df, columns_dic)
    file_name = save_df(df, file_name)
    return df, file_name