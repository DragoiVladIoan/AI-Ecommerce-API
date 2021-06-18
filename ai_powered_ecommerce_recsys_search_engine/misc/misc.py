import pandas as pd


def generate_columns(column_names, number_of_recommendations):
    columns = []
    for i in range(0, number_of_recommendations):
        columns.append('TOP ' + str(i + 1) + ' ' + column_names[0])
        columns.append('TOP ' + str(i + 1) + ' ' + column_names[1])
    return pd.DataFrame(columns=columns)


def concat_data_from_list(my_list, df, current_index, column_names):
    i = 0
    for row in my_list:
        df.at[current_index, 'TOP ' + str(i + 1) + ' ' + column_names[0]] = row[column_names[0]]
        df.at[current_index, 'TOP ' + str(i + 1) + ' ' + column_names[1]] = row[column_names[1]]
        i += 1
    return df


def apply_hash(x):
    try:
        int(x)
        return x
    except Exception:
        return abs(int(hash(x)/1000000000000))


def apply_abs(x):
    return abs(int(x))


def is_float(x):
    try:
        a = float(x)
    except ValueError:
        return False
    else:
        return True


def is_int(x):
    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return True
