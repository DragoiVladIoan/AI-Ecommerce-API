import os


def remove_data_from_data_location():
    for file in os.listdir(os.getcwd() + "/data/data_location"):
        file_path = os.getcwd() + "/data/data_location/" + file
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

