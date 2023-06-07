import pandas as pd
import csv


def read_dataframe_from_file(file_name, columns):
    """
    Creates the dataframe (under the name file_name) with the column names indicated.
    :param file_name: name of the dataframe.
    :param columns: names of the columns of the dataframe.
    """
    data_df = pd.read_csv(file_name)
    data_df.columns = columns
    return data_df


def create_csv(file_name, rows_to_write):
    """
    Creates the csv (under the name file_name) with the rows indicated.
    :param file_name: name of the file to create locally.
    :param rows_to_write: csv rows to write in the file.
    """
    fp = open(file_name, 'w')
    file_readings = csv.writer(fp)
    file_readings.writerows(rows_to_write)
    fp.close()
