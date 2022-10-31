import argparse
from pyspark.sql import SparkSession
import logging

def read_input():
    """
    This method processes the imput parameters and reads the 2 input files as dataframes

    Returns:
    dataset_one_pd: dataset_one input file as a Pandas dataframe
    dataset_two_pd: dataset_two input file as a Pandas dataframe
    countries_list_split: The list of countries from the input

    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_one', '-dataset_one',  help="Pass the dataset_one csv file name ", type=str, default="dataset_one.csv")
    parser.add_argument('--dataset_two', '-dataset_two',  help="Pass the dataset_two file name", type=str, default="dataset_two.csv")
    parser.add_argument('--countries_list', '-countries_list', help="Pass the countries to filter comma(,) separated", type=str, default="")

    args = parser.parse_args()
    dataset_one = args.dataset_one
    dataset_two = args.dataset_two
    countries_list = args.countries_list
    logging.info(' dataset_one: %s, dataset_two: %s',dataset_one, dataset_two)
    countries_list_split = [s for s in countries_list.split(",")]
    dataset_one_pd = spark.read.option("header",True).csv(dataset_one)
    dataset_two_pd = spark.read.option("header",True).csv(dataset_two)

    logging.info(' dataset_one_pd: %s',dataset_one_pd.head())
    logging.info(' dataset_two_pd: %s',dataset_two_pd.head())
    logging.info(' countries_list_split: %s',countries_list_split)

    return dataset_one_pd, dataset_two_pd, countries_list_split

def remove_columns(dataset_pd, list_columns_to_remove):
    """
    Input:
    dataset_pd: input dataframe from which we want to exclude columns,
    list_columns_to_remove: columns to in a list []

    This method excludes the columns in list_columns_to_remove from the dataframe dataset_pd.

    Returns:
    df: The input dataframe excluding the columns in list_columns_to_remove parameter.

    """
    try:
        for i in list_columns_to_remove:
            dataset_pd = dataset_pd.drop(i)

    except Exception as e:
        raise Exception( "Error when removing columns: " + str(e))

    return dataset_pd

def filtering_data(dataset_pd, column_tofilter, values_tofilter):
    """
    Input:
    dataset_pd: input dataframe from which we want to exclude columns,
    column_tofilter: column to filter on,
    values_tofilter: value to filter on the column column_tofilter

    This method excludes the values in the parameter values_tofilter for the column in column_tofilter.

    Returns:
    dataset_pd_filtered: The input dataframe excluding the values in the values_tofilter parameter.

    """

    try:
        if values_tofilter  == '':
            dataset_pd_filtered = dataset_pd.copy()
        else:
            dataset_pd_filtered = dataset_pd[dataset_pd[column_tofilter].isin(values_tofilter)]

    except Exception as e:
        dataset_pd_filtered = []
        raise Exception( "Error when filtering data: " + str(e))

    return dataset_pd_filtered

def renaming_columns(dataset_pd, old_column_names, new_column_names):
    """
    Input:
    dataset_pd: input dataframe from which we want to exclude columns,
    old_column_names: list of old column names,
    new_column_names: list of new column names,

    This method takes as input the dataframe dataset_pd and renames the columns from the list old_column_names,
    to the column names in new_column_names.

    Returns:
    dataset_pd: The input dataframe with the renamed columns.
    """

    try:
        for i,j in zip(old_column_names, new_column_names):
            dataset_pd = dataset_pd.withColumnRenamed(i, j)

    except Exception as e:
        raise Exception( "Error when renaming columns: " + str(e))

    return dataset_pd

def join_datasets(dataset_one_pd, dataset_two_pd, value_to_join):
    """
    Input:
    dataset_one_pd: input left dataframe to join,
    dataset_two_pd: input right dataframe to join,
    value_to_join: value to join on the 2 input dataframes,

    This method joins the datasets dataset_one_pd and dataset_two_pd on value_to_join.

    Returns:
    joined_pd: The joined dataframe
    """

    try:
        joined_pd = dataset_one_pd.join(dataset_two_pd, [value_to_join])

    except Exception as e:
        raise Exception( "Error when joining dataframes: " + str(e))

    return joined_pd

if __name__ == '__main__':

    #logging.basicConfig(filename='logs.log', level=logging.INFO)

    logging.basicConfig(filename='logs.log', format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    spark = SparkSession.builder.master("local[1]") \
        .appName('SparkByExamples.com') \
        .getOrCreate()

    dataset_one_pd, dataset_two_pd, countries_list_split = read_input()

    dataset_one_pd_rmcl = remove_columns(dataset_one_pd, ['first_name','last_name'])
    dataset_two_pd_rmcl = remove_columns(dataset_two_pd, ['cc_n'])

    logging.info(' dataset_one_pd_rmcl: %s',dataset_one_pd_rmcl.head())
    logging.info(' dataset_two_pd_rmcl: %s',dataset_two_pd_rmcl.head())

    joined_pd = join_datasets(dataset_one_pd_rmcl, dataset_two_pd_rmcl, 'id')
    logging.info(' joined_pd: %s',joined_pd.head())


    dataset_pd_filtered = filtering_data(joined_pd, 'country', countries_list_split)
    logging.info(' dataset_pd_filtered: %s',dataset_pd_filtered.head())

    old_column_names= ['id', 'btc_a', 'cc_t']
    new_column_names= ['client_identifier', 'bitcoin_address', 'credit_card_type']

    renaming_columns_pd = renaming_columns(dataset_pd_filtered, old_column_names, new_column_names)

    rows = renaming_columns_pd.count()
    logging.info(' DataFrame Rows count : %s', rows)

    """
    Output the final dataframe in the file results.csv in the folder client_data.
    """
    renaming_columns_pd.toPandas().to_csv('client_data/results_pyspark.csv')