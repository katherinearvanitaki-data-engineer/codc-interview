#imports
import pandas as pd
import argparse

def read_input():

    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_one', '-dataset_one',  help="Pass the dataset_one csv file name ", type=str, default="dataset_one.csv")
    parser.add_argument('--dataset_two', '-dataset_two',  help="Pass the dataset_two file name", type=str, default="dataset_two.csv")
    parser.add_argument('--countries_list', '-countries_list', help="Pass the countries to filter comma(,) separated", type=str, default="")

    args = parser.parse_args()
    dataset_one = args.dataset_one
    dataset_two = args.dataset_two
    countries_list = args.countries_list
    print('dataset_one: ', dataset_one, 'dataset_two: ',dataset_two, 'countries_list: ',countries_list)
    countries_list_split = [s for s in countries_list.split(",")]
    print('countries_list_split: ', countries_list_split)

    dataset_one_pd = pd.read_csv(dataset_one)
    dataset_two_pd = pd.read_csv(dataset_two)

    return dataset_one_pd, dataset_two_pd, countries_list_split

def remove_columns(dataset_pd, list_columns_to_remove):
    try:
        print("")

        df = dataset_pd.drop(list_columns_to_remove, axis=1)

    except Exception as e:
        df = []
        raise Exception( "Error when filtering data: " + str(e))

    return df

def filtering_data(dataset_pd, column_tofilter, values_tofilter):

    try:
        if values_tofilter  == '':
            dataset_pd_filtered = dataset_pd.copy()
        else:
            dataset_pd_filtered = dataset_pd[dataset_pd[column_tofilter].isin(values_tofilter)].reset_index(drop=True)

    except Exception as e:
        dataset_pd_filtered = []
        raise Exception( "Error when filtering data: " + str(e))

    return dataset_pd_filtered

def renaming_columns(dataset_pd, old_column_names, new_column_names):

    try:
        print("")
        for i,j in zip(old_column_names, new_column_names):
            dataset_pd = dataset_pd.rename({i: j}, axis=1)  # new method

    except Exception as e:
        raise Exception( "Error when filtering data: " + str(e))

    return dataset_pd

def join_datasets(dataset_one_pd, dataset_two_pd, value_to_join):

    try:
        joined_pd = pd.merge(dataset_one_pd, dataset_two_pd, on=value_to_join)

    except Exception as e:
        raise Exception( "Error when filtering data: " + str(e))

    return joined_pd

if __name__ == '__main__':
    dataset_one_pd, dataset_two_pd, countries_list_split = read_input()

    dataset_one_pd_rmcl = remove_columns(dataset_one_pd, ['first_name','last_name'])
    dataset_two_pd_rmcl = remove_columns(dataset_two_pd, ['cc_n'])

    print('dataset_one_pd_rmcl')
    print('')
    print(dataset_one_pd_rmcl.head(2))

    print('dataset_two_pd_rmcl')
    print('')
    print(dataset_two_pd_rmcl.head(2))

    joined_pd = join_datasets(dataset_one_pd_rmcl, dataset_two_pd_rmcl, 'id')
    print('joined_pd')
    print('')
    print(joined_pd.head(2))

    dataset_pd_filtered = filtering_data(joined_pd, 'country', countries_list_split)

    print('dataset_pd_filtered')
    print('')
    print(dataset_pd_filtered.head(2))

    old_column_names= ['id', 'btc_a', 'cc_t']
    new_column_names= ['client_identifier', 'bitcoin_address', 'credit_card_type']

    renaming_columns_pd = renaming_columns(dataset_pd_filtered, old_column_names, new_column_names)

    print('renaming_columns_pd')
    print('')
    print(renaming_columns_pd.head(2))

    renaming_columns_pd.to_csv('./client_data/results.csv')
