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

if __name__ == '__main__':
    dataset_one_pd, dataset_two_pd, countries_list_split = read_input()
