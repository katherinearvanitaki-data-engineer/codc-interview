from main_pyspark import filtering_data
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd

def test_filter_spark_data_frame_by_value():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    spark = SparkSession.builder.master("local[1]") \
        .appName('SparkByExamples.com') \
        .getOrCreate()

    # Input and output dataframes
    input = sql_context.createDataFrame(
        [('France', 'rdrinanh@odnoklassniki.ru'),
         ('France', 'wbamfordv@t-online.de'),
         ('France', 'erosengrenx@frt.com'),
         ('United States', 'vnapthine3j@usatoday.com'),
         ('United Kingdom', 'pvolette5g@ask.com'),
         ('Netherlands', 'dbuckthorpz@tmall.com')],
        ['country', 'email'],
    )
    expected_output = sql_context.createDataFrame(
        [('United Kingdom', 'pvolette5g@ask.com'),
         ('Netherlands', 'dbuckthorpz@tmall.com')],
        ['country', 'email'],
    )
    real_output = filtering_data(input, 'country', ['United Kingdom', 'Netherlands'])
    real_output = get_sorted_data_frame(
        real_output.toPandas(),
        ['country', 'email'],
    )
    expected_output = get_sorted_data_frame(
        expected_output.toPandas(),
        ['country', 'email'],
    )
    print('expected_output: ')
    print(expected_output)
    print('real_output: ')
    print(real_output)

    # Equality assertion
    pd.testing.assert_frame_equal(expected_output, real_output)

def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

if __name__ == '__main__':
    test_filter_spark_data_frame_by_value()