import sys
sys.path.extend(['/Users/polupanartem/Desktop/grid_capstone_project/jobs/', '/Users/polupanartem/Desktop/grid_capstone_project'])
import unittest
import logging
from pyspark.sql import SparkSession
from jobs.main_job import (
    clickstream_schema,
    purchase_schema,
    load_clickstream_data,
    load_user_purchases_data,
    transform_data,
    transform_data_with_udf,
    save_data_to_parquet,
    top_campaigns_spark_df,
    top_campaigns_sql,
    channels_engagement_spark_df,
    channels_engagement_sql
)

logger = logging.getLogger()


class SparkJobTests(unittest.TestCase):
    def setUp(self):
      self.spark = (
        SparkSession
          .builder
          .appName('test_app')
          .getOrCreate()
      )
      self.expected_result = (
        self.spark
          .read
          .parquet('data/test/transform_result/*.snappy.parquet')
      )

      self.expected_result.createOrReplaceTempView('target')

      self.input_clicks_data = (
            self.spark
                .read
                .options(header='True')
                .schema(clickstream_schema)
                .csv('data/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz'))
      self.input_purchases_data = (
          self.spark
              .read
              .options(header='True')
              .schema(purchase_schema)
              .csv('data/user_purchases/user_purchases_0.csv.gz'))

    def test_transform_data(self):
      expected_cols = len(self.expected_result.columns)
      expected_rows = self.expected_result.count()

      result = transform_data(self.input_clicks_data, self.input_purchases_data)
      
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])

    def test_transform_data_udf(self):
      input_clicks_data = (
            self.spark
                .read
                .options(header='True')
                .schema(clickstream_schema)
                .csv('data/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz'))
      input_purchases_data = (
          self.spark
              .read
              .options(header='True')
              .schema(purchase_schema)
              .csv('data/user_purchases/user_purchases_0.csv.gz'))
      expected_cols = len(self.expected_result.columns)
      expected_rows = self.expected_result.count()

      result = transform_data_with_udf(input_clicks_data, input_purchases_data)
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])

    def test_channels_engagement_spark_df(self):
      expected_result = (
            self.spark
                .read
                .parquet('data/test/channels_engagement_spark_df/*.snappy.parquet'))
      result = transform_data(self.input_clicks_data, self.input_purchases_data)
      expected_cols = len(expected_result.columns)
      expected_rows = expected_result.count()
      result = channels_engagement_spark_df(result)
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])
    
    def test_channels_engagement_spark_sql(self):
      expected_result = (
            self.spark
                .read
                .parquet('data/test/channels_engagement_spark_sql/*.snappy.parquet'))
      expected_cols = len(expected_result.columns)
      expected_rows = expected_result.count()
      result = channels_engagement_sql(self.spark)
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])
    
    def test_top_campaigns_df(self):
      expected_result = (
            self.spark
                .read
                .parquet('data/test/top_campaigns_df/*.snappy.parquet'))
      result = transform_data(self.input_clicks_data, self.input_purchases_data)
      expected_cols = len(expected_result.columns)
      expected_rows = expected_result.count()
      result = top_campaigns_spark_df(result)
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])
    
    def test_top_campaigns_spark_sql(self):
      expected_result = (
            self.spark
                .read
                .parquet('data/test/top_campaigns_sql/*.snappy.parquet'))
      expected_cols = len(expected_result.columns)
      expected_rows = expected_result.count()
      result = top_campaigns_sql(self.spark)
      cols = len(result.columns)
      rows = result.count()

      self.assertEqual(expected_cols, cols)
      self.assertEqual(expected_rows, rows)
      self.assertTrue([col in self.expected_result.columns for col in result.columns])


if __name__ == '__main__':
    unittest.main()
