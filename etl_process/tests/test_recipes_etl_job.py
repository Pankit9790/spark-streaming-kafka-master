"""
unit tests for the transformation steps of the ETL
job defined in etl_job.py.
"""
import unittest

import json

from dependencies.spark import start_spark
from dependencies.task2 import transform


class EtlJobTest(unittest.TestCase):

    def setUp(self):   
        self.spark, self.log, self.config = start_spark()

    def tearDown(self):
        self.spark.stop()

    def test_transform_data(self):
        """Unit testing using transform function 
           and comparing element wise, column, row
           as well.
        """

        input_data = (
            self.spark
            .read
            .json(self.config['test_input_path']))

        expected_data = (
            self.spark
            .read
            .csv(self.config['test_output_path']))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        transformed_data = transform(input_data, self.config, self.log)

        cols = len(expected_data.columns)
        rows = expected_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        
        """element wise comparison of both dataframes"""
        self.assertEqual(expected_data.subtract(transformed_data).count(), 0) 
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
