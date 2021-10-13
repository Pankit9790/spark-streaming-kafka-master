"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from pyspark.sql.functions import col, when, regexp_replace, avg, lower
from pyspark import StorageLevel

from dependencies.spark import start_spark
from dependencies.task1 import extract
from dependencies.task2 import transform, load

import os

def main():
    """etl logic implementation.

    :return: None
    """
    #Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['/usr/local/airflow/etl/etl_process/configs/etl_config.json'])

    log.warn('Spark session started')

    #ETL pipeline
    log.info('starting task1 etl')
    data = extract(spark, config, log)
    if data:
        log.info('task1 of the etl successfully completed')
        log.info('starting task2 etl')
        data = transform(data, config, log)
        if data:
            log.info('transformation of the data successfully completed')
            load(data, config['output_path'], log)
            log.warn('all tasks are completed')
        else:
            log.error('!!!task2 failed, exiting etl!!!')
    else:
        log.error('!!!task1 failed, exiting etl!!!')

    
    spark.stop()
    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
