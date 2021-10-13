import os

from pyspark import StorageLevel

def read_data(spark,input_path):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    if os.path.isdir(input_path) and os.access(input_path, os.R_OK):
        df = spark\
            .read\
           .json(input_path)
        return df
    else:
    	raise CustomException(f'Unable to access input directory : {input_path}')

def pre_process(df):
    return df
    
def extract(spark, config, log):
    """reads the json files, pre process and persist the dataframe.
       parameters spark : spark session
                  config : configuration dict
                  log : logger object
    """
    try:
    	data = read_data(spark, config['input_path'])
    	data = pre_process(data)
    	data = data.persist(StorageLevel.MEMORY_AND_DISK)    #cited from pyspark documentation.
    	return data
    except Exception as e:
        log.error(e)
        log.error(f'''!!!Unable to read input files from path : {config['input_path']}, exiting etl!!!''')
        return False
