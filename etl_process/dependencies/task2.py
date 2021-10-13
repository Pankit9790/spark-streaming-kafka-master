from pyspark.sql.functions import when, avg, lower, regexp_replace, col, udf
from pyspark.sql.types import IntegerType

from operator import add

from functools import reduce

from dependencies.utils import path_valid, time_convertor

def str_match(df, string, case_sensitive):
    """Finds and filters as per the given string in the given dataframe.
    
       parameters df : Spark DataFrame
                  string : string to be searched
                  case_sensitive : Boolean (for case sensitive match)
       Returns : Spark DataFrame
    """
    if case_sensitive:
    	df = df.withColumn('ingredients', lower(col('ingredients')))
    else: pass
    
    return df.filter(df.ingredients.like(f"%{string}%"))
    
def time_cal(df, time_col):
    """Extracts time from time columns and add the same in new column.
    
       parameters df : Spark DataFrame
                  time_col : list of time columns
                  
       Returns : Spark DataFrame
    """
    time_parser = udf(time_convertor, IntegerType())    
    for col_name in time_col:
        df = df.withColumn(col_name,time_parser(df[col_name]))

    return df.withColumn('total_cook_time',reduce(add, [col(x) for x in time_col]))
    
def time_avg(df, col1, col2):
    """Calculates average of the col2 grouped by col1.
       
       parametres df : Spark DataFrame
                  col1 : column name (groupby column)
                  col2 : column name (aggregrate column)
       Returns : DataFrame
    """
    df_avg = df.groupby(col1).agg(avg(col(col2)))
    col2 = 'avg(' + col2 + ')'
    df_avg = df_avg.withColumnRenamed(col2,"avg_total_cooking_time")
    
    return df_avg

def add_diff_category(df):
    """Adds difficulty categories as per pre-defined criteria.
       
       parameters  df : Spark Dataframe.
       
       Returns : Spark DataFrame	    
    """    
    return df.withColumn(
               'difficulty',
                when((col("total_cook_time") <= 30), 'easy')\
               .when((col("total_cook_time") <= 60), 'medium')\
               .when((col("total_cook_time") > 60), 'hard')\
               .otherwise(0)
                        )

def write_data(df,output_path):
    """Collect data locally and write to CSV.

    :Parameters df : DataFrame to be written as CSV.
    		 output_path : path of output directory.
    """
    if path_valid(output_path):
    	(df
     	.coalesce(1)
     	.write
     	.option("header","true")
     	.csv(output_path, mode='overwrite'))
    else:
    	raise CustomException(f'Unable to write CSV to output directory : {output_path}')
    return None

def transform(data, config, log):
    try:
        data = data.unpersist()
        data = str_match(data, config['ingredient'], config['case_sensitive'])
        log.info('data filtered as per given ingredient')
        if data.count() == 0:
            log.warn(f'''!!!No data found for ingredient : {config['ingredient']}!!!''')
        try:
            data = time_cal(data, config['time_col'])
            log.info('cook time calculated and saved in new column')
            try:
                data = add_diff_category(data)
                log.info('difficulty category added in new column')                
                try:
                    data = time_avg(data, "difficulty", "total_cook_time")
                    log.info('time average as per difficulty calculated')
                    return data
                except Exception as e:
                    print(e)
                    log.error(f'''!!!Error encountered in aggregration of total_cook_time,\
 dataframe length : {data.count()}!!!''')
            except Exception as e:
                print(e)
                log.error(f'''!!!Error encountered, difficulty category cant be \
                        added, dataframe length : {data.count()}!!!''')
        except Exception as e:
           #matching cook time columns in dataframe for respective error raising.
            if list(set(config['time_col']) & set(df.columns)) == config['time_col']:
                print(e)
                log.error(f'''!!!Error occured due to format mismatch while\
                    calculating cook time using\
                    columns : {config['time_col']}!!!''')
            else:
                print(e)
                log.error(f'!!!cook time columns not found in dataframe!!!')
    except Exception as e:
        print(e)
        log.error(f'''!!!Error encountered while filtering\
 dataframe for ingredient : {config['ingredient']}!!!, exiting etl''')
 
def load(data, path, log):
    try:
        write_data(data, path)
        log.info(f'''dataframe persisted as CSV on directory : {path}''')
        return True
    except Exception as e:
        print(e)
        log.error(f'Failed to persist the dataframe') 
