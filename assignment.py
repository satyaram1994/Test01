from pyspark.sql import SparkSession, DataFrame, functions as F
spark=SparkSession.builder.master("local[*]").getOrCreate()

def use_case_1(input_dataframe:DataFrame):
    result_df_file_exten=input_dataframe.withColumn("extention", F.split("nm", "\\.")[1])
	return result_df_file_exten
	
def use_case_2(input_dataframe: DataFrame):
    find_number_of_files=input_dataframe.select("nm", "extention").distinct() \
	     .groupBy("extention").agg(F.count("nm").alias("file_count"))
    return find_number_of_files
	
if __name__=="__main__":
    input=spark.read.json(r"D:\dataset\log.json")
	dataframe_with_extention=use_case_1(input)
	dataframe_with_extension.show()
	
	dataframe_with_extention_count=use case_2(dataframe_with_extention)
	dataframe_with_extention_count.show()
	
    
    
  