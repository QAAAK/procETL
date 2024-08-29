from mtspark import get_spark
from onetl.connection import Postgres, Greenplum
from onetl.db import DBReader, DBWriter
from py4j.protocol import Py4JJavaError 



spark_session = spark_config()
if spark_session:
    print("Spark session created successfully")
else:
    print("Failed to Spark session")
extra = {
    'tcpKeepAlive': 'true',
    'server.port': '49152-65535',
    'pool.maxSize': 10
}


def spark_config():
  
    """
        The Method creates spark configuration considering the number of cores used for calculations, the amount of allocated memory 
            
        :return: customized spark configuration
            
    """

    try: 
        spark = get_spark(
        config = 
        {
        "appName": "mysessionname",  # название сессии
        "spark.jars.packages": Postgres.get_packages() + Greenplum.get_packages(spark_version = "3.2.0"),
        'spark.ui.showConsoleProgress': True,
        'spark.sql.execution.arrow.pyspark.enabled': True,
        "spark.driver.memory": "2g",
        "spark.driver.cores": 2, #Задаем только в Cluster Mode 
        "spark.executor.cores": 6,
        "spark.executor.memory": "20g",
        "spark.submit.deploymode": "client",
        "spark.dynamicAllocation.enabled": True,
        "spark.dynamicAllocation.initialExecutors": 1,
        "spark.dynamicAllocation.minExecutors": 0,
        "spark.dynamicAllocation.maxExecutors": 8,
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        }, 
        spark_version="local",  # версия Spark, он должен быть установлен на edge ноду в /opt/spark
        )

    except Py4JJavaError as e:
        spark = None
        print(f"An error occured:{e}")

    return spark


def source_connection():
      
    """
        The method returns the connection to the source database 
            
        :return: the connection to the source database
            
    """

    try:
        dwh = Postgres(
            host="gp-mis-dwh.pv.mts.ru",
            user="santalovdv",
            password="KristyNik1092",
            database="dwh",
            spark=self.spark,
            extra = self.extra
        ).check()

    except Exception as e:
        dwh = None 
        print(f"An error occured: {e}")

    return dwh

def target_connection():

    """
        The method returns the connection to the destination database
            
        :return: connection to the destination database
            
    """
    try:
        core = Greenplum(
            host="beta-greenplum.bd-cloud.mts.ru",
            user="santalovdv",
            password="KristyNik1092",
            database="core",
            spark=self.spark,
            extra=self.extra
        ).check()

    except Exception as e:
        core=None
        print(f"an error occured: {e}")

    return core


def insert_query(query : str, part_column : str = None):
  
    """
        The method accepts a query body and optionally a partition column to create a query and write to the output of a dataframe with output data

        :param query: SQL Query

        :param part_column: Partition column
            
        :return: DataFrame with output data
            
    """

    try:
        if part_column == None:
                df = dwh.sql(f"{query}")
        else:
                df = dwh.sql(f"{query}", 
                options = Postgres.SQLOptions(
                    partition_column=part_column,
                    num_partitions=8,
                    lower_bound=0,
                    upper_bound=100
                    )
                )
    except Py4JJavaError as e:
        df=None
        print(f"an error occured: {e}")

    return df

def write_to_DB():
    
    """
        The method writes the data that is stored in the data frame to a table
            
        :return: Nothing        
    """ 
    try:
        writer = DBWriter(
            connection=core,
            target="dds.operotchet_final",
            options = Greenplum.WriteOptions(
            if_exists="append",
            truncate="false",
            #distributedBy="regid"
            ),
        )
    except Exception as e:
            print(f"an error occured: {e}")
            return None
            
    writer.run(df)