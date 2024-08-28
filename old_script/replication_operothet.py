from mtspark import get_spark
from onetl.connection import Postgres, Greenplum
from onetl.log import setup_logging
from onetl.db import DBReader, DBWriter
  

setup_logging()
 
# создаем сессию
spark = get_spark(
  {
    "appName": "mysessionname",  # название сессии
    "spark.jars.packages": Postgres.get_packages() + Greenplum.get_packages(spark_version = "3.2.0"),
    'spark.ui.showConsoleProgress': True,
    'spark.sql.execution.arrow.pyspark.enabled': True,
    "spark.driver.memory": "2g",
    "spark.driver.cores": 2, #Задаем только в Cluster Mode 
    "spark.executor.cores": 6,
    "spark.executor.memory": "50g",
    "spark.submit.deploymode": "client",
    "spark.dynamicAllocation.enabled": True,
    "spark.dynamicAllocation.initialExecutors": 1,
    "spark.dynamicAllocation.minExecutors": 0,
    "spark.dynamicAllocation.maxExecutors": 8,
    "spark.dynamicAllocation.executorIdleTimeout": "60s",

  },
  spark_version="local",  # версия Spark, он должен быть установлен на edge ноду в /opt/spark
)
 

print(spark.sql("select 'Hello spark'").collect()[0][0])



extra = {
    'tcpKeepAlive': 'true',
    'server.port': '49152-65535',
    'pool.maxSize': 10
}
  
# инициализируем подключение к MSSQL и проверяем его работоспособность
dwh = Postgres(
    host="gp-mis-dwh.pv.mts.ru",
    user="santalovdv",
    password="KristyNik1092",
    database="dwh",
    spark=spark,
    extra = extra
).check()

core = Greenplum(
            host="beta-greenplum.bd-cloud.mts.ru",
            user="santalovdv",
            password="KristyNik1092",
            database="core",
            spark=spark,
            extra=extra
            ).check()



writer = DBWriter(
        connection=core,
        target="dds.operotchet_final",
        options = Greenplum.WriteOptions(
            if_exists="append",
            truncate="false",
            distributedBy="regid"),
    )



for i in range(1, 26):
    
    df = dwh.sql(f"""
    select a.*
    from analyticsb2b_sb.operotchet_final a
    left join analyticsb2b_sb.sprav_sheta c on c.group_id=a.group_id and concat(c.sub_acc_trans,'00')=a.sub_acc_trans 
    left join analyticsb2b_sb.tab_sprav_tp_big b on a.tp_group_txt=b.tp_group
    where c.product_pl_new in ('Моб') and b.tp_group_big <>'M2M/IOT' and  c.product_pl_det2<>'Прочие VAS' and period != '202406' 
    and table_business_date in ('2024-08-0{i}')
    """, options = Postgres.SQLOptions(
    partition_column="regid",
    num_partitions=8,
    lower_bound=0,
    upper_bound=100
    ))
    
    writer.run(df)
    
    del df
