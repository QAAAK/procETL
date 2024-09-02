from ETL import ETL


etl_proc = ETL('source_table', 'target_table')

etl_proc.source_connection()
etl_proc.target_connection()

etl_proc.query_reader('select * from <table_schema>.<table_name>')
etl_proc.write_to_DB()



if __name__ == "__main__":
    print("ETL Proccess passed")
