class Partition:

    def __init__(self, partition_column:str):

        self.partition_column = partition_column


    def get_value(self, connection:ETL.__source_con, table_name:ETL.__source_table):

        df_part_column =connection.sql(f"""
            select distinct {self.partition_column} 
            from {table_name} a
            """)
        )

        return df_part_column


    def get_bin_key(self, array:[]):
        





