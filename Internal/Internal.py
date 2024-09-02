import pyspark
import onetl
from py4j.protocol import Py4JJavaError



class Internal:
    
    def __init__(self, source_table:str):
        
        self.__source_table = source_table
    
    
    def cnt_rows (self,data_frame: pyspark.sql.dataframe.DataFrame):
        
        """
            Method for counting rows in a dataframe

            :param data_frame: Spark DataFrame

            :return: quantity rows

        """

        try:
             cnt_rows = data_frame.count()

        except TypeError:

            cnt_rows = None

            # print ('DataFrame does not match type Spark')

        return cnt_rows

    
    def round_upper_bound(self, number : int):
             
        """
            A method that calculates the maximum value of the partitioning field.

            :param number: Number

            :return: round Integer number       
        """
        
        try:

            upper_bound = ((number + 9) // 10) * 10

        except TypeError:

            upper_bound = None

            # print(('Param does not match type Integer'))

        return upper_bound
                

    def max_upper_bound(self, conn : onetl.connection, part_column : str = None):

        """
            A method that calculates the maximum value of the partitioning field.

            :param conn: connection DataBase
            :param part_column : partition column (default None)

            :return: max size partition column     
        """

        if part_column != None:

            try:
                max_size = conn.sql(f"select max({part_column}) from {self.__source_table}").head()[0] # add when creating a class
                max_size = self.__round_upper_bound(max_size)

            except Py4JJavaError:

                max_size = None

                # print('Sql query compilation error')


        return max_size


    def _step_batch_insert(self, cnt_rows: int):
        
        """
           The method that determines the size of the batch

            :param cnt_rows: 

            :return: max size partition column     
        """
        
        batch = None
        
        if (cnt_rows >= 10_000_000) and (cnt_rows <= 100_000_000):
            batch = 2_000_000
        elif (cnt_rows > 100_000_000) and (cnt_rows < 500_000_000):
            batch = 5_000_000
        else:
            batch = 1_000_000
        
        return batch     
            
        
