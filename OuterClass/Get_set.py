def Get_info_tables(self):
    return f"source table: {self.__source_table} and target table: {self.__target_table}"

@property
def Get_source_conn(self):
    return self.__source_conn

@property
def Get_target_conn(self):
    return self.__target_conn

def Set_info_tables(self, source_connection : str = None , target_connection : str = None)
    if source_connection = None:
        self.__target_conn = target_connection
    elif target_connection = None:
        self.__source_conn = source_connection
    else:
        self.__source_conn = source_connection
        self.__target_conn = target_connection

@Set_source_conn.setter
def Set_source_conn(self, source_connection:str):
    self.__source_conn = source_connection

@Set_target_conn.setter
def Set_target_conn(self, target_connection:str):
    self.__target_conn = target_connection