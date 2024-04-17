from datetime import timedelta

class specs:
    def __init__(self,num_mappers, num_reducers):
        self.__ping_frequency = timedelta(milliseconds=50)
        self.__ping_failure_time = timedelta(milliseconds=2000)
        self.__num_mappers = num_mappers
        self.__num_reducers = num_reducers
    
    def get_ping_frequency(self):
        return self.__ping_frequency
    
    def get_ping_failure_time(self):
        return self.__ping_failure_time
    
    def get_num_mappers(self):
        return self.__num_mappers
    
    def get_num_reducers(self):
        return self.__num_reducers