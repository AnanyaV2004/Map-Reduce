# mapreduce.py

from mpi4py import MPI
import os
from task import task

class mapreduce_imp:

    def __init__ (self, input_dir):
        self.__input_dir = input_dir
        self.tasks = []

    def check_valid_file(self, file_path):

        def is_txt_file(file_path):
            # Split the file name and its extension
            _, file_extension = os.path.splitext(file_path)

            # Check if the file extension is '.txt'
            return file_extension.lower() == '.txt'

        if (os.path.isfile(file_path) and is_txt_file(file_path)):
            return True
        
        return False
    
    
    # input path directory given
    def make_tasks_array(self):
        id = 0
        for filename in os.listdir(self.__input_dir):
            
            filepath = os.path.join(self.__input_dir, filename)

            # checking if it is a .txt file.
            if self.check_valid_file(filepath):

                with open('filename.txt', 'r') as file:    
                    for line in file:
                        id +=1 
                        task = task(id, line)
                        self.tasks.push(task)
 
        
        





