import os

def create_directory_if_doesnt_exist(directory_name):
    if not os.path.isdir(directory_name):
        os.makedirs(directory_name)

if __name__=='__main__':
    pass
