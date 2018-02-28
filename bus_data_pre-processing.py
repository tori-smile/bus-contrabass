import argparse
from glob import glob
from multiprocessing import Pool
from time import time
import read_transaction_file

def handle_all_the_files(number_of_processes):
    default_file_pattern = '/home/student/DailyTripsFeb-2016/2016-02-*-EZ.csv'
    list_of_files = glob(default_file_pattern)
    handle_files(list_of_files, number_of_processes)

def handle_files(list_of_files, number_of_processes):
    p = Pool(number_of_processes)
    p.map(worker, list_of_files)

def worker(filename):
    start_worker_time = time()
    print "[START]\t %s" % filename
    read_transaction_file.handle_transaction_file(filename, args.directory_name)
    print "[END]\t %s after %f seconds" % (filename, time() - start_worker_time)


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='What kind of work do you prefer ?')

    parser.add_argument('-a','--all', dest='all_files_processing', action='store_true',
                        help='processing all the files. Using this option may take a lot of time')
    parser.add_argument('-l','--list', dest='list_of_files', nargs='+',
                        help='specify list of files to be processed without commas')
    parser.add_argument('-p', '--processes', dest='number_of_processes', type=int, action='store',
                        default=4, help='number of processes for files processing')
    parser.add_argument('-d', '--directory', dest='directory_name', action='store',
                        default='grouped', help='name of directory for generated files')
    args = parser.parse_args()

    if args.all_files_processing:
        handle_all_the_files(args.number_of_processes)
    elif len(args.list_of_files) > 0:
        handle_files(args.list_of_files, args.number_of_processes)
