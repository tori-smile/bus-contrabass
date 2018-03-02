import argparse
from glob import glob
from multiprocessing import Pool
from time import time
import read_transaction_file
from service_handling import handle_bus_service_transactions

def handle_all_the_files(number_of_processes):
    default_file_pattern = 'buses_only/2016-02-*-EZ.csv'
    list_of_files = glob(default_file_pattern)
    handle_files(list_of_files, number_of_processes)

def handle_files(list_of_files, number_of_processes):
    start_time = time()
    for filename in list_of_files:
        start_worker_time = time()
        print '[START]\t %s \tat %f' % (filename, time() - start_time)
        handle_one_day_transaction_file(filename, number_of_processes)
        print '[END]\t %s \tafter %f seconds' % (filename, time() - start_worker_time)

    execution_time = time() - start_time
    print '\n\nexecution time: %f  =  %d hours %d minutes %s seconds' % (execution_time, int(execution_time / 3600), int(execution_time / 60) % 60, int(execution_time) % 60)

def handle_one_day_transaction_file(filename, number_of_processes):
    splitted_transactions = read_transaction_file.handle_transaction_file(filename)
    p = Pool(number_of_processes)
    p.map(worker, splitted_transactions)

def worker((bus_service, df)):
    start_worker_time = time()
    print '\t[START]\t %s' % bus_service
    result = handle_bus_service_transactions(df, bus_service)
    print '\t[RUNNING]\t %s finished sorting' % bus_service
    # read_transaction_file.handle_transaction_file(filename, args.directory_name)
    print '\t[END]\t %s after %f seconds' % (bus_service, time() - start_worker_time)


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='What kind of work do you prefer ?')

    parser.add_argument('-a','--all', dest='all_files_processing', action='store_true',
                        help='processing all the files. Using this option may take a lot of time')
    parser.add_argument('-l','--list', dest='list_of_files', nargs='+', default=['2016-02-01-EZ.test.csv'],
                        help='specify list of files to be processed without commas')
    parser.add_argument('-p', '--processes', dest='number_of_processes', type=int, action='store',
                        default=8, help='number of processes for files processing')
    parser.add_argument('-d', '--directory', dest='directory_name', action='store',
                        default='grouped', help='name of directory for generated files')
    args = parser.parse_args()

    if args.all_files_processing:
        handle_all_the_files(args.number_of_processes)
    elif len(args.list_of_files) > 0:
        handle_files(args.list_of_files, args.number_of_processes)
