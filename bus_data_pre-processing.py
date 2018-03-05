import argparse
from glob import glob
from multiprocessing import Pool
from time import time
from helper import create_directory_if_doesnt_exist
from read_transaction_file import handle_transaction_file
from service_handling import handle_bus_service_transactions
import progressbar

def handle_all_the_files():
    default_file_pattern = 'buses_only/2016-02-*-EZ.csv'
    list_of_files = glob(default_file_pattern)
    handle_files(list_of_files)

def handle_files(list_of_files):
    start_time = time()
    for filepath in list_of_files:
        start_worker_time = time()
        print '[START]\t %s \tat %f' % (filepath, time() - start_time)
        handle_one_day_transaction_file(filepath)
        print '[END]\t %s \tafter %f seconds' % (filepath, time() - start_worker_time)

    execution_time = time() - start_time
    print '\n\nexecution time: %f  =  %d hours %d minutes %s seconds' % (execution_time, int(execution_time / 3600), int(execution_time / 60) % 60, int(execution_time) % 60)

def handle_one_day_transaction_file(filepath):
    splitted_transactions = handle_transaction_file(filepath, args.prepared_files)
    p = Pool(args.number_of_processes)
    bar = progressbar.ProgressBar(max_value=len(splitted_transactions))
    p.map(worker, splitted_transactions)

def worker((bus_service, df, date)):
    global counter
    counter += 1
    if counter % 10 == 0:
        bar.update(counter)
    start_worker_time = time()
    print '\t[START]\t %s' % bus_service
    result = handle_bus_service_transactions(df)
    print '\t[RUNNING]\t %s finished sorting' % bus_service
    create_directory_if_doesnt_exist(args.directory_name)
    result.to_csv('%s/bus%s_%s' % (args.directory_name, bus_service.strip(), date), index=None)
    number_of_passengers = calculate_number_of_passengers(df)
    create_directory_if_doesnt_exist(args.directory_name + "_number_of_passengers")
    number_of_passengers.to_csv('%s/bus%s_%s' % (args.directory_name + "_number_of_passengers", bus_service.strip(), date), index=None)
    print '\t[END]\t %s after %f seconds' % (bus_service, time() - start_worker_time)

def calculate_number_of_passengers(bus_service_transactions):
    return bus_service_transactions.groupby(['bus_registration_number', 'bus_trip_number'])['boarding_station'].count().to_frame(name='count').reset_index()

if __name__=='__main__':
    bar = progressbar.ProgressBar()
    counter = 0
    parser = argparse.ArgumentParser(description='What kind of work do you prefer ?')

    parser.add_argument('-a','--all', dest='all_files_processing', action='store_true',
                        help='processing all the files. Using this option may take a lot of time')
    parser.add_argument('-l','--list', dest='list_of_files', nargs='+', default=['only_buses/2016-02-01-EZ.csv'],
                        help='specify list of files to be processed without commas')
    parser.add_argument('-p', '--processes', dest='number_of_processes', type=int, action='store',
                        default=8, help='number of processes for files processing')
    parser.add_argument('-d', '--directory', dest='directory_name', action='store',
                        default='test_directory', help='name of directory for generated files')
    parser.add_argument('-r', '--prepared', dest='prepared_files', action='store_true',
                        help='use for partially prepared files')
    args = parser.parse_args()

    if args.all_files_processing:
        handle_all_the_files()
    elif len(args.list_of_files) > 0:
        handle_files(args.list_of_files)
