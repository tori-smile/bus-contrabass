#!/usr/bin/python
import pandas as pd
from time import time
from os.path import basename
from helper import create_directory_if_doesnt_exist

def handle_transaction_file(filepath, columns_are_already_removed=True):
    transactions = read_card_transaction_file(filepath, columns_are_already_removed)
    date = basename(filepath)[:10]
    return split_transactions_into_different_bus_services(transactions, date)

def read_card_transaction_file(filepath, columns_are_already_removed=True):
    transactions = pd.read_csv(filepath)
    # transactions = pd.read_csv(filepath, header=None)
    if not columns_are_already_removed:
        transactions = transactions[transactions[3] != 'RTS']
        transactions.drop(transactions.columns[[0, 1, 2, 3, 6, 14]], axis = 1, inplace = True)
    transactions.columns = ['bus_service', 'bus_registration_number', 'bus_trip_number', 'boarding_station', 'alighting_station','boarding_date', 'boarding_time', 'alighting_date','alighting_time']
    return transactions

def split_transactions_into_different_bus_services(transactions, date):
    bus_service_groups = transactions.groupby(['bus_service'])
    get_group = bus_service_groups.get_group
    return [(group_name, get_group(group_name), date) for group_name in bus_service_groups.groups]

def write_to_files(pair, directory_name='test_folder'):
    create_directory_if_doesnt_exist(directory_name)
    pair[1].to_csv("%s/bus%s_06022016" % (directory_name, pair[0]), sep=',', index=None)
    print '[END] writing bus %s' % pair[0]

if __name__=='__main__':
    grouped_transactions =  handle_transaction_file('/home/student/DailyTripsFeb-2016/2016-02-06-EZ.csv')
    map(write_to_files, grouped_transactions)
