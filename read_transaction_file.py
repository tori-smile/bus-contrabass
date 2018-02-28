#!/usr/bin/python
import pandas as pd
from time import time
import os

def handle_transaction_file(filename):
    transactions = read_card_transaction_file(filename)
    return split_transactions_into_different_bus_services(transactions)

def read_card_transaction_file(filename):
    transactions = pd.read_csv(filename, header=None)
    transactions = transactions[transactions[3] != 'RTS']
    transactions.drop(transactions.columns[[0, 1, 2, 3, 6, 10, 12, 14]], axis = 1, inplace = True)
    transactions.columns = ['bus_service', 'bus_registration_number', 'bus_trip_number', 'boarding_station', 'alighting_station', 'boarding_time', 'alighting_time']
    return transactions

def split_transactions_into_different_bus_services(transactions):
    bus_service_groups = transactions.groupby(['bus_service'])
    get_group = bus_service_groups.get_group
    return [(group_name, get_group(group_name)) for group_name in bus_service_groups.groups] # grouped_transactions =
    # map(write_to_files, grouped_transactions)

def write_to_files(pair, directory_name='grouped'):
    create_directory_if_doesnt_exist(directory_name)
    pair[1].to_csv("%s/bus%s_01022016" % (directory_name, pair[0]), sep=',', index=None)

def create_directory_if_doesnt_exist(directory_name):
    if not os.path.isdir(directory_name):
        os.makedirs(directory_name)

if __name__=='__main__':
    handle_transaction_file('2016-02-01-EZ.csv')
