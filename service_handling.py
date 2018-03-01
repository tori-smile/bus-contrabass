#!/usr/bin/python
import pandas as pd
from time import time

def handle_bus_service_transactions(bus_service_transactions):
    boarding = handle_timestamp_column(bus_service_transactions, 'boarding')
    alighting = handle_timestamp_column(bus_service_transactions, 'alighting')

    bus_service_transactions = boarding.append(alighting)
    replace_00_to_24_hours(bus_service_transactions)
    bus_service_transactions.dropna(inplace = True)
    bus_service_transactions.sort_values(by=['time'], inplace=True, kind='mergesort')
    return bus_service_transactions
    # print "shape: {}".format(bus_service_transactions.shape)
    # bus_service_transactions.to_csv('updated/bus10_01022016', index=None, date_format='%H:%M:%S')

def handle_timestamp_column(bus_service_transactions, action):
    bus_service_transactions['%s_time' % action] = pd.to_datetime(bus_service_transactions['%s_time' % action])
    bus_service_transactions['%s_time' % action] = convert_timestamp_to_seconds(bus_service_transactions['%s_time'% action])
    boarding = bus_service_transactions[['bus_registration_number', 'bus_trip_number', '%s_station' % action, '%s_time' % action]]
    boarding.columns = ['bus_registration_number', 'bus_trip_number', 'station', 'time']
    return boarding

def convert_timestamp_to_seconds(column):
    timestamp_to_seconds = lambda timestamp: timestamp.hour*3600 + timestamp.minute*60 + timestamp.second
    return column.apply(timestamp_to_seconds)

def replace_00_to_24_hours(bus_service_transactions, min_value=16200):
    replace_hours = lambda time_in_seconds: time_in_seconds if time_in_seconds >= min_value else time_in_seconds + 24*3600
    bus_service_transactions['time'] = bus_service_transactions['time'].apply(replace_hours)

def apply_medians(bus_service_transactions):
    pass

if __name__=='__main__':
    # handle_bus_service_transactions('grouped/bus10_01022016')
    pass
