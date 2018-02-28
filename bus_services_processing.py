#!/usr/bin/python
import pandas as pd
from time import time

def replace_00_to_24_hours(bus_service_transactions, min_value):
    replace_hours = lambda time_in_seconds: time_in_seconds if time_in_seconds >= min_value else time_in_seconds + 24*3600
    bus_service_transactions['time'] = bus_service_transactions['time'].apply(replace_hours)

def convert_timestamp_to_seconds(column):
    timestamp_to_seconds = lambda timestamp: timestamp.hour*3600 + timestamp.minute*60 + timestamp.second
    return column.apply(timestamp_to_seconds)

def process_timestamp_column(bus_service_transactions, action):
    bus_service_transactions['%s_time' % action] = convert_timestamp_to_seconds(bus_service_transactions['%s_time'% action])
    boarding = bus_service_transactions[['bus_registration_number', 'bus_trip_number', '%s_station' % action, '%s_time' % action]]
    boarding.columns = ['bus_registration_number', 'bus_trip_number', 'station', 'time']
    return boarding

def process_bus_service_file(filename):
    start_time = time()
    bus_service_transactions = pd.read_csv(filename, parse_dates=[6,8], infer_datetime_format=True)

    boarding = process_timestamp_column(bus_service_transactions, 'boarding')
    alighting = process_timestamp_column(bus_service_transactions, 'alighting')

    # bus_service_transactions['boarding_time'] = convert_timestamp_to_seconds(bus_service_transactions['boarding_time'])
    # bus_service_transactions['alighting_time'] = convert_timestamp_to_seconds(bus_service_transactions['alighting_time'])
    #
    # # [bus_service,bus_registration_number,bus_trip_number,boarding_station,alighting_station,boarding_date,boarding_time,alighting_date,alighting_time]
    # boarding = bus_service_transactions[['bus_registration_number', 'bus_trip_number', 'boarding_station', 'boarding_time']]
    # alighting = bus_service_transactions[['bus_registration_number', 'bus_trip_number', 'alighting_station', 'alighting_time']]
    # boarding.columns = ['bus_registration_number', 'bus_trip_number', 'station', 'time']
    # alighting.columns = ['bus_registration_number', 'bus_trip_number', 'station', 'time']
    # print boarding
    # print "\n\n\n\n", "=" * 50, '\n\n\n'
    # print alighting
    bus_service_transactions = boarding.append(alighting)
    replace_00_to_24_hours(bus_service_transactions, bus_service_transactions.iloc[0,3])
    bus_service_transactions.dropna(inplace = True)
    bus_service_transactions.sort_values(by=['time'], inplace=True, kind='mergesort')
    print "shape: {}".format(bus_service_transactions.shape)
    bus_service_transactions.to_csv('updated/bus10_01022016', index=None, date_format='%H:%M:%S')

if __name__=='__main__':
    process_bus_service_file('grouped/bus10_01022016')
