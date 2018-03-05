#!/usr/bin/python
from pandas import to_datetime
from time import time

def handle_bus_service_transactions(bus_service_transactions):
    boarding = handle_timestamp_column(bus_service_transactions, 'boarding')
    alighting = handle_timestamp_column(bus_service_transactions, 'alighting')

    bus_service_transactions = boarding.append(alighting)
    replace_00_to_24_hours(bus_service_transactions)
    bus_service_transactions.dropna(inplace = True)
    bus_service_transactions.sort_values(by=['time'], inplace=True, kind='mergesort')

    return bus_service_transactions

def handle_timestamp_column(bus_service_transactions, action):
    bus_service_transactions['%s_time' % action] = bus_service_transactions[['%s_date' % action, '%s_time' % action]].apply(convert_to_seconds, axis=1)
    action_transactions = bus_service_transactions[['bus_registration_number', 'bus_trip_number', '%s_station' % action, '%s_time' % action]]
    action_transactions.columns = ['bus_registration_number', 'bus_trip_number', 'station', 'time']
    return action_transactions

def convert_to_seconds(pair):
    date, time = pair
    start_time = to_datetime('1/2/2016 00:00:00', dayfirst=True)
    try:
        timestamp =  to_datetime('%s %s' % (str(date), str(time)), dayfirst=True, errors='raise')
        return int((timestamp - start_time).total_seconds())
    except ValueError as e:
        return None

def seconds_to_datetime(seconds_value):
    x= pd.to_datetime('01-02-2016 00:00:00', dayfirst=True)
    return x + pd.Timedelta(seconds=seconds_value)

def replace_00_to_24_hours(bus_service_transactions, min_value=16200):
    replace_hours = lambda time_in_seconds: time_in_seconds if time_in_seconds >= min_value else time_in_seconds + 24*3600
    bus_service_transactions['time'] = bus_service_transactions['time'].apply(replace_hours)

def apply_medians(bus_service_transactions):
    pass

if __name__=='__main__':
    # handle_bus_service_transactions('grouped/bus10_01022016')
    pass
