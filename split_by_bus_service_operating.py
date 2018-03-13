import pandas as pd
from glob import glob
from time import time, gmtime, strftime
import progressbar


def handle_files(filepattern):
    start_time = time()
    files = glob(filepattern)
    bar = progressbar.ProgressBar()
    for filename in bar(files):
        # print filename
        bus_transactions = pd.read_csv(filename)
        bus_transactions['datetime'] = bus_transactions['time'].apply(to_datetime)
        try:
            current_date = bus_transactions.iloc[0]['datetime']
        except IndexError as e:
            print filename
            continue

        value = get_filter_value(current_date)
        append_df = bus_transactions[bus_transactions.time < value]
        bus_transactions = bus_transactions[bus_transactions.time >= value]
        bus_transactions.to_csv(filename, index=None)
        date = get_previous_date(current_date)
        append_filename = filename[:-10] + date
        # print "\t->: %s" % append_filename

        with open('%s' % append_filename, 'a') as f:
            append_df.to_csv(f, header=False, index=None)

        # print bus_transactions

def to_datetime(seconds):
    return pd.Timestamp('2016-02-01') + pd.Timedelta(seconds=seconds)

def get_filter_value(value):
    return (pd.to_datetime(value.date())+ pd.Timedelta(seconds=3600*4.5) - pd.to_datetime('2016-02-01')).total_seconds()

def change_files(filepattern):
    files = glob(filepattern)
    for filename in files:
        bus_transactions = pd.read_csv(filename)
        bus_transactions['datetime'] = bus_transactions['time'].apply(to_datetime)
        bus_transactions.to_csv(filename, index=None)

def get_previous_date(current_time):
    return str((current_time - pd.Timedelta(seconds=3600*24)).date())

if __name__=='__main__':
    filepattern = 'test_splitted/*'
    print "  === FILE CHANGING ==="
    change_files(filepattern)
    print "  === FILE HANDLING ==="
    handle_files(filepattern)
