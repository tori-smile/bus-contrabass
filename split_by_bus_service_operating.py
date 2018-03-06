import pandas as pd
from glob import glob
from time import time
import progressbar


def handle_files(filepattern):
    start_time = time()
    files = glob(filepattern)
    bar = progressbar.ProgressBar()
    for filename in bar(files):
        bus_transactions = pd.read_csv(filename)
        bus_transactions['datetime'] = bus_transactions['time'].apply(to_datetime)
        value = get_filter_value(bus_transactions.loc[0]['datetime'])
        append_df = bus_transactions[bus_transactions.time < value]
        bus_transactions = bus_transactions[bus_transactions.time >= value]
        bus_transactions.to_csv(filename, index=None)
        date = get_previous_date(bus_transactions.iloc[0]['datetime'])
        append_filename = filepattern[:-9] + date
        # print "append_filename: %s" % append_filename

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
    filepattern = 'test_separation/*'
    handle_files(filepattern)
