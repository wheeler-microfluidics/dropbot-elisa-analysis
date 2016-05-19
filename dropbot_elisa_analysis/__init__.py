import time
import cPickle as pickle
import re
import datetime as dt
import cStringIO as StringIO
from copy import deepcopy

import arrow
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from path_helpers import path
import matplotlib.mlab as mlab
from microdrop.experiment_log import ExperimentLog
from microdrop.protocol import Protocol


# TODO: import this function from dstat_interface.analysis
def dstat_to_frame(data_path_i):
    '''
    Convert DStat text file results to `pandas.DataFrame`.

    Args
    ----

        data_path_i (str) : Path to DStat results text file.

    Returns
    -------

        (pandas.DataFrame) : DStat measurements in a table with
            the column `name` and `current_amps`, indexed by
            `utc_timestamp` and `time_s`.
    '''
    with data_path_i.open('r') as input_i:
        diff = (dt.datetime.utcnow() - dt.datetime.now())
        utc_timestamp = arrow.get(input_i.readline().split(' ')[-1]) + diff

    str_data = StringIO.StringIO('\n'.join(l for l in data_path_i.lines()
                                           if not l.startswith('#')))
    df_data = pd.read_csv(str_data, sep='\s+', header=None)
    df_data.rename(columns={0: 'time_s', 1: 'current_amps'}, inplace=True)
    df_data.insert(0, 'step_label', re.sub(r'-data$', '', data_path_i.namebase))
    df_data.insert(0, 'utc_timestamp', utc_timestamp.datetime +
                   df_data.index.map(lambda v: dt.timedelta(seconds=v)))
    df_data.set_index(['utc_timestamp', 'time_s'], inplace=True)
    return df_data

def scrape_microdrop_logs(log_path):
    log_sources = {
        'MR-BOX1': path('X:/MicrodropSettings/devices/GCC-3x3-5-16/logs'),
        'MR-BOX3': path('Y:/MicrodropSettings/devices/GCC3x3/logs'), # MR-BOX3
        #'?': path('Z:/MicrodropSettings/devices/GCC3x3/logs')
    }

    instrument_uuid = {
        'MR-BOX1': '26b8ec81-7cf8-4d38-a925-1f3167b8572b',
        'MR-BOX3': '8d63a715-b2c8-481f-9fc8-743f26b4e3b7',
        '?': 'ce041f1c-f010-4c5d-a88a-712fd38f57ea'
    }

    compiled_data_df = pd.DataFrame()

    index = 0
    for instrument_name, logs_path in log_sources.iteritems():
        for exp_path in logs_path.listdir():
            exp_id = str(exp_path.name)

            """
            for file_path in exp_path.files('*.csv'):
                file_path = exp_path.files('*.csv')[0]
                df = pd.DataFrame().from_csv(path=file_path, index_col=None)
                if 'experiment_uuid' in df.columns and 'utc_timestamp' in df.columns:
                    print '%s is a data file with the right columns' % file_path
                    df = df.set_index(['experiment_uuid', 'utc_timestamp'])
                    compiled_data_df = compiled_data_df.append(df)
            """

            # if don't have any dstat data for this experiment, continue
            if not len(exp_path.files('*Measure*.txt')):
                continue

            log_file = exp_path / 'data'

            try:
                # load the experiment log
                print 'load experiment log %s' % log_file
                log = ExperimentLog.load(log_file)
            except Exception, e:
                print "Couldn't load exp %s" % exp_id
                continue

            step_numbers = log.get('step')
            protocol = Protocol.load(exp_path / 'protocol')

            step_labels = []
            for step in protocol.steps:
                step_labels.append(pickle.loads(step.plugin_data['wheelerlab.step_label_plugin'])['label'])

            relative_humidity = []
            temperature_celsius = []

            for line in log.get('environment state', 'wheelerlab.dropbot_dx_accessories'):
                if line:
                    temperature_celsius.append(line['temperature_celsius'])
                    relative_humidity.append(line['relative_humidity'])
                else:
                    temperature_celsius.append(None)
                    relative_humidity.append(None)

            dstat_enabled = []
            magnet_engaged = []

            for step in protocol.steps:
                dx_data = pickle.loads(step.plugin_data['wheelerlab.dropbot_dx_accessories'])
                dstat_enabled.append(dx_data['dstat_enabled'])
                magnet_engaged.append(dx_data['magnet_engaged'])

            for file_path in exp_path.files('*Measure*.txt'):
                df = dstat_to_frame(file_path)
                df['experiment_uuid'] = log.uuid
                df['experiment_id'] = log.experiment_id
                df = df.reset_index().set_index(['utc_timestamp', 'experiment_uuid'])

                print file_path.name
                match = re.match('(?P<label>.*)(?P<attempt>\d+)\-data.txt', file_path.name)
                attempt = 0
                if match:
                    attempt = int(match.group('attempt'))
                else:
                    match = re.match(r'(?P<label>.*)-data.txt', file_path.name)

                label = match.group('label')
                step_number = step_labels.index(label)

                index = 0
                for i in range(attempt + 1):
                    index = step_numbers.index(step_number, index + 1)

                try:
                    metadata = deepcopy(log.metadata['wheelerlab.metadata_plugin'])
                    device_id = metadata.get('device_id', '')
                    sample_id = metadata.get('sample_id', '')

                    cre_device_id = re.compile(r'#(?P<batch_id>[a-fA-F0-9]+)'
                                               r'%(?P<device_id>[a-fA-F0-9]+)$')

                    # If `device_id` is in the form '#<batch-id>%<device-id>', extract batch and
                    # device identifiers separately.
                    match = cre_device_id.match(device_id)
                    if match:
                        metadata['device_id'] = unicode(match.group('device_id'))
                        metadata[u'batch_id'] = unicode(match.group('batch_id'))
                    else:
                        metadata['device_id'] = ''
                        metadata[u'batch_id'] = ''

                    df['device_id'] = metadata['device_id']
                    df['batch_id'] = metadata['batch_id']
                    df['sample_id'] = metadata['sample_id']
                except:
                    df['device_id'] = ''
                    df['batch_id'] = ''
                    df['sample_id'] = ''

                df['instrument_name'] = instrument_name
                df['instrument_uuid'] = instrument_uuid[instrument_name]
                df['step_number'] = step_number
                df['attempt_number'] = attempt
                df['temperature_celsius'] = temperature_celsius[index]
                df['relative_humidity'] = relative_humidity[index]

                start_time = log.get('start time')[0]
                df['experiment_start'] = dt.datetime.fromtimestamp(start_time).isoformat()
                df['experiment_length_min'] = log.get('time')[-1] / 60

                compiled_data_df = compiled_data_df.append(df)
    return compiled_data_df

def create_summary(compiled_data_df):
    # create a summary of the data
    summary_df = pd.DataFrame()

    for (experiment_uuid, step_label, attempt_number), group in compiled_data_df.groupby(['experiment_uuid', 'step_label', 'attempt_number']):
        group.reset_index()
        data = group[group['time_s'] > 2].mean().to_dict()

        data['experiment_uuid'] = experiment_uuid
        data['step_label'] = step_label
        data['attempt_number'] = attempt_number

        for k in group.columns:
            if k in data.keys():
                del data[k]

            if k not in data.keys():
                data[k] = group[k].values[0]

        summary_df = summary_df.append(pd.DataFrame(data=[data]), ignore_index=True)

    # specify the order of the columns
    summary_df = summary_df[[
        'experiment_start',
        'sample_id',
        'experiment_uuid',
        'step_label',
        'current_amps',
        'instrument_name',
        'relative_humidity',
        'temperature_celsius',
        'experiment_length_min',
    ]]

    summary_df.set_index(['experiment_start', 'sample_id', 'experiment_uuid', 'step_label'], inplace=True)
    summary_df.sort_index(inplace=True)
    summary_df.reset_index(inplace=True)
    return summary_df

if __name__ == '__main__':
    compiled_data_df = scrape_log_directories_to_frame()
    compiled_data_df.to_csv('Y:/compiled_data.csv')

    create_summary(compiled_data_df).to_csv('Y:/summary_data.csv', index=False)
