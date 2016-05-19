import time
import cPickle as pickle
import re
import datetime as dt
import cStringIO as StringIO
from copy import deepcopy
from collections import namedtuple

import arrow
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from path_helpers import path
import matplotlib.mlab as mlab
from microdrop.experiment_log import ExperimentLog
from microdrop.protocol import Protocol


ExperimentLogDir = namedtuple('ExperimentLogDir', ['log_dir', 'instrument_id'])


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

def combine_data_from_microdrop_logs(exp_log_paths):
    combined_data_df = pd.DataFrame()

    for log_dir, instrument_id in exp_log_paths:
        exp_id = str(log_dir.name)

        """
        # TODO: check for existing csv file which provides a cache of the data generated
        # by this script in each directory.

        for file_path in log_dir.files('*.csv'):
            file_path = log_dir.files('*.csv')[0]
            df = pd.DataFrame().from_csv(path=file_path, index_col=None)
            if 'experiment_uuid' in df.columns and 'utc_timestamp' in df.columns:
                print '%s is a data file with the right columns' % file_path
                df = df.set_index(['experiment_uuid', 'utc_timestamp'])
                combined_data_df = combined_data_df.append(df)
        """

        # if don't have any dstat data for this experiment, continue
        if not len(log_dir.files('*Measure*.txt')):
            continue

        log_file = log_dir / 'data'

        try:
            # load the experiment log
            print 'load experiment log %s' % log_file
            log = ExperimentLog.load(log_file)
        except Exception, e:
            print "Couldn't load exp %s" % exp_id
            continue

        step_numbers = log.get('step')
        protocol = Protocol.load(log_dir / 'protocol')

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

        for file_path in log_dir.files('*Measure*.txt'):
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

            df['instrument_id'] = instrument_id
            df['step_number'] = step_number
            df['attempt_number'] = attempt
            df['temperature_celsius'] = temperature_celsius[index]
            df['relative_humidity'] = relative_humidity[index]

            start_time = log.get('start time')[0]
            df['experiment_start'] = dt.datetime.fromtimestamp(start_time).isoformat()
            df['experiment_length_min'] = log.get('time')[-1] / 60

            combined_data_df = combined_data_df.append(df)
    return combined_data_df

def create_summary(combined_data_df):
    # create a summary of the data
    summary_df = pd.DataFrame()

    for (experiment_uuid, step_label, attempt_number), group in combined_data_df.groupby(
            ['experiment_uuid', 'step_label', 'attempt_number']):
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
        'instrument_id',
        'relative_humidity',
        'temperature_celsius',
        'experiment_length_min',
    ]]

    summary_df.set_index(['experiment_start', 'sample_id', 'experiment_uuid', 'step_label'], inplace=True)
    summary_df.sort_index(inplace=True)
    summary_df.reset_index(inplace=True)
    return summary_df