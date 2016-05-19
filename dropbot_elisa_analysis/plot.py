import dstat_interface as di
import dstat_interface.analysis
import dstat_interface.plot
import matplotlib.pyplot as plt


def plot_microdrop_dstat_data(df_md_dstat, settling_period_s=2., axes=None):
    '''
    Args
    ----

        df_md_dstat (pandas.DataFrame) : Microdrop DStat measurements in a
            table with at least the columns `experiment_uuid`, `step_number`,
            `attempt_number`, `target_hz`, `sample_frequency_hz`, `current_amps`,
            and `time_s`.
        settling_period_s (float) : Measurement settling period in seconds.
            Measurements taken before start time will not be included in
            calculations.
        axes (list) : List of at least two `matplotlib` axes to plot to.  The
            first axis is used to plot the `current_amp` values.  The second
            axis is used to plot the FFT for experiments using synchronous
            detection.

    Returns
    -------

        (list) : List of two `matplotlib` axes use for current amps and FFT
            plots, respectively.
    '''
    if axes is None:
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))

    for index_i, df_i in df_md_dstat.groupby(['experiment_uuid', 'step_number',
                                              'attempt_number']):
        experiment_uuid_i, step_number_i, attempt_number_i = index_i
        step_label_i = df_i.iloc[0]['step_label']
        step_label_i = (step_label_i if step_label_i
                        else 'step{:03d}'.format(step_number_i))
        label_i = '[{}]-{}-{:02d}'.format(experiment_uuid_i[:8], step_label_i,
                                          attempt_number_i)
        di.plot.plot_dstat_data(df_i, settling_period_s=settling_period_s,
                                axes=axes, label=label_i)
    return axes
