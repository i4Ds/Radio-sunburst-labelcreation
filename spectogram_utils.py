import pandas as pd
import cv2
import seaborn as sns
import matplotlib.pyplot as plt

def int_time_delta_to_pd_timedelta(int_time_delta):
    return pd.Timedelta(seconds=int_time_delta)

def spec_time_to_pd_datetime(spec):
    start_time = pd.to_datetime(spec.start)
    time_axis = spec.time_axis
    time_frequency = spec.time_axis[1] - spec.time_axis[0]
    datetime_range = pd.date_range(start_time, periods=len(time_axis), freq=int_time_delta_to_pd_timedelta(time_frequency))
    return datetime_range
    
def spec_to_pd_dataframe(spec):
    return pd.DataFrame(spec.data.T, index=spec_time_to_pd_datetime(spec), columns=[str(x) for x in spec.freq_axis])

def change_resolution_over_time(spectogram, time_resolution, interpolation=cv2.INTER_LINEAR):
    """
    Change the resolution of the spectogram over time (x-axis)
    :param spectogram: The spectogram to change the resolution of.
    :param resolution: The new resolution.
    :return: The spectogram with the new resolution.
    """
    index = spectogram.index
    columns = spectogram.columns
    
    spectogram_resized = cv2.resize(spectogram.values, (spectogram.shape[1], time_resolution), interpolation=interpolation)
    
    date_range = pd.date_range(start=index[0], end=index[-1], periods=time_resolution)
    
    return pd.DataFrame(spectogram_resized, index=date_range, columns=columns)

def plot_spectogram(spectogram):
    spectogram = spectogram.copy()
    spectogram.index = spectogram.index.strftime('%Y-%m-%d %H:%M:%S')
    ax = sns.heatmap(spectogram.T, cmap='viridis', cbar_kws={'label': 'Flux (W/m^2)'})
    plt.title('Spectogram')
    plt.ylabel('Frequency (MHz)')
    plt.xlabel('Time UTC')
    plt.show()
