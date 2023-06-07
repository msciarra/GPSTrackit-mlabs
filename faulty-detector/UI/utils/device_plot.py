import pandas as pd
import matplotlib.pyplot as plt


def build_image(device_id, device_df): 
    """
    Filters the dataframe for the device with the device_id specified and plots the scatter using this information.
    :param device_id: id of the device to show.
    :param measures_df: dataframe with past daily readings of all devices.
    :return: the path to the scatter as an image.
    """
    #device_filter = measures_df["deviceId"] == int(device_id)
    #device_df = measures_df[device_filter].drop_duplicates(ignore_index=True)
    #device_df['unitTime'] = pd.to_datetime(device_df['unitTime'], utc=False)

    path_to_image = plot_scatter_and_return_path(device_df, device_id)
    return path_to_image


def plot_scatter_and_return_path(device_df, device_id):
    """
    Plots the scatter showing the history for the device and writes it to disk.
    :param device_df: dataframe containing all measures registered by the device in the past 24 hours.
    :param device_id: the id of the device whose scatter is to be plotted.
    :return: the path to the scatter image.
    """
    plt.figure(figsize=(15, 7), dpi=80, facecolor='white')
    device_df_on = device_df[device_df['ignitionStatus'] == 'on']
    device_df_off = device_df[device_df['ignitionStatus'] == 'off']

    on_scatter = plt.scatter(device_df_on['unitTime'], device_df_on['ecuEngineOdometer'], s=25, c='green')
    off_scatter = plt.scatter(device_df_off['unitTime'], device_df_off['ecuEngineOdometer'], s=25, c='red')
    lgd = plt.legend((on_scatter, off_scatter),
                     ('ignitionStatus: On', 'ignitionStatus: Off'),
                     scatterpoints=1,
                     ncol=1,
                     fontsize=10,
                     loc='center left', bbox_to_anchor=(1, 0.5),
                     fancybox=True, shadow=True)
    plt.grid(linestyle="--", color="grey")
    plt.ylabel('ecuEngineOdometer')
    plt.xlabel('unitTime')
    unit_id = device_df['unitId'].iloc[0]
    plt.title(f'deviceId: {device_id}, unitId: {unit_id}')
    path_to_image = f'images/device_{device_id}.png'
    plt.savefig(f'static/{path_to_image}', bbox_extra_artists=(lgd,), bbox_inches='tight')
    return path_to_image
