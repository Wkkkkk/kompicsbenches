from numpy import double
import pandas as pd
import matplotlib.pyplot as plt
from torch import xlogy_

para_list = ['algorithm','number_of_nodes', 'number_of_proposals', 
    'concurrent_proposals', 'reconfiguration', 'reconfig_policy', 
    'network_scenario', 'probability', 'data_size', 'compression_rate',
    'preprocessing_time', '']

SMALL_SIZE = 16
MEDIUM_SIZE = 18
BIG_SIZE = 22

def line_style(rate):
    return {
        0.2: 'v',
        0.5: 's',
        0.8: 'o'
    }.get(rate, '*') 

def marker_style(preprocessing):
    return {
        0: 'y-',
        2: 'r-.',
        5: 'g--',
        10: 'b:',
        50: 'g--',
        100: 'b:'
    }.get(preprocessing, 'y-') 

def read_to_df(file):
    df = pd.read_csv(file, sep=',')

    df[para_list] = df['PARAMS'].str.split('!N!',expand=True)
    for para in para_list:
        df[para] = df[para].str.split(':').str[1]

    df['label'] = df['probability'] + df['data_size'] + df['compression_rate'] + df['preprocessing_time']
    df['number_of_proposals'] = df['number_of_proposals'].astype('int')
    df['concurrent_proposals'] = df['concurrent_proposals'].astype('int')
    df['data_size'] = df['data_size'].astype('int')
    df['compression_rate'] = df['compression_rate'].astype('float')
    df['probability'] = df['probability'].astype('float')
    df['preprocessing_time'] = df['preprocessing_time'].astype('int')
    df['throughput'] = df['number_of_proposals'] / df['MEAN']

    return df

def format_y(y, _):
    return "{}k".format(int(y))

def format_y2(y, _):
    return "{:.1f}k".format(y)

def format_x(x, _):
	if x >= 1000 or x <= -1000:
		return "{}k".format(int(x/1000))
	else:
		return int(x)

if __name__ == "__main__":
    input = 'ATOMICBROADCAST.data'
    # input = 'ATOMICBROADCAST.data.local'
    df = read_to_df(input)

    number_of_subplots = df['data_size'].nunique() * df['probability'].nunique()
    number_of_columns  = 3
    number_of_rows     = number_of_subplots // number_of_columns
    # If one additional row is necessary -> add one:
    if number_of_subplots % number_of_columns != 0:
        number_of_rows += 1

    # create subtitles for each row
    fig, big_axes = plt.subplots(nrows=3, ncols=1, sharey=True) 
    for row, big_ax in enumerate(big_axes, start=1):
        size = {1: 10,
                2: 100,
                3: 500}
        big_ax.set_title("Data Size: %s bytes\n" % size[row], fontsize=24)

        # Turn off axis lines and ticks of the big subplot 
        # obs alpha is 0 in RGBA string!
        big_ax.tick_params(labelcolor=(1.,1.,1., 0.0), top='off', bottom='off', left='off', right='off')
        # removes the white frame
        big_ax._frameon = False
        big_ax.set_xticks([])
        big_ax.set_yticks([])

    # create subplots
    position = range(1,number_of_subplots + 1)
    fig.set_size_inches(18, 18)
    index = 0
    for k1, grp1 in df.groupby(['data_size', 'probability']):
        ax = fig.add_subplot(number_of_rows, number_of_columns, position[index])
        index += 1
        
        (ds, p) = k1
        size = "{} bytes".format(int(ds))
        proba = "{}%".format(int(p*100))
        ax.set_title("Cache Hit Rate:%s" % (proba))
        for k2, grp2 in grp1.groupby(['compression_rate', 'preprocessing_time']):
            (rate, pre) = k2
            style = line_style(rate) + marker_style(pre)
            label = "No caching"
            if rate > 0.0 or pre > 0:
                rate  = "{:2}%".format(int(rate*100))
                pre   = "{:2}μs".format(int(pre))
                label = "Compression: {}, Preprocess:{}".format(rate, pre)
                
            ax.plot(grp2['concurrent_proposals'], grp2['throughput'], style, label=label)
            ax.set_xticks(grp2['concurrent_proposals'])
            
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                    ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(MEDIUM_SIZE)

        ax.xaxis.set_major_formatter(format_x)
        ax.yaxis.set_major_formatter(format_y)

        # add some custom operations here
        print("ds", ds, " p", p)
        # if ds == 1:
            # ax.set_ylim([3, 60])
        
        # if ds == 16 and p != 1.0:
            # ax.set_ylim([1.5, 7])

        # if ds == 500:
            # ax.set_ylim([45, 160])

        # if ds == 500 and p == 0:
            # ax.yaxis.set_major_formatter(format_y2)

        # plt.ylim(45, 170)
    
    # Add legends
    handles, labels = ax.get_legend_handles_labels()
    handles.reverse() # looks better in the reversed order
    labels.reverse()

    legend_hanldes = [[], [], []]
    legend_labels  = [[], [], []]
    for (handle, label) in zip(handles, labels):
        if label == "No caching" or "2μs" in label:
            legend_hanldes[0].append(handle)
            legend_labels[0].append(label)
        if "5μs" in label:
            legend_hanldes[1].append(handle)
            legend_labels[1].append(label)
        if "10μs" in label:
            legend_hanldes[2].append(handle)
            legend_labels[2].append(label)

    fig.legend(legend_hanldes[0], legend_labels[0], prop={'size': SMALL_SIZE}, bbox_to_anchor=(0.3, 1.02))
    fig.legend(legend_hanldes[1], legend_labels[1], prop={'size': SMALL_SIZE}, bbox_to_anchor=(0.6, 1.02))
    fig.legend(legend_hanldes[2], legend_labels[2], prop={'size': SMALL_SIZE}, bbox_to_anchor=(0.9, 1.02))
    
    fig.text(0.5, 0.06, 'Number of Concurrent Proposals', ha='center', va='center', size=BIG_SIZE)
    fig.text(0.05, 0.5, 'Throughput (Operations/s)', ha='center', va='center', rotation='vertical', size=BIG_SIZE)
    plt.subplots_adjust(left=0.1,
                        bottom=0.1,
                        right=0.9,
                        top=0.9,
                        wspace=0.3,
                        hspace=0.4)
    plt.savefig("{}.pdf".format(input), dpi = 600, bbox_inches='tight')

