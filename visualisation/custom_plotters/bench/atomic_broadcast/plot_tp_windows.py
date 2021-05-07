import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import sys
import argparse
import os
import numpy as np
import datetime
import scipy.stats as st
from pathlib import Path
from matplotlib.ticker import (MultipleLocator,
                               FormatStrFormatter,
                               AutoMinorLocator)

colors = {
  "Omni-Paxos": "dodgerblue",
  "Omni-Paxos replace follower": "dodgerblue",
  "Omni-Paxos replace leader": "midnightblue",
  "Raft": "orange",
  "Raft replace follower": "orange",
  "Raft replace leader": "crimson",
}

def get_label_and_color(filename):
	csv = filename.split(",")
	algorithm = csv[0]
	if algorithm == "paxos":
		algorithm = "Omni-Paxos"
	else:
		algorithm = "Raft"
	reconfig = csv[len(csv)-1].split(".")[0]
	if reconfig == "none":
		label = algorithm
	else:
		label = "{} {}".format(algorithm, reconfig.replace("-", " "))
	color = colors[label]
	return (label, color)

def format_time(seconds, _):
    """Formats a timedelta duration to [N days] %M:%S format"""
    secs_in_a_min = 60

    minutes, seconds = divmod(seconds, secs_in_a_min)

    time_fmt = "{:d}:{:02d}".format(minutes, seconds)
    return time_fmt

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-w', required=True, type=int, help='Window duration (s)')
parser.add_argument('--no-ci', dest='ci', action='store_false')
parser.set_defaults(feature=True)

args = parser.parse_args()
print("Plotting with args:",args)

fig, ax = plt.subplots()
max_ts = 0
data_files = [f for f in os.listdir(args.s) if f.endswith('.data')]
for filename in data_files :
	f = open(args.s + "/" + filename, 'r')
	print("Reading", filename, "...")
	all_tp = []
	for line in f:
		num_decided_per_window = line.split(",")
		for window_idx, num_decided in enumerate(num_decided_per_window):
			if num_decided.isdigit():
				tp = int(num_decided) / args.w
				if len(all_tp) <= window_idx:
					all_tp.append([])
				all_tp[window_idx].append(tp)

	all_ts = []
	all_avg_tp = []
	all_ci95_lo = []
	all_ci95_hi = []

	all_min_tp = []
	all_max_tp = []

	all_tp_filtered = list(filter(lambda x: len(x) == 10, all_tp)) 
	for (window_idx, all_tp_per_window) in enumerate(all_tp_filtered):
		ts = (window_idx+1) * args.w
		if ts > max_ts:
			max_ts = ts
		all_ts.append(ts)
		#all_ts.append(format_time(seconds=(i+1) * args.w))
		avg_tp = sum(all_tp_per_window)/len(all_tp_per_window)
		all_avg_tp.append(avg_tp)

		min_tp = min(all_tp_per_window)
		max_tp = max(all_tp_per_window)
		all_min_tp.append(min_tp)
		all_max_tp.append(max_tp)

		if args.ci:
			if sum(all_tp_per_window) > 0:
				(ci95_lo, ci95_hi) = st.t.interval(alpha=0.95, df=len(all_tp_per_window)-1, loc=np.mean(np.array(all_tp_per_window)), scale=st.sem(np.array(all_tp_per_window))) 
				if ci95_lo < 0:
					ci95_lo = 0
				#print((ci95_lo, ci95_hi))
				all_ci95_lo.append(ci95_lo)
				all_ci95_hi.append(ci95_hi)
			else:
				all_ci95_lo.append(all_tp_per_window[0])
				all_ci95_hi.append(all_tp_per_window[0])

	(label, color) = get_label_and_color(filename)
	ax.plot(all_ts, np.array(all_avg_tp), marker=".", color=color, label=label)
	#ax.plot(all_ts, np.array(all_ci95_lo))
	#ax.plot(all_ts, np.array(all_ci95_hi))
	if args.ci:
		ax.fill_between(all_ts, all_ci95_lo, all_ci95_hi, color=color, alpha=0.2)
	#ax.plot(all_ts, all_min_tp, marker='o')
	#ax.plot(all_ts, all_max_tp, marker='o')

MEDIUM_SIZE = 18
ax.legend(loc = "lower right", fontsize=15)
x_axis = np.arange(0, max_ts+4*args.w, 4*args.w)

for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
             ax.get_xticklabels() + ax.get_yticklabels()):
    item.set_fontsize(MEDIUM_SIZE)

plt.ylabel("Throughput (ops/s)")
plt.xlabel("Time")
plt.xticks(x_axis)
ax.xaxis.set_major_formatter(format_time)

plt.ylim(bottom=0)
plt.gcf().autofmt_xdate()

fig.set_size_inches(12, 6)

split = args.s.split("/")
exp_str = split[len(split)-3]
exp_str_split = exp_str.split("-")
num_nodes = exp_str_split[0]
num_cp = exp_str_split[1]
reconfig = exp_str_split[len(exp_str_split) - 1]
title = "{} nodes, {} concurrent proposals".format(num_nodes, num_cp)
if reconfig != "off":
	title += ", {} reconfiguration".format(reconfig)
plt.title(title, fontsize=MEDIUM_SIZE)

if args.t is not None:
    target_dir = args.t + "/windowed/{}-{}/".format(num_nodes, num_cp)
else:
    target_dir = "./"
if args.ci == False:
	exp_str = exp_str + "-no-ci"
Path(target_dir).mkdir(parents=True, exist_ok=True)
plt.savefig(target_dir + "{}.pdf".format(exp_str), dpi = 600)
