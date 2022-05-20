import numpy as np
import matplotlib.pyplot as plt
import util

FILENAME = "normal"
num_proposals = 20 * 1000000

def to_tp(t):
    return num_proposals/(t/1000)

def ci_to_err_tp(ci):
    (lo, hi) = ci
    (lo_tp, hi_tp) = (to_tp(hi), to_tp(lo))
    err = (hi_tp-lo_tp)/2
    return err


SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

omni3 = [183824.47807730004, 144694.35604539997, 157595.52446299998]
omni5 = [190338.7681753, 145235.4943589, 156933.76605699997]
raft3 = [186248.80721379997, 141961.0995777, 162419.52114549998]
raft5 = [194805.1137586, 147080.8413563, 157769.6113011]

omni3_ci = [(181955.66382045147, 185693.2923341485), (140071.5642970106, 149317.14779378934), (153072.84688704673 ,162118.20203895328)]
omni5_ci =[(186288.86263424775, 194388.67371635226), (140616.68581090137 ,149854.30290689864), (152488.62670508813 ,161378.9054089118)]
raft3_ci = [(181564.88763730158 ,190932.72679029836), (136112.5417621102, 147809.6573932898), (157821.41249263647, 167017.6297983635)]
raft5_ci = [(191575.89927131875 ,198034.32824588125), (144767.4926228062 ,149394.1900897938), (153457.0718096598, 162082.1507925402)]
x_axis = [1, 2, 3]
num_cp = ["500", "5k", "50k"]

all_series = [
    ("Raft, n=3", list(map(to_tp, raft3)), list(map(ci_to_err_tp, raft3_ci))),
    ("Raft, n=5", list(map(to_tp, raft5)), list(map(ci_to_err_tp, raft5_ci))),
    ("Omni-Paxos, n=3", list(map(to_tp, omni3)), list(map(ci_to_err_tp, omni3_ci))),
    ("Omni-Paxos, n=5", list(map(to_tp, omni5)), list(map(ci_to_err_tp, omni5_ci)))
]


fig, ax = plt.subplots()
for (label, data, err) in all_series:
    color = util.colors[label]
    split = label.split(",")
    linestyle = util.linestyles[split[0]]
    marker = util.markers[split[1]]
    plt.errorbar(x_axis, data, label=label, color=color, marker=marker, linestyle=linestyle, yerr=err, capsize=8)

ax.yaxis.set_major_formatter(util.format_k)
ax.legend(loc = "lower right", fontsize=20, ncol=2)

fig.set_size_inches(15, 3.5)
ax.set_ylabel('Throughput (ops/s)')
ax.set_xlabel('Number of concurrent proposals')
plt.xticks(x_axis, num_cp)
plt.yticks([90000, 110000, 130000, 150000])
#ax.yaxis.set_major_formatter(util.format_k)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')