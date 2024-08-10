#!/usr/bin/env python3
import pandas as pd
import sys
import matplotlib.pyplot as plt

from matplotlib.ticker import MultipleLocator

df = pd.read_csv(sys.argv[1])

fses = ["objsnap", "ffs", "zfs"]
plots = ["iops", "lat_ns", "lat_99_ns"]
threads = [ t for t in range(1, df[df["fs"] == "objsnap"].shape[0] + 1) ]
for p in plots:
    fig, ax = plt.subplots()
    for fs in fses:
        ax.plot(threads, df[df["fs"] == fs][p].tolist(), label=fs)
    ax.legend()
    fig.savefig(p.strip() + ".svg")

fig, ax = plt.subplots()
for fs in fses:
    good = df[df["fs"] == fs]["goodput_mib"]
    throughput = df[df["fs"] == fs]["throughput_mib"]
    total = throughput / good
    ax.plot(threads, total.tolist(), label=fs)
    ax.set_ylim(0, 7)
    ax.yaxis.set_major_locator(MultipleLocator(1))
    ax.set_xlabel("Threads")
    ax.set_ylabel("Goodput MiB / Throughput MiB")
ax.legend()
fig.savefig("throughput_over_goodput" + ".svg")
