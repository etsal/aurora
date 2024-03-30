import os
import sys
import matplotlib
import pandas as pd
matplotlib.use('Agg')

import matplotlib.pyplot as plt

def graphme(file):
    df = pd.read_csv(file)
    x_values = df.iloc[:, 0]
    txn_blocks = df.iloc[:, 1]
    total_alloc = df.iloc[:, 2]
    moved = df.iloc[:, 10]
    fig, ax = plt.subplots()
    ax.plot(x_values, moved, label="Moved")
    ax.plot(x_values, txn_blocks, label="TXN Writes")
    ax.plot(x_values, total_alloc, label="Total Allocations")
    ax.legend()
    name = file.split(".")[0]
    fig.savefig("{}.png".format(name))


if __name__ == "__main__":
    graphme(sys.argv[1])