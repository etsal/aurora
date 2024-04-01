import os
import sys
import matplotlib
import pandas as pd
from pprint import pprint
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def toGig(a):
    return (a * 4) / (1024 * 1024)

def graphme(files):

    fig, ax = plt.subplots()
    i = 0
    myfile = None
    rate_of_change = {}
    for file in files:
        df = pd.read_csv(file)
        x_values = df.iloc[:, 0]
        txn_blocks = df.iloc[:, 1]
        total_alloc = df.iloc[:, 2]
        moved = df.iloc[:, 10]
        if (len(x_values.to_list()) > i):
            i = len(x_values.to_list())
            myfile = txn_blocks
        ax.plot(x_values, toGig(moved), label="Moved-{}".format(file))

        a = txn_blocks.to_list()
        b = moved.to_list()
        a = a[-1] - a[-2]
        b = b[-1] - b[-2]
        rate_of_change[file] = [b, a, b / a] 

    ax.plot(x_values, toGig(myfile), label="Written-{}".format(file))

    pprint(sorted(rate_of_change.items(), key = lambda a: a[1]))

    ax.set_ylabel("GiB")
    ax.set_xlabel("Transactions")
    ax.set_title(file.split(".")[0])
    ax.legend()
    ax.grid(True)
    name = file.split(".")[0]
    fig.set_size_inches(12, 12)
    fig.savefig("graph.png")


if __name__ == "__main__":
    graphme(sys.argv[1:])