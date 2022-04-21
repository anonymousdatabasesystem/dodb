import random
import string
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import re

def mean_list(two_dimension_list):
    return [sum(two_dimension_list[i])/len(two_dimension_list[i]) if(len(two_dimension_list[i])>0) else 0 for i in range(len(two_dimension_list))]

def error_bar_list(two_dimension_list):
    res = []
    for i in range(len(two_dimension_list)):
        if(len(two_dimension_list[i])>0):
            mean = sum(two_dimension_list[i]) / len(two_dimension_list[i])
            variance = sum([((x - mean) ** 2) for x in two_dimension_list[i]]) / len(two_dimension_list[i])
            res.append(variance ** 0.5)
        else:
            res.append(0)
    return res


# 1. autogenerate dataset

# 2. Run our system, ObliDB Opaque Spark SQL at different dataset size for 10 times.
# write shell script for each system to auto-run and save logs for later parsing.
hundun_singlethread_log_filename = "hundun_singlethread_dell_log.txt"
hundun_parallel_log_filename = "hundun_parallel_dell_log.txt"
opaque_log_filename = "opaque_dell.txt"
oblidb_log_filename = "oblidb_dell.txt"

# 10k, 100k, 1m, 10m
numInputSize = 3
hundun_singlethread_bdb1 = [[] for i in range(numInputSize)]
hundun_singlethread_bdb2 = [[] for i in range(numInputSize)]
hundun_singlethread_bdb3 = [[] for i in range(numInputSize)]
hundun_singlethread_bdb3_using_oblidb_sort =[[23.68348, 24.41744, 24.14894, 23.94675,23.98340 ], [339.45420, 340.48958, 344.93756, 343.49786, 341.31], [6255.08,6252.15927,6210.27416 ,6341.65234,6272.14241]]
hundun_parallel_bdb1 = [[] for i in range(numInputSize)]
hundun_parallel_bdb2 = [[] for i in range(numInputSize)]
hundun_parallel_bdb3 = [[] for i in range(numInputSize)]
opaque_bdb1 = [[] for i in range(numInputSize)]
opaque_bdb2 = [[] for i in range(numInputSize)]
opaque_bdb3 = [[] for i in range(numInputSize)]
sparksql_bdb1 = [[] for i in range(numInputSize)]
sparksql_bdb2 = [[] for i in range(numInputSize)]
sparksql_bdb3 = [[] for i in range(numInputSize)]
opaque_oblivious_bdb1 = [[] for i in range(numInputSize)]
opaque_oblivious_bdb2 = [[] for i in range(numInputSize)]
opaque_oblivious_bdb3 = [[] for i in range(numInputSize)]
oblidb_bdb1 = [[] for i in range(numInputSize)]
oblidb_bdb2 = [[] for i in range(numInputSize)]
oblidb_bdb3 = [[] for i in range(numInputSize)]


sortjoin_single_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]
sortjoin_parallel_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]
hash_groupby_singlethread_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]
hash_groupby_parallel_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]
filter_single_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]
filter_parallel_breakdown = [[0 for i in range(numInputSize)] for i in range(5)]

find_float = lambda x: re.search("\d+(\.\d+)?",x)  .group()

def parse_log_line(line):
    # format BDB1 - size of table - 123 - seconds
    data = line.split("-")
    input_size = int(data[1].strip())
    run_time = float(data[3].strip())
    #print(input_size, run_time)
    return input_size, run_time

def parse_breakdown(line):
    data = line.split('-')
    input_size = int(data[1].strip())
    encryption = int(data[2].strip().split(" ")[-1].strip())
    decryption = int(data[3].strip().split(" ")[-1].strip())
    inner_memcpy = int(data[4].strip().split(" ")[-1].strip())
    untrusted_to_enclave_memcpy_time = int(data[5].strip().split(" ")[-1].strip())
    enclave_to_untrusted_memcpy_time = int(data[6].strip().split(" ")[-1].strip())
    process_time = int(data[7].strip().split(" ")[-1].strip())
    total = encryption + decryption + inner_memcpy + process_time + untrusted_to_enclave_memcpy_time + enclave_to_untrusted_memcpy_time
    #print(input_size, encryption, decryption, memcpy, process_time)
    return input_size, float(encryption / total), float(decryption / total), float(untrusted_to_enclave_memcpy_time / total), float(enclave_to_untrusted_memcpy_time / total), float((process_time + inner_memcpy)/ total)

def parse_opaque_log(line):
    data = line.split('"')
    size = int(data[3].strip())
    time = float(find_float(data[-1].strip())) / 1000
    return size, time

def normalize_time_breakdown(input):
    for i in range(numInputSize):
        total = 0
        for j in range(5):
            total += input[j][i]

        for j in range(5):
            if(total == 0):
                input[j][i] = 0
            else:
                input[j][i] = input[j][i] / float(total)
    return input


def round_helper(input):
    if(input > 100):
        return (int)(input)
    elif(input > 10):
        return round(input, 1)
    else:
        return round(input, 2)

# 3. parse the log to collect benchmark result
with open(hundun_parallel_log_filename, "r") as file:
    line = file.readline()
    while(line):
        if("DOBDB1" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size >=100000):
                hundun_parallel_bdb1[int(math.log10(input_size)) - 5].append(run_time)
        elif("DOBDB2" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size >=100000):
                hundun_parallel_bdb2[int(math.log10(input_size)) - 5].append(run_time)
        elif("DOBDB3" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size >=100000):
                hundun_parallel_bdb3[int(math.log10(input_size)) - 5].append(run_time)
        elif("DOSortJoin" in line and "Microbench" not in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            sortjoin_parallel_breakdown[0][int(math.log10(input_size)) - 5] += encryption
            sortjoin_parallel_breakdown[1][int(math.log10(input_size)) - 5] += decryption
            sortjoin_parallel_breakdown[2][int(math.log10(input_size)) - 5] += untrusted_to_enclave
            sortjoin_parallel_breakdown[3][int(math.log10(input_size)) - 5] += enclave_to_untrusted
            sortjoin_parallel_breakdown[4][int(math.log10(input_size)) - 5] += process_time
        elif("DOFilter" in line and "Microbench" not in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            filter_parallel_breakdown[0][int(math.log10(input_size)) - 5] += encryption
            filter_parallel_breakdown[1][int(math.log10(input_size)) - 5] += decryption
            filter_parallel_breakdown[2][int(math.log10(input_size)) - 5] += untrusted_to_enclave
            filter_parallel_breakdown[3][int(math.log10(input_size)) - 5] += enclave_to_untrusted
            filter_parallel_breakdown[4][int(math.log10(input_size)) - 5] += process_time
        elif("DOHashBasedGroupby" in line and "Microbench" not in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            hash_groupby_parallel_breakdown[0][int(math.log10(input_size/3)) - 5] += encryption
            hash_groupby_parallel_breakdown[1][int(math.log10(input_size/3)) - 5] += decryption
            hash_groupby_parallel_breakdown[2][int(math.log10(input_size/3)) - 5] += untrusted_to_enclave
            hash_groupby_parallel_breakdown[3][int(math.log10(input_size/3)) - 5] += enclave_to_untrusted
            hash_groupby_parallel_breakdown[4][int(math.log10(input_size/3)) - 5] += process_time
        line = file.readline()

print("hundun parallel done")

with open(hundun_singlethread_log_filename, "r") as file:
    line = file.readline()
    while(line):
        if("DOBDB1" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size >=100000):
                hundun_singlethread_bdb1[int(math.log10(input_size)) - 5].append(run_time)
        elif("DOBDB2" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size >=100000):
                hundun_singlethread_bdb2[int(math.log10(input_size)) - 5].append(run_time)
        elif("DOBDB3" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size == 100000):
                hundun_singlethread_bdb3[0].append(run_time)
            elif(input_size ==300000):
                hundun_singlethread_bdb3[1].append(run_time)
            elif(input_size ==1000000):
                hundun_singlethread_bdb3[2].append(run_time)
        elif("DOSortJoin" in line and "Microbench" not in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            sortjoin_single_breakdown[0][int(math.log10(input_size)) - 5] += encryption
            sortjoin_single_breakdown[1][int(math.log10(input_size)) - 5] += decryption
            sortjoin_single_breakdown[2][int(math.log10(input_size)) - 5] += untrusted_to_enclave
            sortjoin_single_breakdown[3][int(math.log10(input_size)) - 5] += enclave_to_untrusted
            sortjoin_single_breakdown[4][int(math.log10(input_size)) - 5] += process_time
        elif("DOFilter" in line and "Microbench" not in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            filter_single_breakdown[0][int(math.log10(input_size)) - 5] += encryption
            filter_single_breakdown[1][int(math.log10(input_size)) - 5] += decryption
            filter_single_breakdown[2][int(math.log10(input_size)) - 5] += untrusted_to_enclave
            filter_single_breakdown[3][int(math.log10(input_size)) - 5] += enclave_to_untrusted
            filter_single_breakdown[4][int(math.log10(input_size)) - 5] += process_time
        elif("DOHashBasedGroupby" in line):
            input_size, encryption, decryption, untrusted_to_enclave, enclave_to_untrusted, process_time = parse_breakdown(line)
            hash_groupby_singlethread_breakdown[0][int(math.log10(input_size/3)) - 5] += encryption
            hash_groupby_singlethread_breakdown[1][int(math.log10(input_size/3)) - 5] += decryption
            hash_groupby_singlethread_breakdown[2][int(math.log10(input_size/3)) - 5] += untrusted_to_enclave
            hash_groupby_singlethread_breakdown[3][int(math.log10(input_size/3)) - 5] += enclave_to_untrusted
            hash_groupby_singlethread_breakdown[4][int(math.log10(input_size/3)) - 5] += process_time
        line = file.readline()

print("hundun single done")



with open(oblidb_log_filename, "r") as file:
    line = file.readline()
    while(line):
        if("BDB1" in line):
            input_size, run_time = parse_log_line(line)
            oblidb_bdb1[int(math.log10(input_size)) - 5].append(run_time)
        elif("BDB2" in line):
            input_size, run_time = parse_log_line(line)
            oblidb_bdb2[int(math.log10(input_size)) - 5].append(run_time)
        elif("BDB3" in line):
            input_size, run_time = parse_log_line(line)
            if(input_size == 100000):
                oblidb_bdb3[0].append(run_time)
            elif(input_size ==300000):
                oblidb_bdb3[1].append(run_time)
            elif(input_size ==1000000):
                oblidb_bdb3[2].append(run_time)
            # oblidb_bdb3[int(math.log10(input_size)) - 5].append(run_time)
        line = file.readline()

print("oblidb done")

with open(opaque_log_filename, 'r') as opaquefile:
    line = opaquefile.readline()
    while(line):
        if("distributed" in line):
            input_size, run_time = parse_opaque_log(line)
            if("spark sql" in line):
                if("big data 1" in line):
                    sparksql_bdb1[int(math.log10(input_size) - 5)].append(run_time)
                elif("big data 2" in line):
                    sparksql_bdb2[int(math.log10(input_size) - 5)].append(run_time)
                elif("big data 3 foreign key join only" in line):
                    if(input_size == 100000):
                        sparksql_bdb3[0].append(run_time)
                    elif(input_size ==300000):
                        sparksql_bdb3[1].append(run_time)
                    elif(input_size ==1000000):
                        sparksql_bdb3[2].append(run_time)
                    # sparksql_bdb3[int(math.log10(input_size) - 5)].append(run_time)
            elif("encrypted" in line):
                if ("big data 1" in line):
                    opaque_bdb1[int(math.log10(input_size) - 5)].append(run_time)
                elif ("big data 2" in line):
                    opaque_bdb2[int(math.log10(input_size) - 5)].append(run_time)
                elif ("big data 3" in line):
                    opaque_bdb3[int(math.log10(input_size) - 5)].append(run_time)
            elif("opaque" in line):
                if ("big data 1" in line):
                    opaque_oblivious_bdb1[int(math.log10(input_size) - 5)].append(run_time)
                elif ("big data 2" in line):
                    opaque_oblivious_bdb2[int(math.log10(input_size) - 5)].append(run_time)
                elif ("big data 3" in line):
                    opaque_oblivious_bdb3[int(math.log10(input_size) - 5)].append(run_time)
        line = opaquefile.readline()




print("opaque done")
# normalize the time breakdown for each op
filter_parallel_breakdown = normalize_time_breakdown(filter_parallel_breakdown)
filter_single_breakdown = normalize_time_breakdown(filter_single_breakdown)
sortjoin_parallel_breakdown = normalize_time_breakdown(sortjoin_parallel_breakdown)
sortjoin_single_breakdown = normalize_time_breakdown(sortjoin_single_breakdown)
hash_groupby_parallel_breakdown = normalize_time_breakdown(hash_groupby_parallel_breakdown)
hash_groupby_singlethread_breakdown = normalize_time_breakdown(hash_groupby_singlethread_breakdown)


print(hundun_singlethread_bdb3)

title_size = 30
legend_size = 17
breakdown_legend_size = 30
ticks_size = 35
bdbperf_ticks_size=22
padding_ticks_size=30


def draw_bdb_series_perf_comparison():
    width = 0.5
    bdb1_sizes = [ "100K", "1M", "10M"]
    # x = np.linspace(0, 5, 6)
    x = np.linspace(0, 2, 3)

    xais = ["Spark SQL", r"$\bf{Ours}$", "ObliDB"]
    # yais = ["Spark SQL", r"$\bf{ADORE(P)}$", r"$\bf{ADORE(S)}$",  "ObliDB", "Opaque Enc.", "Opaque Obl."]
    null_axis = ["", "",""]
    for j in range(len(bdb1_sizes)):
        plt.figure(figsize=(10,3), dpi=300, facecolor='w', edgecolor='k')
        #plt.title("BDB1 over {} rows".format(bdb1_sizes[j]), fontsize=25)
        y_val = [mean_list(sparksql_bdb1)[j],mean_list(hundun_singlethread_bdb1)[j], mean_list(oblidb_bdb1)[j] ]
        y_err = [error_bar_list(sparksql_bdb1)[j],error_bar_list(hundun_singlethread_bdb1)[j],error_bar_list(oblidb_bdb1)[j] ]
        df = pd.DataFrame({"Running time(sec)" : y_val}, index = xais )
        plt.barh(x[0], mean_list(sparksql_bdb1)[j], xerr=error_bar_list(sparksql_bdb1)[j],  color=[(77/256.0, 153/256.0, 0/256.0)])
        # plt.barh(x[1], mean_list(hundun_parallel_bdb1)[j], xerr=error_bar_list(hundun_parallel_bdb1)[j], color=[(255/256.0,0/256.0,0/256.0)])
        plt.barh(x[1], mean_list(hundun_singlethread_bdb1)[j],  xerr=error_bar_list(hundun_singlethread_bdb1)[j], color=[(225/256.0,10/256.0,10/256.0)])
        plt.barh(x[2], mean_list(oblidb_bdb1)[j],  xerr=error_bar_list(oblidb_bdb1)[j], color=[(76/256.0, 114/256.0, 176/256.0)])
        plt.xlim([0, mean_list(oblidb_bdb1)[j] * 1.13])
        # plt.barh(x[4], mean_list(opaque_bdb1)[j],  xerr=error_bar_list(opaque_bdb1)[j],color=[(155/256.0, 99/256.0, 181/256.0)])
        # plt.barh(x[5], mean_list(opaque_oblivious_bdb1)[j], xerr=error_bar_list(opaque_oblivious_bdb1)[j],color=[(76/256.0, 114/256.0, 176/256.0)])
        #TODO replace the color RGB values
        # plt.barh(x, y_val,xerr=y_err,color=[(173/256.0, 255/256.0, 51/256.0),
        #                 (255/256.0, 0/256.0, 0/256.0),
        #                 (255/256.0, 153/256.0, 0/256.0),

        #                 (153/256.0, 204/256.0, 255/256.0),
        #                 (194/256.0, 102/256.0, 255/256.0),
        #                 (0/256.0, 255/256.0, 255/256.0)],
        #                 hatch = ["", "x", "+","","",""])
        #ax = df.plot.barh(color=["yellow","green","blue","red","black","orange"])
        if(j == 0):
            plt.yticks(x, xais, fontsize = bdbperf_ticks_size)
        else:
            plt.yticks(x, null_axis, fontsize = 5)
        #plt.gca().axes.get_yaxis().set_ticks([])
        plt.xticks( fontsize=bdbperf_ticks_size)
        #plt.ylim(0, mean_list(opaque_oblivious_bdb1)[j]*1.1 + 0.5)
        plt.xlabel("Running time(sec)", fontsize=bdbperf_ticks_size)
        #plt.ylabel("system type", fontsize=ticks_size)
        plt.text(y=x[0] - 0.1, x = mean_list(sparksql_bdb1)[j]*1.1  , s = round_helper(mean_list(sparksql_bdb1)[j]), size = bdbperf_ticks_size)
        #plt.text(y=x[1]- 0.1, x=mean_list(hundun_parallel_bdb1)[j]*1.02 , s=round_helper(mean_list(hundun_parallel_bdb1)[j]), size=bdbperf_ticks_size)
        plt.text(y=x[1]- 0.1, x=mean_list(hundun_singlethread_bdb1)[j]*1.02  , s=round_helper(mean_list(hundun_singlethread_bdb1)[j]), size=bdbperf_ticks_size)
        #plt.text(y=x[4]- 0.1, x=mean_list(opaque_bdb1)[j]*1.02 , s=round_helper(mean_list(opaque_bdb1)[j]), size=bdbperf_ticks_size)
        #plt.text(y=x[5]- 0.1, x=mean_list(opaque_oblivious_bdb1)[j]*1.01 , s=round_helper(mean_list(opaque_oblivious_bdb1)[j]), size=bdbperf_ticks_size)
        plt.text(y=x[2]- 0.1, x=mean_list(oblidb_bdb1)[j]*1.01 , s=round_helper(mean_list(oblidb_bdb1)[j]), size=bdbperf_ticks_size)

        #plt.legend(loc="upper left", bbox_to_anchor=(0, -0.2), ncol=3, fontsize=28)
        #plt.show()
        plt.savefig("BDB1_{}.pdf".format(bdb1_sizes[j]), dpi=None, facecolor='w', edgecolor='w',
                    orientation='portrait', bbox_inches='tight')

    bdb2_sizes = [ "300K", "3M", "30M"]
    for j in range(len(bdb2_sizes)):
        plt.figure(figsize=(10, 3), dpi=300, facecolor='w', edgecolor='k')
        #plt.title("BDB2 over {} rows".format(bdb2_sizes[j]), fontsize=25)
        plt.barh(x[0], mean_list(sparksql_bdb2)[j], xerr=error_bar_list(sparksql_bdb2)[j], color=[(77/256.0, 153/256.0, 0/256.0)])
        # plt.barh(x[1], mean_list(hundun_parallel_bdb2)[j], color=[(255/256.0,0/256.0,0/256.0)],xerr=error_bar_list(hundun_parallel_bdb2)[j]
        #         )
        plt.barh(x[1], mean_list(hundun_singlethread_bdb2)[j], color=[(225/256.0,10/256.0,10/256.0)],  xerr=error_bar_list(hundun_singlethread_bdb2)[j]
                )
        plt.barh(x[2], mean_list(oblidb_bdb2)[j], xerr=error_bar_list(oblidb_bdb2)[j], color=[(76/256.0, 114/256.0, 176/256.0)])
        plt.xlim([0, mean_list(hundun_singlethread_bdb2)[j] * 1.13])

        # plt.barh(x[4], mean_list(opaque_bdb2)[j], xerr=error_bar_list(opaque_bdb2)[j], color=[(155/256.0, 99/256.0, 181/256.0)])
        # plt.barh(x[5], mean_list(opaque_oblivious_bdb2)[j], xerr=error_bar_list(opaque_oblivious_bdb2)[j], color=[(76/256.0, 114/256.0, 176/256.0)])
        
        y_err = [error_bar_list(sparksql_bdb2)[j],error_bar_list(hundun_parallel_bdb2)[j],error_bar_list(hundun_singlethread_bdb2)[j],error_bar_list(oblidb_bdb2)[j],error_bar_list(opaque_bdb2)[j],error_bar_list(opaque_oblivious_bdb2)[j] ]

        y_val = [mean_list(sparksql_bdb2)[j],mean_list(hundun_singlethread_bdb2)[j], mean_list(oblidb_bdb2)[j] ]
        df = pd.DataFrame({"Running time(sec)" : y_val}, index = xais )
        #TODO replace the color RGB values
        # plt.barh(x, y_val,xerr=y_err,color=[(173/256.0, 255/256.0, 51/256.0),
        #                 (255/256.0, 0/256.0, 0/256.0),
        #                 (255/256.0, 153/256.0, 0/256.0),

        #                 (153/256.0, 204/256.0, 255/256.0),
        #                 (194/256.0, 102/256.0, 255/256.0),
        #                 (0/256.0, 255/256.0, 255/256.0)])
        #ax = df.plot.barh(color=["yellow","green","blue","red","black","orange"])
        if(j == 0):
            plt.yticks(x, xais, fontsize = bdbperf_ticks_size)
        else:
            plt.yticks(x, null_axis, fontsize = 5)
        #plt.gca().axes.get_yaxis().set_ticks([])
        plt.xticks( fontsize=bdbperf_ticks_size)
        #plt.ylim(0, mean_list(opaque_oblivious_bdb1)[j]*1.1 + 0.5)
        plt.xlabel("Running time(sec)", fontsize=bdbperf_ticks_size)

        plt.text(y=x[0] - 0.1, x = mean_list(sparksql_bdb2)[j]*1.1  , s = round_helper(mean_list(sparksql_bdb2)[j]), size = bdbperf_ticks_size)
        #plt.text(y=x[1]- 0.1, x=mean_list(hundun_parallel_bdb2)[j]*1.02 , s=round_helper(mean_list(hundun_parallel_bdb2)[j]), size=bdbperf_ticks_size)
        plt.text(y=x[1]- 0.1, x=mean_list(hundun_singlethread_bdb2)[j]*1.02  , s=round_helper(mean_list(hundun_singlethread_bdb2)[j]), size=bdbperf_ticks_size)
        # if(mean_list(opaque_oblivious_bdb2)[j] == 0):
        #     plt.text(y=x[4] - 0.1, x=mean_list(opaque_bdb2)[j] * 1.01 + 10, s=r"$\bf{N/A}$", size=bdbperf_ticks_size)
        #     plt.text(y=x[5] - 0.1, x=mean_list(opaque_oblivious_bdb2)[j] * 1.01 + 10, s=r"$\bf{N/A}$", size=bdbperf_ticks_size)

        #     #plt.ylim(0, mean_list(hundun_singlethread_bdb2)[j] * 1.2)
        # else:
        #     plt.text(y=x[4]- 0.1 - 0.01 * j, x=mean_list(opaque_bdb2)[j]*1.01 , s=round_helper(mean_list(opaque_bdb2)[j]), size=bdbperf_ticks_size)
        #     plt.text(y=x[5]- 0.1 - 0.02 * j, x=mean_list(opaque_oblivious_bdb2)[j]*1.01 , s=round_helper(mean_list(opaque_oblivious_bdb2)[j]), size=bdbperf_ticks_size)

        if(mean_list(oblidb_bdb2)[j] == 0):
            plt.text(y=x[2]- 0.1, x=mean_list(oblidb_bdb2)[j]*1.02 , s=r"$\bf{N/A}$", size=bdbperf_ticks_size)
        else:
            plt.text(y=x[2]- 0.1  - 0.03 * j, x=mean_list(oblidb_bdb2)[j]*1.01 , s=round_helper(mean_list(oblidb_bdb2)[j]), size=bdbperf_ticks_size)
            #plt.ylim(0, mean_list(opaque_oblivious_bdb2)[j] * 1.2)
        #plt.legend(loc="upper left", bbox_to_anchor=(0, -0.2), ncol=3, fontsize=28)
        #plt.show()
        plt.savefig("BDB2_{}.pdf".format(bdb2_sizes[j]), dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')
    # x = np.linspace(0, 6, 7)
    x = np.linspace(0, 2, 3)
    bdb3_sizes = [ "300K", "900K", "3M"]
    xais = ["Spark SQL", r"$\bf{ours}$", "ObliDB"]
    null_axis = ["", "",""]

    for j in range(len(bdb3_sizes)):
        plt.figure(figsize=(10,3), dpi=300, facecolor='w', edgecolor='k')
        # plt.title("BDB3 over {} rows".format(bdb3_sizes[j]), fontsize=25)
        y_err = [error_bar_list(sparksql_bdb3)[j],error_bar_list(hundun_parallel_bdb3)[j],error_bar_list(hundun_singlethread_bdb3)[j], error_bar_list(hundun_singlethread_bdb3_using_oblidb_sort)[j], error_bar_list(oblidb_bdb3)[j],error_bar_list(opaque_bdb3)[j],error_bar_list(opaque_oblivious_bdb3)[j] ]



        y_val = [mean_list(sparksql_bdb3)[j],mean_list(hundun_singlethread_bdb3)[j], mean_list(oblidb_bdb3)[j]]
        df = pd.DataFrame({"Running time(sec)" : y_val}, index = xais )
        #TODO replace the color RGB values
        plt.barh(x[0], mean_list(sparksql_bdb3)[j], xerr=error_bar_list(sparksql_bdb3)[j], color=[(77/256.0, 153/256.0, 0/256.0)])
        # plt.barh(x[1], mean_list(hundun_parallel_bdb3)[j], xerr=error_bar_list(hundun_parallel_bdb3)[j], color=[(255/256.0,0/256.0,0/256.0)])
        plt.barh(x[1], mean_list(hundun_singlethread_bdb3)[j], xerr=error_bar_list(hundun_singlethread_bdb3)[j],color=[(225/256.0,10/256.0,10/256.0)])
        # plt.barh(x[3], mean_list(hundun_singlethread_bdb3_using_oblidb_sort)[j], xerr=error_bar_list(hundun_singlethread_bdb3_using_oblidb_sort)[j],color=[(255/256.0,180/256.0,180/256.0)])
        plt.barh(x[2], mean_list(oblidb_bdb3)[j], xerr=error_bar_list(oblidb_bdb3)[j], color=[(76/256.0, 114/256.0, 176/256.0)])

        # plt.barh(x[5], mean_list(opaque_bdb3)[j], xerr=error_bar_list(opaque_bdb3)[j], color=[(155/256.0, 99/256.0, 181/256.0)])
        # plt.barh(x[6], mean_list(opaque_oblivious_bdb3)[j], xerr=error_bar_list(opaque_oblivious_bdb3)[j],color=[(76/256.0, 114/256.0, 176/256.0)])
        # plt.barh(x, y_val,xerr=y_err,color=[(173/256.0, 255/256.0, 51/256.0),
        #                 (255/256.0, 0/256.0, 0/256.0),
        #                 (255/256.0, 153/256.0, 0/256.0),
        #                 (255/256.0, 200/256.0, 0/256.0),

        #                 (153/256.0, 204/256.0, 255/256.0),
        #                 (194/256.0, 102/256.0, 255/256.0),
        #                 (0/256.0, 255/256.0, 255/256.0)])

        plt.xlim([0, mean_list(oblidb_bdb3)[j] * 1.13])


        #ax = df.plot.barh(color=["yellow","green","blue","red","black","orange"])
        if(j == 0):
            plt.yticks(x, xais, fontsize = bdbperf_ticks_size)
        else:
            plt.yticks(x, null_axis, fontsize = 5)
        #plt.gca().axes.get_yaxis().set_ticks([])
        plt.xticks( fontsize=bdbperf_ticks_size)
        #plt.ylim(0, mean_list(opaque_oblivious_bdb1)[j]*1.1 + 0.5)
        plt.xlabel("Running time(sec)", fontsize=bdbperf_ticks_size)

        plt.text(y=x[0] - 0.1, x=mean_list(sparksql_bdb3)[j] * 1.1, s=round_helper(mean_list(sparksql_bdb3)[j]),
                 size=bdbperf_ticks_size)
        # plt.text(y=x[1] - 0.1, x=mean_list(hundun_parallel_bdb3)[j] * 1.02, s=round_helper(mean_list(hundun_parallel_bdb3)[j]),
        #          size=bdbperf_ticks_size)
        plt.text(y=x[1] - 0.1 - 0.01*j, x=mean_list(hundun_singlethread_bdb3)[j] * 1.02,
                 s=round_helper(mean_list(hundun_singlethread_bdb3)[j]), size=bdbperf_ticks_size)
        # plt.text(y=x[3] - 0.1 - 0.01*j, x=mean_list(hundun_singlethread_bdb3_using_oblidb_sort)[j] * 1.02,
        #          s=round_helper(mean_list(hundun_singlethread_bdb3_using_oblidb_sort)[j]), size=bdbperf_ticks_size)
        plt.text(y=x[2]- 0.1 - 0.01*j, x=mean_list(oblidb_bdb3)[j]*1.01 , s=round_helper(mean_list(oblidb_bdb3)[j]), size=bdbperf_ticks_size)

        # if (mean_list(opaque_oblivious_bdb3)[j] == 0):
        #     plt.text(y=x[4]- 0.1 - 0.01*j, x=mean_list(oblidb_bdb3)[j]*1.01 + 30 , s=r"$\bf{N/A}$", size=bdbperf_ticks_size)
        #     # plt.text(y=x[5] - 0.1 - 0.01*j, x=mean_list(opaque_bdb3)[j] * 1.01 + 30, s=r"$\bf{N/A}$", size=bdbperf_ticks_size)
        #     # plt.text(y=x[6] - 0.1 - 0.01*j, x=mean_list(opaque_oblivious_bdb3)[j] * 1.01 + 30, s=r"$\bf{N/A}$", size=bdbperf_ticks_size)

        # else:

        #     plt.text(y=x[4]- 0.1 - 0.01*j, x=mean_list(oblidb_bdb3)[j]*1.01 , s=round_helper(mean_list(oblidb_bdb3)[j]), size=bdbperf_ticks_size)
        #     plt.text(y=x[5] - 0.1 - 0.01*j, x=mean_list(opaque_bdb3)[j] * 1.01, s=round_helper(mean_list(opaque_bdb3)[j]),
        #              size=bdbperf_ticks_size)
        #     plt.text(y=x[6] - 0.1 - 0.01*j, x=mean_list(opaque_oblivious_bdb3)[j] * 1.01,
        #              s=round_helper(mean_list(opaque_oblivious_bdb3)[j]), size=bdbperf_ticks_size)

        #plt.legend(loc="upper left", bbox_to_anchor=(0, -0.2), ncol=3, fontsize=28)
        # plt.show()
        plt.savefig("BDB3_{}.pdf".format(bdb3_sizes[j]), dpi=None, facecolor='w', edgecolor='w',
                    orientation='portrait', bbox_inches='tight')
breakdown_width = 0.2

def draw_filter_time_breakdown():
    # HUNDUN parallel Project and filter operator time cost break down
    x = np.linspace(0, len(hundun_parallel_bdb1) - 2, len(hundun_parallel_bdb1))
    xais = [ "100K", "1M", "10M"]
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])

    plt.title("HUNDUN parallel Filter operator", fontsize=title_size)
    plt.bar(x, filter_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)],width=0.3, label='Encrypt')
    plt.bar(x, filter_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)],width=0.3, bottom=filter_parallel_breakdown[0],
            label='Decrypt')
    # plt.bar(x, filter_parallel_breakdown[2],color=[(0,187/256.0,116/256.0)], width=breakdown_width, bottom=[x + y for (x, y) in zip(
    #     filter_parallel_breakdown[0], filter_parallel_breakdown[1])],
    #         label="within_enclave_memcpy")
    plt.bar(x, filter_parallel_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=0.3, bottom=[x + y for (x, y) in zip(
        filter_parallel_breakdown[0], filter_parallel_breakdown[1])], label='Read')
    plt.bar(x, filter_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=0.3, bottom=[x + y + z for (x, y, z) in zip(
        filter_parallel_breakdown[0], filter_parallel_breakdown[1],
        filter_parallel_breakdown[2])],
            label='Write')
    plt.bar(x, filter_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=0.3, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                            zip(filter_parallel_breakdown[
                                                                                    0],
                                                                                filter_parallel_breakdown[
                                                                                    1],
                                                                                filter_parallel_breakdown[
                                                                                    2],
                                                                                filter_parallel_breakdown[
                                                                                    3])], label='Compute')
    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais,fontsize=ticks_size)
    plt.ylabel("Time breakdown", fontsize=ticks_size)
    plt.xlabel("Rankings table size", fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("parallel_filter_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')

    # HUNDUN single thread Project and filter operator time cost break down
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])

    plt.title("HUNDUN single thread Filter operator", fontsize=title_size)
    plt.bar(x, filter_single_breakdown[0],color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    plt.bar(x, filter_single_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width,
            bottom=filter_single_breakdown[0], label='Decrypt')
    plt.bar(x, filter_single_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y for (x, y) in zip(
        filter_single_breakdown[0], filter_single_breakdown[1])], label='Read')
    plt.bar(x, filter_single_breakdown[3],color=[(76/256.0, 114/256.0, 176/256.0)], width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
        filter_single_breakdown[0], filter_single_breakdown[1],
        filter_single_breakdown[2])],
            label='Write')
    plt.bar(x, filter_single_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                                zip(
                                                                                    filter_single_breakdown[
                                                                                        0],
                                                                                    filter_single_breakdown[
                                                                                        1],
                                                                                    filter_single_breakdown[
                                                                                        2],
                                                                                    filter_single_breakdown[
                                                                                        3])], label='Compute')
    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais, fontsize=ticks_size)
    plt.ylabel("Time breakdown",fontsize=ticks_size)
    plt.xlabel("Rankings table size",fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("singlethread_filter_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')

def draw_project_time_breakdown():
    # HUNDUN parallel Project and filter operator time cost break down
    print("filter time break down ", sortjoin_single_breakdown)
    x = np.linspace(0, len(hundun_parallel_bdb1) - 2, len(hundun_parallel_bdb1))
    xais = [ "100K", "1M", "10M"]
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("parallel Project", fontsize=title_size)
    plt.bar(x, sortjoin_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)],width=breakdown_width, label='Encrypt')
    plt.bar(x, sortjoin_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)],width=breakdown_width, bottom=sortjoin_parallel_breakdown[0],
            label='Decrypt')
    plt.bar(x, sortjoin_parallel_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y  for (x, y) in zip(
        sortjoin_parallel_breakdown[0], sortjoin_parallel_breakdown[1])], label='Read')
    plt.bar(x, sortjoin_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
        sortjoin_parallel_breakdown[0], sortjoin_parallel_breakdown[1],
        sortjoin_parallel_breakdown[2])],
            label='Write')
    plt.bar(x, sortjoin_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k for (x, y, z, k) in
                                                                            zip(sortjoin_parallel_breakdown[
                                                                                    0],
                                                                                sortjoin_parallel_breakdown[
                                                                                    1],
                                                                                sortjoin_parallel_breakdown[
                                                                                    2],
                                                                                sortjoin_parallel_breakdown[
                                                                                    3])], label='Compute')
    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais,fontsize=ticks_size)
    plt.ylabel("Time breakdown", fontsize=ticks_size)
    plt.xlabel("Rankings table size", fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("parallel_project_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')

    # HUNDUN single thread Project and filter operator time cost break down
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("single thread Project", fontsize=title_size)
    plt.bar(x, sortjoin_single_breakdown[0],color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    plt.bar(x, sortjoin_single_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width,
            bottom=sortjoin_single_breakdown[0], label='Decrypt')
    plt.bar(x, sortjoin_single_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y for (x, y) in zip(
        sortjoin_single_breakdown[0], sortjoin_single_breakdown[1])], label='Read')
    plt.bar(x, sortjoin_single_breakdown[3],color=[(76/256.0, 114/256.0, 176/256.0)], width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
        sortjoin_single_breakdown[0], sortjoin_single_breakdown[1],
        sortjoin_single_breakdown[2])],
            label='Write')
    plt.bar(x, sortjoin_single_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                                zip(
                                                                                    sortjoin_single_breakdown[
                                                                                        0],
                                                                                    sortjoin_single_breakdown[
                                                                                        1],
                                                                                    sortjoin_single_breakdown[
                                                                                        2],
                                                                                    sortjoin_single_breakdown[
                                                                                        3])], label='Compute')
    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais, fontsize=ticks_size)
    plt.ylabel("Time breakdown",fontsize=ticks_size)
    plt.xlabel("Rankings table size",fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("singlethread_project_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')


def draw_hashgroupby_time_breakdown():
    # HashGroupby operator time cost break down
    x = np.linspace(0, len(hundun_parallel_bdb2) - 2, len(hundun_parallel_bdb2))
    xais = ["300K", "3M", "30M"]
    print("hashgroup breakdown ", hash_groupby_singlethread_breakdown)
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("parallel HashGroupby", fontsize=title_size)
    plt.bar(x, hash_groupby_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    plt.bar(x, hash_groupby_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width, bottom=hash_groupby_parallel_breakdown[0],
            label='Decrypt')
    plt.bar(x, hash_groupby_parallel_breakdown[2],color=[(155/256.0, 99/256.0, 181/256.0)], width=breakdown_width, bottom=[x + y for (x, y) in
                                                                      zip(hash_groupby_parallel_breakdown[0],
                                                                          hash_groupby_parallel_breakdown[1])],
            label='Read')
    plt.bar(x, hash_groupby_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=breakdown_width, bottom=[x + y + z for (x, y, z) in
                                                                      zip(hash_groupby_parallel_breakdown[0],
                                                                          hash_groupby_parallel_breakdown[1],
                                                                          hash_groupby_parallel_breakdown[2])],
            label='Write')
    plt.bar(x, hash_groupby_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                      zip(hash_groupby_parallel_breakdown[0],
                                                                          hash_groupby_parallel_breakdown[1],
                                                                          hash_groupby_parallel_breakdown[2],
                                                                          hash_groupby_parallel_breakdown[3])],
            label='Compute')

    plt.xticks(x, xais,fontsize=ticks_size)
    plt.yticks(fontsize=ticks_size)
    plt.ylabel("Time breakdown", fontsize=ticks_size)
    plt.xlabel("UserVisits table size", fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("parallel_hashgroupby_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')

    # HashGroupby operator time cost break down
    plt.figure(figsize=(10,6), dpi=300, facecolor='w', edgecolor='k')
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("single thread HashGroupby", fontsize=title_size)
    plt.bar(x, hash_groupby_singlethread_breakdown[0], width=breakdown_width, color=[(228/256.0,26/256.0,28/256.0)], label='Encrypt')
    plt.bar(x, hash_groupby_singlethread_breakdown[1], width=breakdown_width, color=[(255/256.0, 157/256.0, 65/256.0)], bottom=hash_groupby_singlethread_breakdown[0],
            label='Decrypt')
    plt.bar(x, hash_groupby_singlethread_breakdown[2], width=breakdown_width, color=[(155/256.0, 99/256.0, 181/256.0)], bottom=[x + y  for (x, y) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1])],
            label='Read')
    plt.bar(x, hash_groupby_singlethread_breakdown[4], width=breakdown_width, color=[(76/256.0, 114/256.0, 176/256.0)], bottom=[x + y + z for (x, y, z) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1],
                                                                              hash_groupby_singlethread_breakdown[2])],
            label='Write')
    plt.bar(x, hash_groupby_singlethread_breakdown[4], width=breakdown_width, color=[(140/256.0, 86/256.0, 75/256.0)],bottom=[x + y + z + k  for (x, y, z, k) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1],
                                                                              hash_groupby_singlethread_breakdown[2],
                                                                              hash_groupby_singlethread_breakdown[3])],
            label='Compute')


    plt.xticks(x, xais,fontsize=ticks_size)
    plt.yticks(fontsize=ticks_size)
    plt.ylabel("Time breakdown", fontsize=ticks_size)
    plt.xlabel("UserVisits table size", fontsize=ticks_size)
    plt.legend(loc="upper left",bbox_to_anchor=(0, -0.2), ncol=3, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("hash_groupby_singlethread_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')


def draw_padding_comparison():
    #bdb1
    width = 0.4
    x = np.linspace(0, 2, 3)
    xais = ["100K", "1M", "10M"]
    fig, axes = plt.subplots(1, 3, figsize=(25,6), sharey=True, sharex=True)
    fig.text(0.5, -0.07, 'Input table size', ha='center', fontsize= 42)
    fig.text(0.07, 0.5, 'Padding size', va='center', rotation='vertical', fontsize= 42)
    plt.subplot(1,3,1)
    plt.title("BDB1", fontsize=45)
    plt.bar(x - width/2, [65543,65327,65461], width=width, label="Diff. Obl.")
    plt.bar(x + width/2 , [99967, 999612, 9996296], width=width, label="Full Obl.")
    plt.xticks(x, xais, fontsize=padding_ticks_size)
    plt.yticks(fontsize=padding_ticks_size)
    plt.yscale("log")



    bdb2_size = [ "300K", "3M", "30M"]
    plt.subplot(1,3,2)
    plt.title("BDB2", fontsize=45)
    plt.bar(x - width / 2, [191763, 77703, 164782], width=width, label="Diff. Obl.")
    plt.bar(x + width / 2, [142634, 2377703, 28414792], width=width, label="Full Obl.")
    plt.xticks(x, bdb2_size, fontsize=padding_ticks_size)
    plt.yticks(fontsize=padding_ticks_size)
    plt.yscale("log")



    bdb3_size = [ "300K", "3M", "30M"]
    plt.subplot(1,3,3)
    plt.title("BDB3", fontsize=45)
    plt.bar(x - width / 2, [1220000, 3291851, 21074695], width=width, label="Diff. Obl.")
    plt.bar(x + width / 2, [272059+372060+374770, 3850000+ 2720000 + 3720000 ,  27292000 + 37292000 + 39400000], width=width, label="Full Obl.")
    plt.xticks(x, bdb3_size, fontsize=padding_ticks_size)
    plt.yticks(fontsize=padding_ticks_size)
    plt.yscale("log")

    plt.legend(loc="upper center",bbox_to_anchor=(-0.7, -0.25), ncol=2, fontsize=50)
    #plt.show()
    plt.savefig("padding_bdb.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')





def draw_perf_breakdown():
    x = np.linspace(0, len(hundun_parallel_bdb1) - 2, len(hundun_parallel_bdb1))
    xais = [ "100K", "1M", "10M"]
    fig, axes = plt.subplots(1, 6, figsize=(20,6), sharey=True, sharex=True)
    fig.text(0.5, -0.1, 'Input table size', ha='center', fontsize= 42)
    fig.text(0.08, 0.5, 'Time breakdown', va='center', rotation='vertical', fontsize= 42)
    #fig = plt.figure(figsize=(40,6), dpi=300)
    

    plt.subplot(1,3,1)
    plt.gca().axes.get_yaxis().set_ticks([])

    plt.title("Selection with projection", fontsize=title_size)
    plt.bar(x, filter_single_breakdown[0],color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    plt.bar(x, filter_single_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width,
            bottom=filter_single_breakdown[0], label='Decrypt')
    plt.bar(x, filter_single_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y for (x, y) in zip(
        filter_single_breakdown[0], filter_single_breakdown[1])], label='Read')
    plt.bar(x, filter_single_breakdown[3],color=[(76/256.0, 114/256.0, 176/256.0)], width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
        filter_single_breakdown[0], filter_single_breakdown[1],
        filter_single_breakdown[2])],
            label='Write')
    plt.bar(x, filter_single_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                                zip(
                                                                                    filter_single_breakdown[
                                                                                        0],
                                                                                    filter_single_breakdown[
                                                                                        1],
                                                                                    filter_single_breakdown[
                                                                                        2],
                                                                                    filter_single_breakdown[
                                                                                        3])], label='Compute')
    
    for i in range(len(xais)):
        plt.text(0.13+ 0.52*i, (filter_single_breakdown[0][i] +
                                filter_single_breakdown[1][i] +
                                filter_single_breakdown[2][i] +
                                filter_single_breakdown[3][i]) * 0.5 , "{}%".format(round((filter_single_breakdown[0][i] +
                                filter_single_breakdown[1][i] +
                                filter_single_breakdown[2][i] +
                                filter_single_breakdown[3][i]) * 100, 1) ), size=16)

        plt.annotate(text='', xy=(0.12 + 0.5*i,(filter_single_breakdown[0][i] +
                                filter_single_breakdown[1][i] +
                                filter_single_breakdown[2][i] +
                                filter_single_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))


    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais, fontsize=ticks_size)
    #plt.ylabel("Time breakdown",fontsize=ticks_size)
    #plt.xlabel("Input table size",fontsize=ticks_size)
    # HUNDUN single thread Project and filter operator time cost break down

    xais = [ "300K", "900K", "3M"]

    plt.subplot(1,3,2)
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("Sort foreign key join", fontsize=title_size)
    plt.bar(x, sortjoin_single_breakdown[0],color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    plt.bar(x, sortjoin_single_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width,
            bottom=sortjoin_single_breakdown[0], label='Decrypt')
    plt.bar(x, sortjoin_single_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y for (x, y) in zip(
        sortjoin_single_breakdown[0], sortjoin_single_breakdown[1])], label='Read')
    plt.bar(x, sortjoin_single_breakdown[3],color=[(76/256.0, 114/256.0, 176/256.0)], width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
        sortjoin_single_breakdown[0], sortjoin_single_breakdown[1],
        sortjoin_single_breakdown[2])],
            label='Write')
    plt.bar(x, sortjoin_single_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
                                                                                zip(
                                                                                    sortjoin_single_breakdown[
                                                                                        0],
                                                                                    sortjoin_single_breakdown[
                                                                                        1],
                                                                                    sortjoin_single_breakdown[
                                                                                        2],
                                                                                    sortjoin_single_breakdown[
                                                                                        3])], label='Compute')
    for i in range(len(xais)):
        plt.text(0.13+ 0.52*i, (sortjoin_single_breakdown[0][i] +
                            sortjoin_single_breakdown[1][i] +
                            sortjoin_single_breakdown[2][i] +
                            sortjoin_single_breakdown[3][i]) * 0.5 , "{}%".format(round((sortjoin_single_breakdown[0][i] +
                            sortjoin_single_breakdown[1][i] +
                            sortjoin_single_breakdown[2][i] +
                            sortjoin_single_breakdown[3][i]) * 100, 1) ), size=16)

        plt.annotate( text="",xy=(0.12 + 0.5*i,(sortjoin_single_breakdown[0][i] +
                                sortjoin_single_breakdown[1][i] +
                                sortjoin_single_breakdown[2][i] +
                                sortjoin_single_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))
                            
    plt.yticks(fontsize=ticks_size)
    plt.xticks(x, xais, fontsize=ticks_size)
    #plt.ylabel("Time breakdown",fontsize=ticks_size)
    #plt.xlabel("Input table size",fontsize=ticks_size)

    # HashGroupby operator time cost break down
    xais = ["300K", "3M", "30M"]    
    plt.subplot(1,3,3)
    plt.gca().axes.get_yaxis().set_ticks([])
    plt.title("Hash groupby", fontsize=title_size)
    plt.bar(x, hash_groupby_singlethread_breakdown[0], width=breakdown_width, color=[(228/256.0,26/256.0,28/256.0)], label='Encrypt')
    plt.bar(x, hash_groupby_singlethread_breakdown[1], width=breakdown_width, color=[(255/256.0, 157/256.0, 65/256.0)], bottom=hash_groupby_singlethread_breakdown[0],
            label='Decrypt')
    plt.bar(x, hash_groupby_singlethread_breakdown[2], width=breakdown_width, color=[(155/256.0, 99/256.0, 181/256.0)], bottom=[x + y  for (x, y) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1])],
            label='Read')
    plt.bar(x, hash_groupby_singlethread_breakdown[4], width=breakdown_width, color=[(76/256.0, 114/256.0, 176/256.0)], bottom=[x + y + z for (x, y, z) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1],
                                                                              hash_groupby_singlethread_breakdown[2])],
            label='Write')
    plt.bar(x, hash_groupby_singlethread_breakdown[4], width=breakdown_width, color=[(140/256.0, 86/256.0, 75/256.0)],bottom=[x + y + z + k  for (x, y, z, k) in
                                                                          zip(hash_groupby_singlethread_breakdown[0],
                                                                              hash_groupby_singlethread_breakdown[1],
                                                                              hash_groupby_singlethread_breakdown[2],
                                                                              hash_groupby_singlethread_breakdown[3])],
            label='Compute')

    for i in range(len(xais)):
        plt.text(0.13+ 0.52*i, (hash_groupby_singlethread_breakdown[0][i] +
                            hash_groupby_singlethread_breakdown[1][i] +
                            hash_groupby_singlethread_breakdown[2][i] +
                            hash_groupby_singlethread_breakdown[3][i]) * 0.5 , "{}%".format(round((hash_groupby_singlethread_breakdown[0][i] +
                            hash_groupby_singlethread_breakdown[1][i] +
                            hash_groupby_singlethread_breakdown[2][i] +
                            hash_groupby_singlethread_breakdown[3][i]) * 100, 1) ), size=16)

        plt.annotate( text="",xy=(0.12 + 0.5*i,(hash_groupby_singlethread_breakdown[0][i] +
                                hash_groupby_singlethread_breakdown[1][i] +
                                hash_groupby_singlethread_breakdown[2][i] +
                                hash_groupby_singlethread_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))
    plt.xticks(x, xais,fontsize=ticks_size)
    plt.yticks(fontsize=ticks_size)
    #plt.ylabel("Time breakdown", fontsize=ticks_size)
    #plt.xlabel("Input table size", fontsize=ticks_size)
    
    
    # xais = [ "100K", "1M", "10M"]
    # plt.subplot(1,6,4)
    # plt.gca().axes.get_yaxis().set_ticks([])

    # plt.title("Parallel Filter", fontsize=title_size)
    # plt.bar(x, filter_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)],width=breakdown_width, label='Encrypt')
    # plt.bar(x, filter_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)],width=breakdown_width, bottom=filter_parallel_breakdown[0],
    #         label='Decrypt')
    # plt.bar(x, filter_parallel_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y for (x, y) in zip(
    #     filter_parallel_breakdown[0], filter_parallel_breakdown[1])], label='Read')
    # plt.bar(x, filter_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=breakdown_width, bottom=[x + y + z for (x, y, z) in zip(
    #     filter_parallel_breakdown[0], filter_parallel_breakdown[1],
    #     filter_parallel_breakdown[2])],
    #         label='Write')
    # plt.bar(x, filter_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
    #                                                                         zip(filter_parallel_breakdown[
    #                                                                                 0],
    #                                                                             filter_parallel_breakdown[
    #                                                                                 1],
    #                                                                             filter_parallel_breakdown[
    #                                                                                 2],
    #                                                                             filter_parallel_breakdown[
    #                                                                                 3])], label='Compute')
    # for i in range(len(xais)):
    #     plt.text(0.13+ 0.5*i, (filter_parallel_breakdown[0][i] +
    #                     filter_parallel_breakdown[1][i] +
    #                     filter_parallel_breakdown[2][i] +
    #                     filter_parallel_breakdown[3][i]) * 0.5 , "{}%".format(round((filter_parallel_breakdown[0][i] +
    #                     filter_parallel_breakdown[1][i] +
    #                     filter_parallel_breakdown[2][i] +
    #                     filter_parallel_breakdown[3][i]) * 100, 1) ), size=20)

    #     plt.annotate( text="",xy=(0.12 + 0.5*i,(filter_parallel_breakdown[0][i] +
    #                             filter_parallel_breakdown[1][i] +
    #                             filter_parallel_breakdown[2][i] +
    #                             filter_parallel_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))

    # plt.yticks(fontsize=ticks_size)
    # plt.xticks(x, xais,fontsize=ticks_size)
    # #plt.ylabel("Time breakdown", fontsize=ticks_size)
    # #plt.xlabel("Input table size", fontsize=ticks_size)


    # plt.subplot(1,6,5)
    # plt.gca().axes.get_yaxis().set_ticks([])
    # plt.title("Parallel SortJoin", fontsize=title_size)
    # plt.bar(x, sortjoin_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)],width=breakdown_width, label='Encrypt')
    # plt.bar(x, sortjoin_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)],width=breakdown_width, bottom=sortjoin_parallel_breakdown[0],
    #         label='Decrypt')
    # plt.bar(x, sortjoin_parallel_breakdown[2], color=[(155/256.0, 99/256.0, 181/256.0)],width=breakdown_width, bottom=[x + y  for (x, y) in zip(
    #     sortjoin_parallel_breakdown[0], sortjoin_parallel_breakdown[1])], label='Read')
    # plt.bar(x, sortjoin_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=breakdown_width, bottom=[x + y + z  for (x, y, z) in zip(
    #     sortjoin_parallel_breakdown[0], sortjoin_parallel_breakdown[1],
    #     sortjoin_parallel_breakdown[2])],
    #         label='Write')
    # plt.bar(x, sortjoin_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k for (x, y, z, k) in
    #                                                                         zip(sortjoin_parallel_breakdown[
    #                                                                                 0],
    #                                                                             sortjoin_parallel_breakdown[
    #                                                                                 1],
    #                                                                             sortjoin_parallel_breakdown[
    #                                                                    2],
    #                                                                             sortjoin_parallel_breakdown[
    #                                                                                 3])], label='Compute')
    
    # for i in range(len(xais)):
    #     plt.text(0.13+ 0.5*i, (sortjoin_parallel_breakdown[0][i] +
    #                     sortjoin_parallel_breakdown[1][i] +
    #                     sortjoin_parallel_breakdown[2][i] +
    #                     sortjoin_parallel_breakdown[3][i]) * 0.5 , "{}%".format(round((sortjoin_parallel_breakdown[0][i] +
    #                     sortjoin_parallel_breakdown[1][i] +
    #                     sortjoin_parallel_breakdown[2][i] +
    #                     sortjoin_parallel_breakdown[3][i]) * 100, 1) ), size=20)

    #     plt.annotate( text="",xy=(0.12 + 0.5*i,(sortjoin_parallel_breakdown[0][i] +
    #                             sortjoin_parallel_breakdown[1][i] +
    #                             sortjoin_parallel_breakdown[2][i] +
    #                             sortjoin_parallel_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))

    # plt.yticks(fontsize=ticks_size)
    # plt.xticks(x, xais,fontsize=ticks_size)
    # #plt.ylabel("Time breakdown", fontsize=ticks_size)
    # #plt.xlabel("Input table size", fontsize=ticks_size)


    



    # xais = ["300K", "3M", "30M"]
    # print("hashgroup breakdown ", hash_groupby_singlethread_breakdown)
    # plt.subplot(1,6,6)
    # plt.gca().axes.get_yaxis().set_ticks([])
    # plt.title("Parallel HashGroupby", fontsize=title_size)
    # plt.bar(x, hash_groupby_parallel_breakdown[0], color=[(228/256.0,26/256.0,28/256.0)], width=breakdown_width, label='Encrypt')
    # plt.bar(x, hash_groupby_parallel_breakdown[1], color=[(255/256.0, 157/256.0, 65/256.0)], width=breakdown_width, bottom=hash_groupby_parallel_breakdown[0],
    #         label='Decrypt')
    # plt.bar(x, hash_groupby_parallel_breakdown[2],color=[(155/256.0, 99/256.0, 181/256.0)], width=breakdown_width, bottom=[x + y for (x, y) in
    #                                                                   zip(hash_groupby_parallel_breakdown[0],
    #                                                                       hash_groupby_parallel_breakdown[1])],
    #         label='Read')
    # plt.bar(x, hash_groupby_parallel_breakdown[3], color=[(76/256.0, 114/256.0, 176/256.0)],width=breakdown_width, bottom=[x + y + z for (x, y, z) in
    #                                                                   zip(hash_groupby_parallel_breakdown[0],
    #                                                                       hash_groupby_parallel_breakdown[1],
    #                                                                       hash_groupby_parallel_breakdown[2])],
    #         label='Write')
    # plt.bar(x, hash_groupby_parallel_breakdown[4], color=[(140/256.0, 86/256.0, 75/256.0)],width=breakdown_width, bottom=[x + y + z + k  for (x, y, z, k) in
    #                                                                   zip(hash_groupby_parallel_breakdown[0],
    #                                                                       hash_groupby_parallel_breakdown[1],
    #                                                                       hash_groupby_parallel_breakdown[2],
    #                                                                       hash_groupby_parallel_breakdown[3])],
    #         label='Compute')
    # for i in range(len(xais)):
    #     plt.text(0.13+ 0.5*i, (hash_groupby_parallel_breakdown[0][i] +
    #                     hash_groupby_parallel_breakdown[1][i] +
    #                     hash_groupby_parallel_breakdown[2][i] +
    #                     hash_groupby_parallel_breakdown[3][i]) * 0.5 , "{}%".format(round((hash_groupby_parallel_breakdown[0][i] +
    #                     hash_groupby_parallel_breakdown[1][i] +
    #                     hash_groupby_parallel_breakdown[2][i] +
    #                     hash_groupby_parallel_breakdown[3][i]) * 100, 1) ), size=20)

    #     plt.annotate( text="",xy=(0.12 + 0.5*i,(hash_groupby_parallel_breakdown[0][i] +
    #                             hash_groupby_parallel_breakdown[1][i] +
    #                             hash_groupby_parallel_breakdown[2][i] +
    #                             hash_groupby_parallel_breakdown[3][i])), xytext=(0.12+ 0.5*i,0), arrowprops=dict(arrowstyle='<|-|>', lw=2))

    # plt.xticks(x, xais,fontsize=ticks_size)
    # plt.yticks(fontsize=ticks_size)
    # #plt.ylabel("Time breakdown", fontsize=ticks_size)
    # #plt.xlabel("Input table size", fontsize=ticks_size)



    




    plt.legend(loc="upper center",bbox_to_anchor=(-0.7, 1.35), ncol=5, fontsize=breakdown_legend_size)
    #plt.show()
    plt.savefig("operator_perf_breakdown.pdf", dpi=None, facecolor='w', edgecolor='w',
                orientation='portrait', bbox_inches='tight')

#draw_padding_comparison()
#draw_bdb_series_perf_comparison()
# draw_project_time_breakdown()
# draw_filter_time_breakdown()
# draw_hashgroupby_time_breakdown()
draw_perf_breakdown()