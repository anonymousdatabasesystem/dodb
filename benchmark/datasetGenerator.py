import random
import string
import pandas as pd
import numpy as np
import os



os.system("mkdir data")
for i in range(0, 10):
    idx = str(i)
    idx = '0' * (5-len(idx)) + idx 
    os.system("wget -P ./data/rankings https://s3.amazonaws.com/big-data-benchmark/pavlo/text/1node/rankings/part-{}".format(idx))

for i in range(0, 10):
    idx = str(i)
    idx = '0' * (5-len(idx)) + idx 
    os.system("wget -P ./data/uservisits https://s3.amazonaws.com/big-data-benchmark/pavlo/text/1node/uservisits/part-{}".format(idx))

os.system("cat ./data/rankings/part-* > ./data/rankings_sampling.csv")
os.system("cat ./data/uservisits/part-* > ./data/uservisits_sampling.csv")
os.system("rm ./data/rankings/part-*")
os.system("rm ./data/uservisits/part-*")


seed = "abcdefghijklmnopsdgafgvsdfgretaergafdasdfasdgvsdfagdfsadfasdfadssxgsdthyjnyjngadfawefaaqrstuvwxyz"
userAgents = ["iPhone 3.0: Mozilla/5.0 ", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)", "Mozilla/5.0 (Windows; U; Windows NT 5.2) ", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"]

# use rankings sample to sample pageRank column during data generation
rankings_sample = pd.read_csv("./data/rankings_sampling.csv")

def randomURLGenerator():
    salt = ''.join(random.sample(seed, random.randrange(15, 80)))
    return salt

def randomStringGenerator():
    salt = ''.join(random.sample(string.ascii_letters, random.randrange(15, 30)))
    return salt

def randomSourceIP():
    res = ""
    for i in range(4):
        res += str(random.randrange(1, 255))
        res += '.'
    res = res[:-1]
    return res

def randomVisitDate():
    year = str(random.randrange(1980, 2012))
    month = random.randrange(1, 13)
    day = random.randrange(1, 30)
    if(month < 10):
        month = "0"+str(month)
    else:
        month = str(month)

    if(day < 10):
        day = "0" + str(day)
    else:
        day = str(day)
    return year + "-" + month + "-" + day

def randomRevenue():
    return (float)(random.randrange(1, 1000000)) / 3000000

def ramdomRank():
    return int(rankings_sample.loc[random.randint(0, len(rankings_sample) - 1)][1])

size_list = [10000, 100000, 1000000, 10000000]

# for size in size_list:
#     pageURL = [randomURLGenerator() for i in range(size)]
#     sourceIP = [randomSourceIP() for i in range(size * 3)]
#     rankings = []
#     uservisits = []
#     rankings = [[pageURL[i], ramdomRank(), random.randrange(1, 100)] for i in range(size)]
#     uservisits = [[sourceIP[i], pageURL[random.randrange(1, size)], randomVisitDate(), randomRevenue(), userAgents[random.randrange(0, len(userAgents))],''.join(random.sample(string.ascii_letters, 3)), ''.join(random.sample(string.ascii_letters, 6)),
#                         randomStringGenerator(), random.randrange(1, 100)] for i in range(size * 3)]
#     rankings = pd.DataFrame(rankings)
#     uservisits = pd.DataFrame(uservisits)
#     os.system("mkdir ./join_data/rankings/{}/".format(size))
#     os.system("mkdir ./join_data/uservisits/{}/".format(size))
#     rankings.to_csv("./join_data/rankings/{}/rankings_{}.csv".format(size, size), header=False, index=False)
#     uservisits.to_csv("./join_data/uservisits/{}/uservisits_{}.csv".format(size, size), header=False, index=False)
#     print("finish size {}".format(size))


for size in size_list:
    pageURL = [randomURLGenerator() for i in range(size)]
    sourceIP = [randomSourceIP() for i in range(size * 3)]
    rankings = []
    uservisits = []
    rankings = [[pageURL[i], ramdomRank(), random.randrange(1, 100)] for i in range(size)]
    uservisits = [[''.join(random.sample(string.ascii_letters, 16)), pageURL[random.randrange(1, size)], randomVisitDate(), randomRevenue(), userAgents[random.randrange(0, len(userAgents))],''.join(random.sample(string.ascii_letters, 3)), ''.join(random.sample(string.ascii_letters, 6)),
                        randomStringGenerator(), random.randrange(1, 100)] for i in range(size * 3)]
    rankings = pd.DataFrame(rankings)
    uservisits = pd.DataFrame(uservisits)
    os.system("mkdir ./join_data/rankings/{}/".format(size))
    os.system("mkdir ./join_data/uservisits/{}/".format(size))
    rankings.to_csv("./join_data/rankings/{}/rankings_{}.csv".format(size, size), header=False, index=False)
    uservisits.to_csv("./join_data/uservisits/{}/distinct_uservisits_{}.csv".format(size, size), header=False, index=False)
    print("finish size {}".format(size))
