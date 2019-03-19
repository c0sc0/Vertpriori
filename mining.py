from csv import DictReader
from collections import defaultdict
import time

minimum_supp = 2


def candidateset(freqk, k):
    cset = set()
    for item in freqk:
        for other in freqk:
            cand = item.union(other)
            if item != other and len(cand) == k:
                cset.add(cand)
    return cset


def prune(candidate, minsupp):
    delete_these = list()
    for key, val in candidate.items():
        if len(val) < minsupp:
            delete_these.append(key)
    #print(delete_these)
    for pruned in delete_these:
        del candidate[pruned]


database = defaultdict(set)
ins = DictReader(open("test.csv"))
start = time.time()
#load the database into vertical format
for row in ins:
    database[frozenset([int(row["product_id"])])].add(row["order_id"])

#created L1
prune(database, minimum_supp)

l1 = database.keys()

#Created C2
cands = candidateset(l1, 2)

subset = dict()
k = 3
while cands:
    for s in cands: #create L(k)
        inter = database[frozenset([next(iter(s))])]
        for x in s:
            inter = inter.intersection(database[frozenset([x])])
        if inter:
            database[s] = inter
            subset[s] = inter
    #create C(k+1)
    prune(subset, minimum_supp)
    knext = subset.keys()
    cands = candidateset(knext,k)
    print("\npass:", k-1)
    print("num cands:", len(cands))
    #print(cands)
    prune(database, minimum_supp)
    print(subset)
    k += 1
    subset = dict()

print("final pass:", k-2)
for key, val in database.items():
    print(key)
stop = time.time()
print("runtime (seconds) : ", stop-start)
