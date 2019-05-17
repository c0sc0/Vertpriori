
import time
import pandas
import IPython.display
from collections import Counter
from itertools import groupby, combinations

num_items = 0
num_transactions = 0
supp_percent = 0.001
minimum_supp = 0

pandas.set_option('display.width', 1000)
pandas.set_option('display.max_colwidth', 60)
pandas.set_option('display.max_rows', 40)
pandas.set_option('display.max_columns', 15)

#Generator function, generates k-sets
#(will work past k=2 but for our purposes, we'll stop there)
def generate_pairs(orders, k):
    orders = orders.reset_index().values #Treat series like an array
    #Group orders by the first element (i.e the order_id)
    for order_id, order in groupby(orders, lambda x: x[0]):
        items = [item[1] for item in order] #list of items in order_id

        for pair in combinations(items, k): #generate combinations up to k
            yield pair

#If using a pandas series, return a series of itemcounts. Otherwise we're using the generator, and we use Counter to
#count the generated k-sets and convert it to a series
def itemcount(iterable):
    if type(iterable) == pandas.core.series.Series:
        return iterable.value_counts().rename("count")
    else:
        return pandas.Series(Counter(iterable)).rename("count")

#Returns set of unique orders
def ordercount(orders):
    return len(set(orders.index))

#Using multilevel indexing, merge the two levels and split into item A and item B
def merge_stats(pairs, stats):
    return (pairs
                .merge(stats.rename(columns={'count': 'count(A)', 'support': 'support(A)'}), left_on='item_A', right_index=True)
                .merge(stats.rename(columns={'count': 'count(B)', 'support': 'support(B)'}), left_on='item_B', right_index=True))


# Rename item A / item B with the name corresponding to the item_id in products.csv
def merge_item_name(rules, item_name):
    #list of final column headers
    columns = ['item A','item B','count(AB)','support(AB)','count(A)','support(A)','count(B)','support(B)',
               'conf(A -> B)','conf(B -> A)','lift']
    rules = (rules
                .merge(item_name.rename(columns={'item_name': 'item A'}), left_on='item_A', right_on='item_id')
                .merge(item_name.rename(columns={'item_name': 'item B'}), left_on='item_B', right_on='item_id'))
    return rules[columns]

# Main rule generation function
def generate_rules(orders, minsupp):
    num_trans = ordercount(orders)
#get itemcounts in a dataframe, use counts to calculate support
    stats = itemcount(orders).to_frame("count")
    stats['support'] = stats['count'] / num_trans
#Prune items based on support
    supported_items = stats[stats['support'] >= minsupp].index
    orders = orders[orders.isin(supported_items)]

    print("Supported items >= {}: {:31d}".format(minsupp, len(supported_items)))
#Prune transactions based on size
    tran_size = itemcount(orders.index)
    supported_orders = tran_size[tran_size >= 2].index
    orders = orders[orders.index.isin(supported_orders)]

    print("Transactions remaining with 2+ items: {:18d}".format(len(supported_orders)))
#Generate pairs
    pair_generator = generate_pairs(orders, 2)
#Count their occurrence in a dataframe
    pairframe = itemcount(pair_generator).to_frame("count(AB)")
#Calculate the support
    pairframe['support(AB)'] = pairframe['count(AB)'] / num_trans

    print("Item pairs: {:44d}".format(len(pairframe)))

#Prune the pairs
    pairframe = pairframe[pairframe['support(AB)'] >= minsupp]
    print("Supported pairs >= {}: {:31d}\n\n".format(minsupp, len(pairframe)))
#Split and merge the pairs using multilevel indexing
    pairframe = pairframe.reset_index().rename(columns={'level_0' : 'item_A', 'level_1' : 'item_B'})
    pairframe = merge_stats(pairframe, stats)
#Calculate statistics
    pairframe['conf(A -> B)'] = pairframe['support(AB)'] / pairframe['support(A)']
    pairframe['conf(B -> A)'] = pairframe['support(AB)'] / pairframe['support(B)']

    pairframe['lift'] = pairframe['support(AB)'] / (pairframe['support(A)'] * pairframe['support(B)'])

    return pairframe.sort_values('lift', ascending=False)


start = time.time()

transactions = pandas.read_csv("orders.csv")
#Sort the input on 2 columns, this stops the creation of "flip-flopped" itemsets when they are generated
transactions.sort_values(['order_id', 'product_id'], ascending=[True, True], inplace=True)
print("Sorted input\n")
transactions = transactions.set_index('order_id')['product_id'].rename('item_id')

num_items = len(transactions.value_counts())
num_transactions = ordercount(transactions)
minimum_supp = int(supp_percent * num_transactions)

print("\nNumber of transactions: {:32d}".format(num_transactions))
print("Number of unique items: {:32d}".format(num_items))
print("Average transaction size: {:30d}".format(int(len(transactions) / num_transactions)))

print("\nSupport percentage: {:35.2f}%".format(supp_percent * 100))
print("Minimum support: {:39d}\n".format(minimum_supp))

rules = generate_rules(transactions, supp_percent)
#Load the DB of item names
names = pandas.read_csv("products.csv", encoding="utf8")
#Replace 'item a' / 'item b' with the name corresponding to its item_id
names = names.rename(columns={'product_id': 'item_id', 'product_name': 'item_name'})
finalrules = merge_item_name(rules, names).sort_values('lift', ascending=False)
IPython.display.display(finalrules)


stop = time.time()
print("\nruntime (seconds) : ", stop-start)
exit(0)
