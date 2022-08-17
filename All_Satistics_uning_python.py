import statistics
import collections
lists_from_csv=[]
import statistics
with open("trial_incomes.csv", 'r') as infile:
    i=0
    for line in infile:
        i+=1
        element = int(line.strip())
        lists_from_csv.append(element)
        # if i==1000000:
        #     break

# print(lists_from_csv)
print(len(set(lists_from_csv)))
print(statistics.median(sorted(lists_from_csv)))
# print(max(lists_from_csv))
print(statistics.mode(lists_from_csv))
temp=dict(collections.Counter(lists_from_csv))
print(max(temp, key=temp.get))