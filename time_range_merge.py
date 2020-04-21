import random
import functools

def generate_time_range_pairs():
    pairs = []
    for i in range(10):
        start = random.uniform(0, 30)
        duration = random.uniform(0, 10)
        end = start + duration
        pairs.append((start, end))
    return pairs

s = generate_time_range_pairs()
print(s)

def merge(exist_list, pair):
    new_list = []
    for exist_pair in exist_list:
        if pair[0] > exist_pair[1] or pair[1] < exist_pair[0]:
            new_list.append(exist_pair)
            print("skip:", pair, exist_pair)
            continue
        print("merging:", pair, exist_pair)
        pair = (min(pair[0], exist_pair[0]), max(pair[1], exist_pair[1]))
        print("merged:", pair)

    new_list.append(pair)
    return new_list


merged = functools.reduce(merge, s, [] )

print(merged)