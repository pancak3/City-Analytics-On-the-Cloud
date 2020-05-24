import sys
import json
import pandas as pd


def to_df(json_dict):
    keys, nums = [], []
    for k, v in json_dict.items():
        keys.append(k)
        nums.append(v)
    return pd.DataFrame(index=keys, columns=['values'], data=[nums]).sort_index()


def get_pearson_corr(a, b):
    pearson_corr = pd.DataFrame([a, b]).T.corr(method='pearson')
    return pearson_corr.iloc[0, 1]


def solve(a_in, b_in):
    """
    output to stdout of two indexed nums, need to be with exactly the same ids
    but the order of ids does not need to be the same
    :param a_in: dict in json str, {"1":0.6, "68":0.3, ... ... , "2":45}
    :param b_in: dict in json str, {"1":12, "2":8, ... ... , "68":0.5}
    :return: print pearson correlation, 0.04
    """
    a_json = json.loads(a_in)
    b_json = json.loads(b_in)
    print(get_pearson_corr(to_df(a_json)['values'], to_df(b_json)['values']))


if __name__ == '__main__':
    std_input = [line for line in sys.stdin]
    solve(std_input[0], std_input[1])
