"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import sys
import json
import scipy.stats as stats


def solve(a_in, b_in):
    """
    input two num lists, indexed by the same location id order,
    print and return pearson correlation results
    :param a_in: list in json str, [1, 3, 53, 1, 7]
    :param b_in: list in json str, [4, 3, 2, 9, 6]
    :return: print pearson correlation, (corr, p-value)
    """
    a_list = json.loads(a_in)
    b_list = json.loads(b_in)
    res = stats.pearsonr(a_list, b_list)
    print(f'{res[0]} {res[1]}')


if __name__ == '__main__':
    std_input = [line for line in sys.stdin]
    solve(std_input[0], std_input[1])
