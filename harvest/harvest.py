import argparse
import logging
from utils.control import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Harvest')
logger.setLevel(logging.DEBUG)


def reg(ip):
    registry = Registry(ip)
    registry.run()


def worker(ip):
    worker = Worker(ip)
    worker.run()


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", help="specify the role in [reg,]")
    parser.add_argument("ip", help="IP address that used to communicate with other roles.")
    args = parser.parse_args()
    if args.role and args.ip:
        if args.role == 'reg':
            reg(args.ip)
        elif args.role == 'worker':
            worker(args.ip)


if __name__ == '__main__':
    parse()
