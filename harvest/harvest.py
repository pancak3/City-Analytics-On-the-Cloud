import argparse
import logging
from utils.registry import Registry
from utils.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Harvest')
logger.setLevel(logging.DEBUG)


def reg(ip):
    registry = Registry(ip)
    registry.run()


def worker():
    worker = Worker()
    worker.run()


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", help="specify the role in [reg,]")
    parser.add_argument("ip", nargs='?', help="IP address that Registry used to communicate with workers.")
    args = parser.parse_args()
    if args.role:
        if args.role == 'reg' and args.ip:
            reg(args.ip)
        elif args.role == 'worker':
            worker()


if __name__ == '__main__':
    parse()
