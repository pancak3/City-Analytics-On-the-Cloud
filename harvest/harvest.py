import argparse
import logging
from utils.registry import Registry
from utils.worker import Worker
from utils.logger import get_logger

logger = get_logger('Harvest', logging.DEBUG)


def reg(ip, log_level):
    """
    create a registry(master) and run it
    :param ip:
    :param log_level: log level to filter logs
    :return:
    """
    registry = Registry(ip, log_level)
    registry.run()


def worker(log_level):
    """
    create a worker and run it
    :param log_level: log level to filter logs
    :return:
    """
    worker = Worker(log_level)
    worker.run()


def parse_log_level(log_level):
    """
    parse log level to logging level names
    :param log_level:
    :return:
    """
    options_map = {
        'critical': logging.CRITICAL,
        'fatal': logging.FATAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'warn': logging.WARN,
        'info': logging.INFO,
        'debug': logging.DEBUG,
        'notset': logging.NOTSET
    }
    return options_map[log_level] if log_level in options_map else logging.INFO


def parse():
    """
    Parse the input args and run as the specific role
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--role", help="specify the role in [reg,]")
    parser.add_argument("-i", "--ip", nargs='?', help="IP address that Registry used to communicate with workers.")
    parser.add_argument("-l", "--log", nargs='?', default='debug',
                        help="output log level  [critical, fatal, error, warning, warn ,info, debug, notset]")
    args = parser.parse_args()
    args.log = parse_log_level(args.log)
    if args.role:
        if args.role == 'reg' and args.ip:
            reg(args.ip, args.log)
        elif args.role == 'worker':
            worker(args.log)
        else:
            parser.print_help()
    else:
        parser.print_help()


if __name__ == '__main__':
    parse()
