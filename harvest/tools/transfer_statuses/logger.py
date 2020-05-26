'''
comp90024 team 42
Qifan Deng
1077479
Zijie Pan
1059454
Mandeep Singh
991857
Steven Tang
832031
26/05/2020
'''
import logging


# https://www.programcreek.com/python/example/192/logging.Formatter
# https://stackoverflow.com/questions/533048
def get_logger(logger_name, level_name=logging.DEBUG, create_file=False, log_file_name='harvest.log'):
    # create logger for prd_ci
    log = logging.getLogger(logger_name)
    log.setLevel(level_name)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        fmt='[%(asctime)s,%(msecs)d][%(filename)s:%(lineno)d][%(name)s][%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    if create_file:
        # create file handler for logger.
        fh = logging.FileHandler(log_file_name)
        fh.setLevel(level=level_name)
        fh.setFormatter(formatter)
        # add handlers to logger.
        log.addHandler(fh)
    # reate console handler for logger.
    ch = logging.StreamHandler()
    ch.setLevel(level=level_name)
    ch.setFormatter(formatter)
    # while log.handlers:
    #     log.handlers.pop()
    log.addHandler(ch)
    return log
