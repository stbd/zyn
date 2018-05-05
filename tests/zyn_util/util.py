import logging


def verbose_count_to_log_level(verbose_count):
    logger = logging.getLogger()
    level = logging.WARNING
    if verbose_count == 1:
        level = logging.INFO
    elif verbose_count == 2:
        level = logging.DEBUG
    else:
        logger.warn('Maximum number of verbose flags is 2, greater value is ignored')
    logger.setLevel(level)
