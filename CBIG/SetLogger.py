
import logging

class Logs:

    def setup_logger(self, logger_name, log_file, level=logging.DEBUG):
        # logging.basicConfig(logger_name,format='[%(asctime)s] [%(filename)s:%(lineno)s - %(funcName)20s()] %(levelname)s - %(message)s',datefmt='%m-%d %H:%M:%S',filemode='a',level="INFO")
        # Create logger object
        logger = logging.getLogger(logger_name)

        # format the log output
        formatter = logging.Formatter(
            '[%(asctime)s] [%(filename)s:%(lineno)s - %(funcName)20s()] %(levelname)s - %(message)s',
            datefmt='%m-%d %H:%M:%S')

        # Create file handler
        fileHandler = logging.FileHandler(log_file, mode='a')
        streamHandler = logging.StreamHandler()
        # Set the Formatter
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)

        logger.setLevel(level)
        logger.addHandler(fileHandler)
        logger.addHandler(streamHandler)

        return logger