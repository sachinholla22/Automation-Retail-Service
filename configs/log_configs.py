import logging

def setup_logs():
    logging.basicConfig(level=logging.info, format='%(levelname)s %(asctime)s %(message)s',file='./logs.log')