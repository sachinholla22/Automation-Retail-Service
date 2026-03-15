import logging

def setup_logs():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s',filename='./logs.log')
