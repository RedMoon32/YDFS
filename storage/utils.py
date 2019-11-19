import os
import logging
from logging.handlers import RotatingFileHandler


def create_log(app, node_name):
    if not os.path.exists('logs'):
        os.mkdir('logs')
    open(f'logs/{node_name}.log', 'w+')
    file_handler = RotatingFileHandler(f'logs/{node_name}.log', 'a', 1 * 1024 * 1024, 10)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    app.logger.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info(f'{node_name} startup')
