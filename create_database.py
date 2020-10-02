import pymongo
import logging
from utils.log import init_logger


def create_db(db_name):
    logger = init_logger(__name__, testing_mode=False)
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        logger.info('Database created.')  # will not print anything
    except:
        db = None
        logger.error("Can't connect to MongoDb, please check your connection")

    return db


create_db("test")