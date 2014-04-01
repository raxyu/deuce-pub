
import pymongo
from pymongo import MongoClient as mongoclient


def MongoClient(url):  # pragma: no cover
    return mongoclient(url)
