import uuid
import logging
import hashlib


# Implement manager class with persitency, buffer, Singleton and all the good stuff
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


# Methods needed beginTransaction(), commit(int taid), write(int taid, int pageid, String data)
class Manager(object):
    __metaclass__ = Singleton

    def __init__(self):
        self.transactions = self.transaction_logger('transactions')
        self.commits = self.transaction_logger('commits')

    def beginTransaction(self):
        return uuid.uuid4()

    def write(self, taid, page_id, data):
        LSN = hashlib.sha256(str([taid, page_id, data]))
        LSN = LSN.hexdigest()
        self.transactions.info('{LSN}, {taid}, {page_id}, {data}'.format(LSN=LSN, taid=taid,
                                                                         page_id=page_id, data=data))

    def commit(self, taid):
        # read/recover  commit
        self.commits.info('{taid}'.format(taid=taid))

    def transaction_logger(self, file_name):
        l = logging.getLogger(file_name)

        fileHandler = logging.FileHandler(file_name, mode='w+')
        formatter = logging.Formatter('%(message)s')
        streamHandler = logging.StreamHandler()

        streamHandler.setFormatter(formatter)
        fileHandler.setFormatter(formatter)

        l.setLevel(logging.DEBUG)
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
        return l
