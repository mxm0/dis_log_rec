import uuid
import time
import logging
import hashlib
import threading

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
    pending_transactions = {}
    commited_transactions = []
    lsn = 0
    buffer_size = 0
    lock = threading.Lock()
    
    def __init__(self):
        self.logger = self.initialize_writer('transactions')
        self.writer = self.initialize_writer('commits')
 
    def begin_transaction(self):
        with Manager.lock:
            taid = uuid.uuid4()
            self.pending_transactions[taid] = {}
        return taid

    def write(self, taid, page_id, data):
        #lsn = hashlib.sha256(str(time.time()))
        # LSN is an increasing unique number for each write
        with Manager.lock:
            Manager.lsn += 1
            Manager.buffer_size += 1
        #lsn = hashlib.sha256(str([taid, page_id, data]))

            logging.debug('appending transaction for taid={taid}: '
                          '{pending_transactions}'.format(pending_transactions=self.pending_transactions,
                                                          taid=taid))

            if page_id not in self.pending_transactions[taid]:
                self.pending_transactions[taid][page_id] = []

            self.pending_transactions[taid][page_id].append({'lsn':self.lsn, 'data':data})
            self.logger.info('{lsn}, {taid}, {page_id}, {data}'.format(lsn=self.lsn,
                                                                       page_id=page_id,
                                                                       taid=taid,
                                                                       data=data))
            
            # If more then 5 datasets, in this case 'writes', are in the buffer then propagate to storage
            if Manager.buffer_size > 5:
                self.propagate()
        '''
        self.pending_transactions[taid][page_id].append({'lsn':lsn.hexdigest(), 'data':data})
        self.logger.info('{lsn}, {taid}, {page_id}, {data}'.format(lsn=lsn.hexdigest(),
                                                                   page_id=page_id,
                                                                   taid=taid,
                                                                   data=data))
        '''

    # Commit transaction in the buffer
    def commit(self, taid):
        self.commited_transactions.append(taid)
             
    # Propagate commited transaction in the physical storage
    def propagate(self):
        for taid in self.commited_transactions:
            for page_id in self.pending_transactions[taid]:
                data = self.pending_transactions[taid][page_id][-1]
                self.writer.info('{lsn}, {page_id}, {data}'.format(page_id=page_id, **data))
                Manager.buffer_size -= len(self.pending_transactions[taid][page_id])
            del self.pending_transactions[taid]
            logging.info('pending transactions after commit: {0}'.format(self.pending_transactions))
        del self.commited_transactions[:]


    @staticmethod
    def initialize_writer(file_name):
        logger = logging.getLogger(file_name)
        formatter = logging.Formatter('%(message)s')

        file_handler = logging.FileHandler(file_name, mode='w+')
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger
