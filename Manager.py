import uuid
import time
import logging
import hashlib
import threading
import os

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
        with Manager.lock:
            self.logger = self.initialize_writer_log('transactions')
        #self.writer = self.initialize_writer('data')

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
                self.__propagate()
        '''
        self.pending_transactions[taid][page_id].append({'lsn':lsn.hexdigest(), 'data':data})
        self.logger.info('{lsn}, {taid}, {page_id}, {data}'.format(lsn=lsn.hexdigest(),
                                                                   page_id=page_id,
                                                                   taid=taid,
                                                                   data=data))
        '''

    # Commit transaction in the buffer
    def commit(self, taid):
        with Manager.lock:
            Manager.lsn += 1
            self.logger.info('{lsn}, {taid}, {redo}'.format(lsn=self.lsn,
                                                            taid=taid,
                                                            redo=True))
            Manager.commited_transactions.append(taid)
             
    # Propagate commited transaction in the physical storage
    def __propagate(self):
        for taid in Manager.commited_transactions:
            for page_id in Manager.pending_transactions[taid]:
                writer = self.initialize_writer('data/' + str(page_id))
                data = Manager.pending_transactions[taid][page_id][-1]
                writer.info('{lsn}, {page_id}, {data}'.format(page_id=page_id, **data))
                Manager.buffer_size -= len(Manager.pending_transactions[taid][page_id])
            del Manager.pending_transactions[taid]
            logging.info('pending transactions after commit: {0}'.format(Manager.pending_transactions))
        del Manager.commited_transactions[:]
    
    # Recover state before crash
    def recover(self):
        # Load logfile into memory
        with open('transactions', 'r') as data_file:
            if os.stat('transactions').st_size == 0:
                return
            else:
                read_lines = data_file.readlines()
                transactions = [(line.rstrip('\n')).split(',') for line in read_lines]
                # Recover LSN
                Manager.lsn = int(transactions[-1][0])
        
        # Find winner transactions
        winners_ta = self.__find_ta_winners(transactions)
        # Read log life sequentially and redo needed transactions
        self.__redo_winners(transactions, winners_ta)

    def __find_ta_winners(self, transactions):
        winners_ta = []
        for TA in transactions:
            if len(TA) < 4:
                winners_ta.append(TA[1].strip()) 
        return winners_ta

    def __redo_winners(self, transactions, winners_ta):
        for TA in transactions:
            if len(TA) > 3:
                log_lsn = int(TA[0])
                taid = TA[1].strip()
                page_id = TA[2].strip()
                data = TA[3].strip()
                if os.path.exists(page_id):
                    # Find LSN of page
                    page_lsn = self.__load_page_lsn(page_id)  
                else:
                    page_lsn = 0
                # Find write to redo
                if taid in winners_ta and log_lsn  > page_lsn:
                    with open('data/' + page_id, 'w') as fp:
                        record = '{lsn}, {page_id}, {data}'.format(lsn = log_lsn, page_id = page_id, data = data)
                        record += '\n'
                        fp.write(record)

    def __load_page_lsn(self, pageid):
        page_lsn = 0
        with open('data/' + pageid, 'r') as data_file:
            read_lines = data_file.readlines()
        return int(read_lines[0].rstrip('\n').split(',')[0])

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

    @staticmethod
    def initialize_writer_log(file_name):
        logger = logging.getLogger(file_name)
        formatter = logging.Formatter('%(message)s')

        file_handler = logging.FileHandler(file_name, mode='a+')
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger
