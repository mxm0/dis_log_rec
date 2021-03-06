# App application that start multiple clients in parallel on different threads and concurrently access the manager

import uuid
import random
import threading
from Client import Client
from Manager import Manager
import os

n_pages = 2
n_workers = 3

def worker():
    pages = [str(uuid.uuid4()).split('-')[0] for _ in range(random.randint(1, n_pages))]
    c = Client(page_ids=pages)
    c.execute()


if __name__ == '__main__':
    
    # Recover state
    #if os.path.exists('transactions'):
    manager = Manager()
    manager.recover()
   
    '''
    pool = [threading.Thread(target=worker) for _ in range(n_workers)]
    map(lambda t: t.start(), pool)
    '''
