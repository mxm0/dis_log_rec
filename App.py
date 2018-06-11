# App application that start multiple clients in parallel on different threads and concurrently access the manager

import uuid
import random
import threading
from Client import Client

n_pages = 3
n_workers = 5


def worker(i):
    pages = [str(uuid.uuid4()).split('-')[0] for _ in range(random.randint(1, n_pages))]
    c = Client(page_ids=pages)
    c.execute()


if __name__ == '__main__':
    pool = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    map(lambda t: t.start(), pool)

