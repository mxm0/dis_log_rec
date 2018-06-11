# Client class implementation
# The Client repeatedly execute transactions on the manager

import sys
import time
import random
import string
import logging
import itertools

from Manager import Manager

class Client:
    def __init__(self, page_ids, logfile='Client.log'):
        logging.basicConfig(filename=logfile,
                            filemode='w+',
                            level=logging.DEBUG,
                            stream=sys.stdout,
                            format='%(relativeCreated)6d %(threadName)s %(message)s')

        writes = {page_id: random.randint(1, 5) for page_id in page_ids}
        writes = [page_id for page_id, n in writes.items() for _ in itertools.repeat(page_id, n)]
        random.shuffle(writes)

        logging.debug('page write order {0}'.format(writes))
        self.writes = writes

    def execute(self):
        manager = Manager()
        taid = manager.beginTransaction()

        for page_id in self.writes:
            manager.write(taid, page_id, self.data(10))
            time.sleep(3)

        manager.commit(taid)

    def data(self, bits, CHAR_SET = string.ascii_uppercase):
        return ''.join(random.choice(CHAR_SET) for _ in range(bits))
