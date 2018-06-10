# Client class implementation
# The Client repeatedly execute transactions on the manager

from Manager import Manager
import time, random, string

class Client:
  def __init__(self, client_n, pageids):
      self.client_n = client_n
      self.pageids = pageids
  
  def execute(self):
      while(True):
        manager = Manager()
        taid = manager.beginTransaction()
        print("Client", self.client_n, "started new transaction number:.", taid)

        # Build random string as data and write it
        for pageid in self.pageids:
            data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
            manager.write(taid, pageid, data)

        # Commit transaction
        manager.commit(taid)
        print("Client", self.client_n, "commited transaction number:", taid)
        time.sleep(3)
