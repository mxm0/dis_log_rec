# Implement manager class with persitency, buffer, Singleton and all the good stuff
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# Methods needed beginTransaction(), commit(int taid), write(int taid, int pageid, String data)
class Manager(metaclass=Singleton):
    def __init__(self, val):
        # Initialize something if we need to

    def beginTransaction(self,):
        pass

    def write(self, int taid, int pageid, String data):
        pass
    
    def commit(self, int taid):
        pass



