class MContext:
    """Mapper context"""

    data = {}

    def emit(self, key, value):
        """emits a key value to the context"""
        key_s = str(key)
        if key_s not in self.data:
            self.data[key_s] = []
        self.data[key_s].append(value)

    def keys(self):
        """returns a copy of the key set"""
        tmpkeys =[]
        tmpkeys.extend(self.data.keys())
        return tmpkeys

    def get(self, key):
        """returns the value for the key"""
        return self.data[str(key)]
class RContext:
    """Reducer context"""
    
    tuples = []

    def emit(self, key, value):
        """emits a key value to the context"""
        self.tuples.append((key,value))

    def data(self):
        """returns the key/value tuples"""
        return self.tuples
