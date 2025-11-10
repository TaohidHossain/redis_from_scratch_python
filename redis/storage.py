import time
import fnmatch

class DataStore:
    def __init__(self):
        # storage formate: { key : (value, type, expiry_time)}
        self._data = {}
        self._memory_usage = 0
    
    def set(self, key, value, expiry_time=None):
        if key in self._data:
            old_value, _, _ = self.data[key]
            self._memory_usage -= self._calculate_memory_usage(key, old_value)
        
        data_type = self._get_data_type(value)
        self._data[key] = (value, data_type, expiry_time)
        self._memory_usage += self._calculate_memory_usage(key, value)
    
    def get(self, key):
        if not self._is_valid_key(key):
            return None
        value, _, _ = self._data[key]
        return value
        
    def delete(self, *keys):
        count = 0
        for key in keys:
            if key in self._data:
                value, _, _ = self._data[key]
                self._memory_usage -= self._calculate_memory_usage(key, value)
                del self._data[key]
                count += 1
        return count
    
    def exists(self, *keys):
        return sum(1 for key in keys if self._is_valid_key(key))
    
    def keys(self, pattern="*"):
        valid_keys = [key for key in self._data.keys() if self._is_valid_key(key)]
        if pattern == "*":
            return valid_keys
        return [key for key in valid_keys if fnmatch.fnmatch(key, pattern)]
    
    def flush(self):
        self._data.clear()
        self._memory_usage = 0

    def _is_valid_key(self, key):
        if key not in self._data:
            return False
        
        value, _, expiry_time = self._data[key]
        if expiry_time is not None and expiry_time <= time.time():
            self._memory_usage -= self._calculate_memory_usage(key, value)
            del self._data[key]
            return False
        return True
    
    def _calculate_memory_usage(self, key, value):
        key_size = len(str(key).encode("utf-8"))
        value_size = len(str(value).encode("utf-8"))
        return key_size + value_size + 64 # Add overhead for metadata
    
    def _get_data_type(self, value):
        if isinstance(value, str):
            return "string"
        elif isinstance(value, int):
            return "string"
        elif isinstance(value, list):
            return "list"
        elif isinstance(value, set):
            return "set"
        elif isinstance(value, dict):
            return "hash"
        else:
            return "string"