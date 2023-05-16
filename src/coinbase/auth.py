import time, hmac, hashlib
import configparser
from pathlib import Path
import os

global p
p = Path(os.getenv('lc_secrets'))/'coinbase.ini'
if not p.exists():
    raise Exception('Environment variable lc_secrets not set to a valid path. No API keys can be read.')

class Auth:
    def __init__(self):
        self.read_config()
    
    def __call__(self, message):
        timestamp = str(int(time.time()))
        signature = hmac.new(self.SECRET.encode('utf-8'), (timestamp + message).encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        headers = {
            'CB-ACCESS-KEY': self.API_KEY,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-SIGN': signature,
            'Content-Type': 'application/json'
        }
        return headers, signature, self.API_KEY, timestamp
    
    def read_config(self, debug:bool=False, config_path:str=None):
        """Helper function to read coinbase.ini file which has API secrets"""
        config = configparser.ConfigParser()
        if config_path is None:
            config_path = p
        try:
            if debug:
                print(f'Reading config file in {config_path}')
            config.read(config_path)
        except:
            raise Exception(f'coinbase.ini file not found in {config_path} or not readable')
        # base configs for any connection are server, database, and driver
        self.API_KEY = config['COINBASE_API']['api_key']
        self.SECRET = config['COINBASE_API']['secret']