import http.client
import json
from coinbase.auth import Auth
import pandas as pd
from datetime import datetime, timedelta
import warnings

def send_request(method:str, path:str, payload:dict) -> dict:
    auth = Auth()
    headers = auth(method + path + payload)[0]
    # print(headers)
    conn = http.client.HTTPSConnection('api.coinbase.com')
    conn.request(method, path, payload, headers)
    return json.loads(conn.getresponse().read())

def list_accounts(json:bool=True) -> dict:
    """Returns a JSON object of all accounts and their balances from Coinbase Brokerage API"""
    method = 'GET'
    path = '/api/v3/brokerage/accounts'

    payload = ''
    data = send_request(method, path, payload)
    if json:
        return data
    else:
        return json.dumps(data, indent=2)

def list_orders(fill:bool=True) -> dict:
    """Returns a JSON object of all orders from Coinbase Brokerage API"""
    method = 'GET'
    if fill:
        path = '/api/v3/brokerage/orders/historical/fills'
    else:
        path = '/api/v3/brokerage/orders/historical/batch'

    payload = ''
    data = send_request(method, path, payload)
    return data

def get_order(orderID:str) -> dict:
    """Returns a JSON object from a specific order from Coinbase Brokerage API"""
    method = 'GET'
    path = f'/api/v3/brokerage/orders/historical/{orderID}'

    payload = ''
    data = send_request(method, path, payload)
    return data

def list_transactions() -> dict:
    """Returns a JSON object of all transactions from Coinbase Brokerage API"""
    method = 'GET'
    path = '/api/v3/brokerage/transaction_summary'

    payload = ''
    data = send_request(method, path, payload)
    return data

def get_product(productID:str='BTC-USD') -> dict:
    """Returns a JSON object of a product from Coinbase Brokerage API"""
    method = 'GET'
    path = '/api/v3/brokerage/products/' + productID

    payload = ''
    data = send_request(method, path, payload)
    return data

def cancel_order(all:bool=True, orderIDs:list=None, debug:bool=False) -> dict:
    """Cancels existing orders from Coinbase Brokerage API"""
    if all:
        orderIDs = list(pd.DataFrame(list_orders(fill=False)['orders']).query('status == "OPEN"').order_id.unique())
    if orderIDs is None:
        raise ValueError('Must specify orderIDs as a list type or set all=True')
    if debug:
        print(f'Cancelling {len(orderIDs)} orders')
    method = 'POST'
    path = '/api/v3/brokerage/orders/batch_cancel'
    payload = json.dumps({
    "order_ids": orderIDs
    })
    data = send_request(method, path, payload)
    return data

def quote_base(side:str, order_type:str, budget:float, quote_size:str, base_size:str) -> float:
    """Helper function to determine quote and base size"""
    if order_type.upper() == 'MARKET':
        if side.upper() == 'BUY':
            if quote_size == 'max':
                quote_size = budget
            else:
                quote_size = float(quote_size)
                quote_size = quote_size*budget
        else:
            if base_size == 'max':
                base_size = budget
            else:
                base_size = float(base_size)
                base_size = base_size*budget
    elif order_type.upper() == 'LIMIT' or order_type.upper() == 'STOP':
        if base_size == 'max':
            base_size = budget
        else:
            base_size = float(base_size)
            base_size = base_size*budget
    else:
        raise Exception('side must be BUY or SELL')
    return quote_size, base_size

def limit_order(base_size:str, limit_price:float, gtd:bool=False, end_time:str=None) -> dict:
    """Helper function. Creates a limit order configuration for the payload sent in the create_order function for Coinbase Brokerage API"""
    if gtd:
        if end_time is None:
            end_time = datetime.strftime(datetime.now() + timedelta(days=90), '%Y-%m-%dT%H:%M')
        else:
            try:
                datetime.strftime(datetime.strptime(end_time, '%Y-%m-%d'), '%Y-%m-%dT%H:%M')
            except:
                raise ValueError('end_time must be in format YYYY-MM-DD')
        return {"limit_limit_gtd":{"limit_price": str(limit_price), "post_only": True, "base_size": str(base_size), "end_time": end_time}}
    else:
        return {"limit_limit_gtc": {"limit_price": str(limit_price), "post_only": True, "base_size": str(base_size)}}

def market_order(quote_size:str, base_size:str) -> dict:
    """Helper function. Creates a market order configuration for the payload sent in the create_order function for Coinbase Brokerage API"""
    return {"market_market_ioc": {"quote_size": str(quote_size), "base_size": str(base_size)}}

def stop_order(base_size:str, limit_price:float, stop_price:float, gtc:bool=True) -> dict:
    """Helper function. Creates a stop order configuration for the payload sent in the create_order function for Coinbase Brokerage API"""
    if gtc:
        stop_order_type = 'gtc'
    else: 
        stop_order_type = 'gtd'
    return {f"stop_limit_stop_limit_{stop_order_type}": {"limit_price": str(limit_price), "base_size": str(base_size), "stop_price": str(stop_price)}}

def create_order(side:str, order_type:str='limit', productID:str='BTC-USD', quote_size:str='max', base_size:str='max', limit_price:float=None, stop_price:float=None, client_oid:str=None, gtd:bool=False, end_time:str=None, fee_rate:float=40, debug:bool=False) -> dict:
    """Creates an order from Coinbase Brokerage API"""
    if order_type.upper() == 'LIMIT' or order_type.upper() == 'STOP':
        if limit_price is None:
            raise ValueError('limit_price must be specified for limit or stop orders')
        if order_type.upper() == 'STOP':
            if stop_price is None:
                stop_price = limit_price-100
                warnings.warn(f'stop_price not specified. Using {stop_price}')
    data = pd.DataFrame.from_dict(list_accounts()['accounts'])
    base_currency = productID.split('-')[0]
    quote_currency = productID.split('-')[1]
    if side.upper() == 'BUY':
        if order_type.upper() == 'LIMIT' or order_type.upper() == 'STOP':
            budget = round(float(data.query(f'currency == "{quote_currency}"').available_balance.values[0]['value'])*(1-fee_rate/10000)/float(limit_price),8)
        else:
            budget = data.query(f'currency == "{quote_currency}"').available_balance.values[0]['value']
    elif side.upper() == 'SELL':
        budget = data.query(f'currency == "{base_currency}"').available_balance.values[0]['value']
    else:
        raise Exception('side must be BUY or SELL')
    quote_size, base_size = quote_base(side, order_type, budget, quote_size, base_size)

    if order_type.upper() == 'LIMIT':
        order_configuration = limit_order(base_size, limit_price, gtd, end_time)
    elif order_type.upper() == 'MARKET':
        order_configuration = market_order(quote_size, base_size)
    elif order_type.upper() == 'STOP':
        order_configuration = stop_order(base_size, limit_price, stop_price)
    else:
        raise Exception('order_type must be LIMIT, MARKET, or STOP')

    if client_oid is None:
        client_oid = 'coinbase_' + order_type + '_' + side.lower() + '_' + productID.lower() + '_' + str(limit_price)

    payload = json.dumps({
            "client_order_id": client_oid,
            "product_id": productID,
            "side": side.upper(),
            "order_configuration": order_configuration}, indent=2)
    method = 'POST'
    path = '/api/v3/brokerage/orders'
    if debug:
        print(payload)
    data = send_request(method, path, payload)
    return data