from coinbase.base import list_accounts, get_product
import pandas as pd

def get_accounts():
    """Returns a dataframe of all accounts and their balances from Coinbase Brokerage API"""
    data = list_accounts()
    df = pd.DataFrame.from_dict(data['accounts'])
    df[['available_balance', 'currency']] = pd.DataFrame(df.available_balance.tolist(), index=df.index)
    df[['hold', 'hold_currency']] = pd.DataFrame(df.hold.tolist(), index=df.index)
    pd.set_option("display.precision", 8)
    df['available_balance'] = df['available_balance'].astype(float)
    df['hold'] = df['hold'].astype(float)
    return df


def get_balance():
    """Returns a dataframe of all accounts and their USD denominated balances from Coinbase Brokerage API"""
    df = get_accounts()
    prod = get_product()
    exchange_rate = prod['price']
    currency = prod['product_id'].split('-')[0]
    pricedf = pd.DataFrame({'currency': currency, 'exchange_rate': exchange_rate}, index=[0])
    pricedf['exchange_rate'] = pricedf['exchange_rate'].astype(float)
    df = pd.merge(df, pricedf, on='currency', how='outer').fillna(1)
    df['usd_value'] = round((df.available_balance+df.hold)*df.exchange_rate,2)
    return df[['name', 'currency', 'available_balance', 'hold', 'exchange_rate', 'usd_value']]