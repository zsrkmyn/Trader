#!/bin/env python3

from okx.PublicData import PublicAPI
from okx.MarketData import MarketAPI
import time

flag = '0'

if flag == '1':
  WSPubUrl = 'wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999'
  WSPrvUrl = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'
else:
  WSPubUrl = 'wss://wsaws.okx.com:8443/ws/v5/public'
  WSPrvUrl = 'wss://wsaws.okx.com:8443/ws/v5/private'

proxy = None

pub = PublicAPI(proxy=proxy, flag=flag,debug=False)
market = MarketAPI(proxy=proxy, flag=flag, debug=False)


def get_futures(family):
  msg = pub.get_instruments('FUTURES', instFamily=family)
  if (code:= msg['code']) != '0':
    raise Exception(
      'failed to get contract info; code %s, msg: %s' % (code, msg['msg']))
  return [(d['instId'], d['settleCcy'], int(d['expTime'])//1000) for d in msg['data']]


def get_prices(inst_type, inst_family, is_ask=False):
  msg = market.get_tickers(inst_type, instFamily=inst_family)
  if (code:= msg['code']) != '0':
    raise Exception(
      'failed to get contract info; code %s, msg: %s' % (code, msg['msg']))
  px_key = 'askPx' if is_ask else 'bidPx'
  return {d['instId'] : float(px) for d in msg['data'] if (px:=d[px_key])}


families = ('BTC-USD', 'ETH-USD', 'XRP-USD')

print('%-16s %-10s %-8s %7s %7s' % ('Future', 'Spot', 'Delivery', 'Spread', 'APY'))

spot_pxs = get_prices('SPOT', '', True)
for family in families:
  future_pxs = get_prices('FUTURES', family, False)
  for futrue_id, settle, exp_time in get_futures(family):
    spot_id = settle + '-USDC'
    now = int(time.time())
    day_left = (exp_time - now) // (60 * 60 * 24)
    future_px = future_pxs[futrue_id]
    spot_px = spot_pxs[spot_id]
    spread_p = (future_px - spot_px) / spot_px * 100
    apy = spread_p / day_left * 365
    # print('%16s %10s %8d %8a %8a %6.2f%% %6.2f%%' % (futrue_id, spot_id, day_left, future_px, spot_px, spread_p, apy))
    print('%-16s %-10s %-8d %6.2f%% %6.2f%%' % (futrue_id, spot_id, day_left, spread_p, apy))
