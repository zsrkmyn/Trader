#!/bin/env python3

flag = '1'

import asyncio
import colorlog
import json
import logging
import time
import traceback

from okx.Account import AccountAPI
from okx.PublicData import PublicAPI
from okx.okxclient import OkxClient
from okx.websocket.WsPublicAsync import WsPublicAsync
from okx.websocket.WsPrivateAsync import WsPrivateAsync


if flag == '1':
  from config_sim import *
else:
  from config import *


if flag == '1':
  WSPubUrl = 'wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999'
  WSPrvUrl = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'
else:
  WSPubUrl = 'wss://wsaws.okx.com:8443/ws/v5/public'
  WSPrvUrl = 'wss://wsaws.okx.com:8443/ws/v5/private'


j = json.JSONEncoder(separators=(',', ':'))

class TraderError(Exception):
  pass


class LogginFailedError(TraderError):
  pass


class ExtWsPrivateAsync(WsPrivateAsync):
  def __init__(self, /, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.order_id = 0

  async def place_order(self, *, instId, tdMode, side, ordType, sz, **kwargs):
    kwargs.update(
      {'instId': instId, 'tdMode': tdMode, 'side': side, 'ordType': ordType,
       'sz': sz})
    payload = j.encode(
      {'op': 'order', 'id': str(self.order_id), 'args': [kwargs]})
    logger.debug('prv msg sent: %s' % payload)
    self.order_id += 1
    await self.websocket.send(payload)



class Trader:
  MODE_OPEN = 0
  MODE_CLOSE = 1

  STATE_INIT = 0
  STATE_MONITORING = 1
  STATE_TRADING = 2

  ORDER_STATE_DONE = 0
  ORDER_STATE_PLACING = 1
  ORDER_STATE_PLACED = 2
  ORDER_STATE_LIVE = 3
  ORDER_STATE_PARTIAL = 4

  order_state_map = {
    'live': ORDER_STATE_LIVE,
    'partially_filled': ORDER_STATE_PARTIAL,
    'filled': ORDER_STATE_DONE
  }


  def __init__(self, loop, left, right, mode, exp_spread, total_amount, single_amount,
               lag_threshold=500, http_proxy=None):
    self.loop = loop
    self.http_proxy = http_proxy
    self.left = left
    self.right = right
    self.mode = mode
    self.exp_spread = exp_spread
    self.total_amount = total_amount
    self.single_amount = single_amount
    self.trading_amount = None
    self.fee_rate = 0.0015 # FIXME
    self.left_px = 1.
    self.right_px = 1.
    self.state = self.STATE_INIT
    self.lstate = self.ORDER_STATE_DONE
    self.rstate = self.ORDER_STATE_DONE
    l_px_key= 'bidPx'
    r_px_key = 'askPx'
    if mode == self.MODE_CLOSE:
      r_px_key, l_px_key = l_px_key, r_px_key
    self.r_px_key = r_px_key
    self.l_px_key = l_px_key
    self.inst_subscribled = 0
    self.order_subscribled = 0
    self.spread_satisfied = 0
    self.logged_in = False
    self.pub_channel = None
    self.prv_channel = None
    self.lag_threshold = lag_threshold
    self.lag = 0
    self.order_id = 0
    self.l_order_ids = []
    self.r_order_ids = []
    self.right_remaining = 0.
    self.contract_px = self.get_contract_px(left, proxy=http_proxy)
    logger.info('contract price is set to %.2f', self.contract_px)

  @staticmethod
  def get_contract_px(contract_id, proxy=None):
    pub = PublicAPI(proxy=proxy, debug=False)
    msg = pub.get_instruments("FUTURES", instId=contract_id)
    if (code:= msg['code']) != '0':
      raise TraderError(
        'failed to get contract price; code %s, msg: %s' % (code, msg['msg']))
    return float(msg['data'][0]['ctVal'])


  def try_to_run(self):
    if self.state == self.STATE_INIT:
      if self.inst_subscribled == 2 and \
         self.order_subscribled == 2 and \
         self.logged_in == True:
        logger.info('state changed: INIT -> MONITORING')
        self.state = self.STATE_MONITORING
        self.loop.create_task(self.start_monitoring())


  def handle_pub_sub_ev(self, msg):
    inst = msg['arg']['instId']
    if inst == self.left:
      ticker = 'left'
    elif inst == self.right:
      ticker = 'right'
    else:
      logger.warning('Unknown subscrible event: %s', msg)
      return
    logger.info('%s ticker subscribed', ticker)
    self.inst_subscribled += 1
    self.try_to_run()


  def handle_prv_sub_ev(self, msg):
    inst = msg['arg']['instId']
    if inst == self.left:
      ticker = 'left'
    elif inst == self.right:
      ticker = 'right'
    else:
      logger.warning('Unknown subscrible event: %s', msg)
      return
    logger.info('%s order subscribed', ticker)
    self.order_subscribled += 1
    self.try_to_run()


  def check_spread(self, spread):
    logger.info('spread satisfied: %d', self.spread_satisfied)
    if self.spread_satisfied < 3:
      return False
    return True


  def check_lag(self):
    if self.lag > self.lag_threshold:
      logger.warning('lag is too high; trade postponed')
      return False
    return True


  def place_right_order(self, side, size):
    self.rstate = self.ORDER_STATE_PLACING
    self.loop.create_task(self.place_right_order_async(side, size))


  async def place_right_order_async(self, side, size):
    o_id = self.order_id
    self.order_id += 1
    self.r_order_ids.append(o_id)
    logger.info(
      'place right order: %s %s, size: %f, id: %d' %
      (side, self.right, size, o_id))
    await self.prv_channel.place_order(
      instId=self.right,
      tdMode='cash',        # FIXME
      side=side,
      ordType='market',     # FIXME
      sz=size,
      tgtCcy='base_ccy',
      clOrdId=str(o_id))


  def place_left_order(self, side, sz):
    self.lstate = self.ORDER_STATE_PLACING
    self.loop.create_task(self.place_left_order_async(side, sz))


  async def place_left_order_async(self, side, sz):
    o_id = self.order_id
    self.order_id += 1
    self.l_order_ids.append(o_id)
    logger.info(
      'place left order: %s %s, size: %d, id: %d' % (side ,self.left, sz, o_id))
    await self.prv_channel.place_order(
      instId=self.left,
      tdMode='isolated',    # FIXME
      side=side,
      ordType='market',     # FIXME
      sz=sz,
      clOrdId=str(o_id))


  def try_to_open_position(self, spread):
    if not self.check_lag():
      return False
    if spread <= self.exp_spread:
      self.spread_satisfied = 0
      return False
    self.spread_satisfied += 1
    if not self.check_spread(spread):
      return False
    # Floor trading_amount (in USDT) to multiple of a single contract price.
    # The contract price is corrected to the actual USDT we need to pay to
    # short a future contract.
    trading_amount = min(self.single_amount, self.total_amount)
    actual_contract_px = self.contract_px / self.left_px * self.right_px
    contract_num = int(trading_amount / actual_contract_px)
    if contract_num == 0:
      return False
    self.trading_amount = trading_amount = contract_num * actual_contract_px
    size = trading_amount / self.right_px * (1. + self.fee_rate)
    # size also equals,
    #   contract_num * self.contract_px / self.left_px * (1. + self.fee_rate)
    self.place_right_order('buy', size)
    return True


  def try_to_close_position(self, spread):
    if not self.check_lag():
      return False
    if spread >= self.exp_spread:
      self.spread_satisfied = 0
      return False
    self.spread_satisfied += 1
    if not self.check_spread(spread):
      return False
    self.trading_amount = min(self.single_amount, self.total_amount)
    self.place_left_order('buy', self.trading_amount)
    return True


  def check_spread_and_trade(self):
    spread = (self.left_px - self.right_px) / self.right_px * 100
    logger.info('left_px: %f, right_px %f, spread: %.2f%%, lag: %dms' %
                (self.left_px, self.right_px, spread, self.lag))

    if self.state != self.STATE_MONITORING:
      return

    if self.mode == self.MODE_OPEN:
      trade_started =  self.try_to_open_position(spread)
    else:
      trade_started = self.try_to_close_position(spread)

    if trade_started:
      self.spread_satisfied = 0
      logger.info('state: MONITORING -> TRADING')
      self.state = self.STATE_TRADING


  async def start_monitoring(self):
    logger.info("monitor started; unexecuted amount: %f", self.total_amount)
    if self.mode == self.MODE_OPEN:
      min_unit = self.contract_px
    else:
      min_unit = 1
    while self.total_amount >= min_unit:
      self.check_spread_and_trade()
      await asyncio.sleep(1)
    await self.stop_self_and_loop()

  @staticmethod
  def log_warn_unknown_msg(channel, msg):
    logger.warning('unknown msg recieved in %s channel: %s', channel, msg)


  def handle_ticker_msg(self, msg):
    data = msg['data'][0]
    inst = data['instId']
    server_ts = int(data['ts'])
    self.lag = int(time.time() * 1000) - self.time_diff - server_ts
    if inst == self.left:
      self.left_px = float(data[self.l_px_key])
    elif inst == self.right:
      self.right_px = float(data[self.r_px_key])


  def handle_pub_msg(self, raw_msg):
    logger.debug('pub msg recved: %s', raw_msg)
    msg = json.loads(raw_msg)
    ev = msg.get('event')
    if ev == 'subscribe':
      return self.handle_pub_sub_ev(msg)

    if ev == 'error':
      # FIXME
      logger.error(msg)
      return

    if ev is not None:
      logger.warning('unknown event: %s', msg)
      return

    arg = msg.get('arg')
    if arg is None:
      self.log_warn_unknown_msg('pub', raw_msg)
      return

    channel = arg.get('channel')
    if channel == 'tickers':
      self.handle_ticker_msg(msg)
      return

    self.log_warn_unknown_msg('pub/' + str(channel), raw_msg)


  def handle_loggin_ev(self, msg):
    code = msg['code']
    if msg['code'] == '0':
      self.logged_in = True
    else:
      raise LogginFailedError(
        f'login failed with code: {code}, msg: {msg["msg"]}')


  def handle_order_op(self, msg):
    code = int(msg.get('code', 0))
    if code != 0:
      raise TraderError('order error occured, msg: %s' % msg)
    data = msg['data'][0]
    code = int(data['sCode'])
    if code != 0:
      raise TraderError('order error occured, msg: %s' % msg)

    o_id = data['clOrdId']
    try:
      o_id = int(o_id)
    except ValueError:
      pass
    if o_id in self.r_order_ids:
      logger.info('right order placed, id: %s' % o_id)
      self.rstate = self.ORDER_STATE_PLACED
    elif o_id in self.l_order_ids:
      logger.info('left order placed, id: %s' % o_id)
      self.lstate = self.ORDER_STATE_PLACED
    else:
      logger.info('unknown order placed, id: %s; ignore it', o_id)


  @staticmethod
  def log_warn_unknown_execed_order(o_id):
    logger.warning(f'unknown executed order recieved, id: {o_id}; ignore it')


  @staticmethod
  def check_oid(id_list, o_id, do_delete):
    try:
      idx = id_list.index(o_id)
    except ValueError:
      return False
    if do_delete:
      del id_list[idx]
    return True


  def handle_order_msg(self, msg):
    for data in msg['data']:
      o_id = data['clOrdId']
      try:
        o_id = int(o_id)
      except ValueError:
        self.log_warn_unknown_execed_order(o_id)
        continue

      state_str = data['state']
      state = self.order_state_map.get(data['state'])
      fill_sz = data['fillSz']
      if self.check_oid(self.r_order_ids, o_id, state==self.ORDER_STATE_DONE):
        self.rstate = state
        logger.info(
          'right order executed, fill_sz: %s, state: %s' % (fill_sz, state_str))
        if self.mode == self.MODE_OPEN:
          if fill_sz == '0':
            continue
          right_remaining = float(fill_sz) + self.right_remaining
          contract_px = self.contract_px
          # floor the future number
          size = right_remaining * self.left_px / contract_px
          size = int(size + 0.1) # add 0.1 to ignore slipage
          logger.info('unblanced right holdings: %f' % right_remaining)

          # FIXME: correct right_remaining by actual trading price
          self.right_remaining = \
              right_remaining - size * contract_px / self.left_px
          if size > 0:
            self.place_left_order('sell', size)

      elif self.check_oid(self.l_order_ids, o_id, state==self.ORDER_STATE_DONE):
        self.lstate = state
        logger.info(
          'left order executed, fill_sz: %s, state: %s' % (fill_sz, state_str))
        if self.mode == self.MODE_CLOSE:
          if fill_sz == '0':
            continue
          fill_notional_usd = float(data['fillNotionalUsd'])
          fill_px = float(data['fillPx'])
          fill_fee = float(data['fee'])
          size = fill_notional_usd / fill_px + fill_fee
          self.place_right_order('sell', size)
      else:
        self.log_warn_unknown_execed_order(o_id)

    if self.state == self.STATE_TRADING and \
       self.lstate == self.ORDER_STATE_DONE and \
       self.rstate == self.ORDER_STATE_DONE:
      if self.right_remaining > 0:
        logger.warning(
          'unblanced right holdings after position opened: %f' % self.right_remaining)
      self.right_remaining = 0.
      logger.info('state: TRADING -> MONITORING')
      self.state = self.STATE_MONITORING
      self.total_amount -= self.trading_amount


  def handle_prv_msg(self, raw_msg):
    logger.debug('prv msg recved: %s', raw_msg)
    msg = json.loads(raw_msg)
    ev = msg.get('event')
    if ev == 'login':
      return self.handle_loggin_ev(msg)

    if ev == 'subscribe':
      return self.handle_prv_sub_ev(msg)

    if ev == 'error':
      code = int(msg['code'])
      if code == 60024:
        raise LogginFailedError(
          f'login failed with code: {code}, msg: {msg["msg"]}')
      raise TraderError('unhandled error occured, msg: %s' % msg)
      return

    if ev is not None:
      logger.warning('unknown event: %s', msg)
      return

    if msg.get('op') == 'order':
      return self.handle_order_op(msg)

    arg = msg.get('arg')
    if arg is None:
      self.log_warn_unknown_msg('prv', raw_msg)
      return

    channel = arg.get('channel')
    if channel == 'orders':
      return self.handle_order_msg(msg)

    self.log_warn_unknown_msg('prv/' + str(channel), raw_msg)


  def calibrate_local_time(self):
    client = OkxClient(proxy=self.http_proxy)
    lag = []
    for i in range(3):
      t0 = int(time.time() * 1000)
      server_time = int(client._get_timestamp())
      t1 = int(time.time() * 1000)
      lag.append((t1 - t0) / 2)
    lag = round(sum(lag) / len(lag))
    self.time_diff = t1 - (server_time + lag)
    logger.info('local time calibrated, time difference: %d' % self.time_diff)


  async def start(self):
    self.calibrate_local_time()
    prv_channel = ExtWsPrivateAsync(
      api_key, passphrase, secret_key, WSPrvUrl, False)
    await prv_channel.start()
    await prv_channel.subscribe(
      [{'channel': 'orders', 'instType': 'ANY', 'instId': self.left},
       {'channel': 'orders', 'instType': 'ANY', 'instId': self.right}],
      lambda raw_msg: self.handle_prv_msg(raw_msg))
    self.prv_channel = prv_channel

    pub_channel = WsPublicAsync(WSPubUrl)
    await pub_channel.start()
    await pub_channel.subscribe(
      [{'channel': 'tickers', 'instId': self.left},
       {'channel': 'tickers', 'instId': self.right}],
      lambda raw_msg: self.handle_pub_msg(raw_msg))
    self.pub_channel = pub_channel

  async def stop(self):
    await asyncio.gather(
      *map(lambda x: x.factory.close(),
           filter(lambda x: x is not None, (self.pub_channel, self.prv_channel))))

  async def stop_self_and_loop(self):
    await self.stop()
    self.loop.stop()

  def handle_task_exception(self, loop, ctx):
    e = ctx['exception']
    msg = ctx['message']
    logger.error(f'Exception occurred, msg={msg}, exception={e}')
    loop.create_task(self.stop_self_and_loop())
    logger.critical(''.join(traceback.format_exception(e)))

  def run_until_complete(self):
    loop = self.loop
    loop.set_exception_handler(lambda loop, ctx: self.handle_task_exception(loop, ctx))
    loop.create_task(self.start())
    try:
      loop.run_forever()
    except KeyboardInterrupt:
      loop.run_until_complete(trader.stop())
    finally:
      asyncio.runners._cancel_all_tasks(loop)
      loop.close()


def set_trade_mode(trade_mode, proxy=None):
  account = AccountAPI(
    api_key, secret_key, passphrase, flag=flag,
    debug=False, proxy=proxy)
  res = account.set_position_mode(trade_mode)
  if res['code'] != '0':
    raise TraderError(
      'failed to set trade mode to %s, msg: %s' %
      (trade_mode, json.dumps(res)))
  logger.info('set trade mode to %s' % trade_mode)


def set_leverage(inst_id, leverage, proxy=None):
  account = AccountAPI(
    api_key, secret_key, passphrase, flag=flag,
    debug=False, proxy=proxy)
  res = account.set_leverage(
    instId=inst_id, lever=leverage, mgnMode='isolated')
  if res['code'] != '0':
    raise TraderError(
      'failed to set leverage for %s to %s, msg: %s' %
      (inst_id, leverage, json.dumps(res)))
  logger.info('set leverage for %s to %s' % (inst_id, leverage))


def config_root_logger(level=logging.DEBUG):
  # Silly okx library calls basicConfig when importing, so set force=true to
  # clear all existing handlers.
  logging.basicConfig(level=level, force=True, handlers=())
  logging.getLogger().setLevel(level)


def config_console_logger(level=logging.INFO):
  formatter = colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s.%(msecs)03d][%(levelname)s][%(module)s:%(lineno)d] %(message)s',
    log_colors={
      'DEBUG': 'cyan',
      'INFO': 'green',
      'WARNING': 'yellow',
      'ERROR': 'red',
      'CRITICAL': 'bold_red',
    },
    datefmt='%Y-%m-%d %H:%M:%S')
  console_handler = logging.StreamHandler()
  console_handler.setLevel(level)
  console_handler.setFormatter(formatter)
  logging.getLogger().addHandler(console_handler)


def config_file_logger(filename, level=logging.DEBUG):
  if filename is None:
    return
  formatter = logging.Formatter(
    '[%(asctime)s.%(msecs)03d][%(levelname)s][%(module)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%D %H:%M:%S')
  file_handler = logging.FileHandler(filename)
  file_handler.setLevel(level)
  file_handler.setFormatter(formatter)
  logging.getLogger().addHandler(file_handler)

config_root_logger(logging.INFO)
config_console_logger(logging.INFO)
logfile = 'log_sim/' if flag == '1' else 'log/'
logfile = logfile + time.strftime('%Y%m%d-%H%M%S') + '.log'
config_file_logger(logfile, logging.DEBUG)

logging.getLogger('WsPublic').setLevel(level=logging.WARNING)
logging.getLogger('WsPrivate').setLevel(level=logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

spot = 'XRP-USDT'
future = 'XRP-USD-240927'

http_proxy = 'socks5://172.18.80.1:7890'

# set_trade_mode('net_mode', proxy=http_proxy)
set_leverage(future, '1.5', proxy=http_proxy)

loop = asyncio.get_event_loop()
#trader = Trader(loop, future, spot, Trader.MODE_OPEN, 3., 4000, 713,
trader = Trader(loop, future, spot, Trader.MODE_CLOSE, 9., 400, 73,
                lag_threshold=1000, http_proxy=http_proxy)
trader.run_until_complete()
