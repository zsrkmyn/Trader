from okx.MarketData import MarketAPI
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import pickle
import os
from math import sqrt


def load_candles(fname):
  if not os.path.exists(fname):
    return {}
  with open(fname, 'rb') as f:
    return pickle.load(f)

def update_candles(fname):
  bar = '6H'
  limit = 100
  time_offset_one = 6 * 60 * 60 * 1000
  time_offset = time_offset_one  * limit

  all_candles = load_candles(fname)
  default_start_time = int(time.mktime(time.strptime('2022-09-01', '%Y-%m-%d'))*1000)
  end_time = time.time_ns() // 1000000

  market = MarketAPI(debug=0, flag='0')

  tickers = market.get_tickers('SPOT')
  tickers = list(filter(lambda x: float(x['volCcy24h']) > 1000, tickers['data']))
  # tickers = [{'instId': 'SONIC-USDT'}]

  num = len(tickers)
  print("total tickers: ", num)

  i = 0
  j = 0
  for ticker in tickers:
    j += 1
    instId = ticker['instId']
    candles = all_candles.get(instId)
    if not candles:
      candles = []
      all_candles[instId] = candles
      start_time = default_start_time
    else:
      start_time = int(candles[-1][0])

    cnt = (end_time-start_time)//time_offset + 1
    et = end_time
    print(instId, j, '/', num)
    tmp_candles = []
    while et - time_offset_one > start_time:
        i += 1
        if i == 10:
          i = 0
          time.sleep(1.3)
        print(et, start_time, (end_time-et)//time_offset + 1, '/', cnt)
        for _ in range(3):
          try:
            c = market.get_history_candlesticks(instId, bar='6H', before=start_time, after=et, limit=limit)
            break
          except Exception as e:
            print(e)

        try:
          data = c['data']
        except KeyError as e:
          print(e)
          print(c['code'], c['msg'])

        if not data:
          break

        tmp_candles += data
        et = int(data[-1][0])

    candles += reversed(tmp_candles)

  with open(fname, 'wb') as f:
    pickle.dump(all_candles, f)

  return all_candles


hist_fname = 'hist.pickle'

# all_candles = update_candles(hist_fname)
all_candles = load_candles(hist_fname)

all_candles = {k: v for k, v in all_candles.items() if v and ('USDT' in k or 'USDC' in k)}

btc_candles = all_candles['BTC-USDT']


def correlation(btc_candles, coin_candles):
  l = len(coin_candles)
  btc_candles = btc_candles[-l:]
  btc_px0 = float(btc_candles[0][4])
  c_px0 = float(coin_candles[0][4])
  btc_px = tuple(float(b[4]) for b in btc_candles)
  coin_px = tuple(float(c[4]) for c in coin_candles)
  b_mean = sum(btc_px) / l
  c_mean = sum(coin_px) / l
  cov = sum((b - b_mean) * (c - c_mean) for b, c in zip(btc_px, coin_px))
  b_sqr_var = sum(map(lambda x: (y := x - b_mean, y * y)[1], btc_px))
  c_sqr_var = sum(map(lambda x: (y := x - c_mean, y * y)[1], coin_px))
  return cov / sqrt(b_sqr_var * c_sqr_var)

corr = {}

for c in all_candles.items():
  inst, c = c
  corr[inst] = correlation(btc_candles, c)


all_candles = {k: v for k, v in sorted(all_candles.items(), key=lambda x: corr[x[0]], reverse=True)}

figs = []

# i = 0
for c in all_candles.items():
  instId = c[0]
  candles = c[1]
  # i+=1
  # if i == 6: break
  l = len(candles)
  btc_slice = btc_candles[-l:]
  btc_px0 = float(btc_slice[0][4])
  c_px0 = float(candles[0][4])
  # ratio = tuple((float(c[4])/c_px0)/(float(b[4])/btc_px0) for b, c in zip(btc_slice, candles))
  # ratio = tuple((float(b[4])/btc_px0) - (float(c[4])/c_px0) for b, c in zip(btc_slice, candles))
  btc_px = tuple(float(b[4])/btc_px0 - 1 for b in btc_slice)
  coin_px = tuple(float(c[4])/c_px0 - 1 for c in candles)
  ratio = tuple(b - c for b, c in zip(btc_px, coin_px))
  vol = tuple(float(c[6]) for c in candles)
  t = tuple(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(c[0]) // 1000)) for c in candles)
  fig = make_subplots(vertical_spacing=0, rows=2, cols=1, row_heights=[0.7, 0.3], shared_xaxes=True)
  fig.add_trace(go.Scatter(x=t, y=ratio, name='ratio', mode='lines', xhoverformat='%b. %d, %Y'))
  fig.add_trace(go.Scatter(x=t, y=btc_px, name='btc', mode='lines', xhoverformat='%b. %d, %Y'))
  fig.add_trace(go.Scatter(x=t, y=coin_px, name='coin', mode='lines', xhoverformat='%b. %d, %Y'))
  fig.add_trace(go.Bar(x=t, y=vol, name='vol', marker=dict(color='red')), row=2, col=1)
  fig.update_yaxes(autorange=True, fixedrange=False)
  fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=False)
  fig.update_layout(xaxis_rangeslider_visible=False,
                    xaxis=dict(zerolinecolor='black', showticklabels=False))
  figs.append((instId, fig))

# fig.add_trace(
#     go.Scatter(x=t, y=tuple((float(b[4])/b0) for b in bs), name='BTC', mode='lines', xhoverformat='%b. %d, %Y'))
# fig.add_trace(
#     go.Scatter(x=t, y=tuple((float(c[4])/c0) for c in cs), name='DOGE', mode='lines', xhoverformat='%b. %d, %Y'))

with open('hist.html', 'w') as f:
  f.write('<!DOCTYPE html>')
  f.write("<html><head><meta charset='UTF-8'><script src='js/plotly-2.6.3.min.js'></script>")
  f.write("<link rel='stylesheet' type='text/css' href='style.css'>")
  f.write('''<script>
        // JavaScript 函数用于折叠/展开内容
        function toggleContent(contentId) {
            const content = document.getElementById(contentId);
            if (content.classList.contains('active')) {
                content.classList.remove('active');
            } else {
                content.classList.add('active');
            }
        }
    </script>''')
  f.write('</head>')
  for instId, fig in figs:
    f.write(f'''<div class="container">
        <div class="header">
            <h2>{instId}</h2>
            <button class="toggle-btn" onclick="toggleContent('{instId}')">折叠/展开</button>
        </div>
        <div id="{instId}" class="content">
          <text>R = {corr[instId]}</text>
          {fig.to_html(full_html=False, include_plotlyjs=False)}
        </div>
    </div>''')
  f.write('</html>')
