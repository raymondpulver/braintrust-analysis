import sqlite3, datetime, time, requests, dateutil, numpy as np
#import matplotlib.pyplot as plt
import pandas as pd

connbars = sqlite3.connect('../market-data-sqlite/barsDb.db')

def timestampToDate(ts):
    """Returns a datetime object for the ts - whichis assumed to be in UTC time."""
    utc_dt = datetime.datetime.utcfromtimestamp(ts)
    aware_utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    return aware_utc_dt

def bDateRange(s, tb = 'binance'):
    """Returns the earliest, latest timestamps for the symbol. """
    cursor = connbars.cursor()
    
    sql = """SELECT ts FROM """ + tb + """ WHERE symbol = ? ORDER BY ts"""
    cursor.execute(sql, (s,))
    r = cursor.fetchall()
    if r:
        earliest = r[0][0]
        latest = r[-1][0]
    else:
        earliest = 0
        latest = int(datetime.datetime(2019, 1, 1).timestamp())
        
    return earliest, latest
        
def writeBarsToMex(bars, symbol):    
    cursor = connbars.cursor()
    
    sql = """INSERT OR IGNORE INTO bitmex VALUES (?,?,?,?,?,?,?)"""
    for b in bars:
        try:
            ts = dateutil.parser.parse(b['timestamp'])
            ts = int(ts.timestamp())
            o = float(b['open'])
            h = float(b['high'])
            l = float(b['low'])
            c = float(b['close'])
            v = float(b['volume'])
            cursor.execute(sql, (ts, symbol, o, h, l, c, v))
        except:
            pass
    connbars.commit()
    
def writeBarsToGDAX(bars, symbol):
    cursor = connbars.cursor()
    
    # bar format = [timestamp, low, high, open, close, volume ]
    
    sql = """INSERT OR IGNORE INTO gdax VALUES (?,?,?,?,?,?,?)"""
    for b in bars:
        try:
            ts = b[0]
            o = float(b[3])
            h = float(b[2])
            l = float(b[1])
            c = float(b[4])
            v = float(b[5])
            cursor.execute(sql, (ts, symbol, o, h, l, c, v))
        except:
            pass
    connbars.commit()    
    
    
def writeBarsToB(bars, symbol, tb = "binance"):    
    """Bars is an array of:
      [
                  1499040000000,      // Open time
                "0.01634790",       // Open
                "0.80000000",       // High
                "0.01575800",       // Low
                "0.01577100",       // Close
                "148976.11427815",  // Volume
                1499644799999,      // Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "17928899.62484339" // Ignore
              ]
    """
    cursor = connbars.cursor()
    
    sql = """INSERT OR IGNORE INTO """ + tb + """ VALUES (?,?,?,?,?,?,?)"""
    for b in bars:
        ts = round(b[6]/1000)
        o = float(b[1])
        h = float(b[2])
        l = float(b[3])
        c = float(b[4])
        v = float(b[5])
        cursor.execute(sql, (ts, symbol, o, h, l, c, v))
    connbars.commit()   
    
def writeBarsToFunding(bars, symbol, exchange):    
    """Bars is an array of:
     [
                {'symbol': 'BTCUSDT', 
                'fundingTime': 1568102400000,   #Binance ts are in milliseconds.
                'fundingRate': '0.00010000'},
                
                {...}
            ]      
    """
    cursor = connbars.cursor()
    
    sql = """INSERT OR IGNORE INTO perpfunding VALUES (?,?,?,?)"""
    for b in bars:
        ts = round(b['fundingTime']/1000)
        rate = float(b['fundingRate'])
        cursor.execute(sql, (exchange, b['symbol'], ts, rate))
    connbars.commit()        
    
def fetchBinanceSyms(b, symbolList = ['ETHBTC', 'XRPBTC', 'LTCBTC', 'ADABTC', 'BCCBTC']):
    """Fetches 1m bars for symbolList. If symbolList is empty, fetches all binance symbols with 'BTC' in the name. """ 
    #BSymbols = [x for x in b.symbolExchInfo.keys() if b.symbolExchInfo[x]['quoteAsset'] == 'BTC']
    #if symbolList:
        #syms = set(BSymbols).intersection(symbolList)
    #else:
        #syms = BSymbols
    
    currentTime = time.time() - 1
    for s in symbolList:
        print('Starting download of symbol: ', s, ' from Binance.')
        earliest, latest = bDateRange(s)
        startTime = latest 
        endTime = startTime + 499 * 60      
        if earliest != 0:
            print('Esimated number of bars to download: ', int((currentTime - startTime)/60) ) 
        else:
            print('No previous entry for the symbol in the db')
        while startTime < currentTime:
            r = b.getBarData(s, startTime * 1000, endTime * 1000)
            writeBarsToB(r, s)
            print(s, ' bars downloaded - latest date is: ', timestampToDate(endTime), 'Number:', len(r))
            startTime = endTime 
            endTime = endTime + 499 * 60
            time.sleep(2)    
            
def fetchBinanceSymsFutures(b, symbolList):
    """Fetches 1m bars for symbolList."""
    currentTime = time.time() - 1
    for s in symbolList:
        print('Starting download of future symbol: ', s, ' from Binance.')
        earliest, latest = bDateRange(s, "binanceFutures")
        if earliest == 0:
            print("No previous funding data for this exchange and symbol")
            startTime = 0
            endTime = currentTime
        else:
            startTime = latest
            endTime = startTime + 1499 * 60        
        if earliest != 0:
            print('Esimated total number of bars to download: ', int((currentTime - startTime)/60) ) 
        else:
            print('No previous entry for the symbol in the db')
        while startTime < currentTime:
            r = b.getBarDataFutures(s, startTime, endTime)
            writeBarsToB(r, s, "binanceFutures")
            print(s, ' bars downloaded - latest date is: ', misc.timestampToDate(r[-1][0]/1000), 'Number:', len(r))
            startTime = int(r[-1][0]/1000)
            endTime = startTime + 1499 * 60
            time.sleep(2)                
            
def fetchBinanceFunding(b, symbolList ):
    """Fetches funding data, from binance, for symbols in symbolList. """ 
    
    EIGHT_HOURS = 60 * 60 * 8
    currentTime = time.time() - 1
    for s in symbolList:
        samples_fetched = 0
        print('Starting download of funding data for symbol: ', s, ' from Binance.')
        earliest, latest = fundingDateRange(s, "binance")
        if earliest == 0:
            print("No previous funding data for this exchange and symbol")
            startTime = 0
            endTime = currentTime
        else:
            startTime = latest
            endTime = currentTime
        print('Estimated number of bars to download: ', int((currentTime - startTime)/(3600*8) )) 
        while startTime < currentTime:
            r = b.getFundingRateHistory(s, startTime, endTime)
            samples_fetched += len(r)
            writeBarsToFunding(r, s, "binance")
            print(s, ' funding bars downloaded - latest date is: ', misc.timestampToDate(endTime), 'Number:', len(r))
            startTime = endTime 
            endTime = startTime + EIGHT_HOURS * 1000
            time.sleep(2)    
        print("Done fetching funding info for",s,"Samples fetched:", samples_fetched)
        
def fetchBitmexFunding(symbolList):
    """Fetches funding data, from bitmex, for symbols in symbolList. """ 
    baseURI = "https://www.bitmex.com/api/v1"
    endpoint = "/funding"   
    currentTime = datetime.datetime.now().isoformat() + "Z"
    for symbol in symbolList:
        print('Fetching funding data for', symbol,' from bitmex.')
        samples_fetched = 0
        earliest, latest = fundingDateRange(symbol, "bitmex")
        if earliest == 0:
            print("No previous funding data for this exchange and symbol")
            startTime = "2016-06-29T20:00:00.000Z"
        else:
            startTime = datetime.datetime.fromtimestamp(latest).isoformat() + "Z"
            
        blnDone = False
        while datetime.datetime.fromisoformat(startTime[:-1]) < datetime.datetime.fromisoformat(currentTime[:-1]) and not blnDone:
            params = {'symbol': symbol, 'startTime' : startTime}
            r = requests.get(baseURI + endpoint, params = params)
            r = r.json()
            samples_fetched += len(r)
            #convert to binance format
            res = []
            for v in r:
                o = {'symbol': v['symbol'], 
                     'fundingTime': datetime.datetime.fromisoformat(v['timestamp'][:-1]).timestamp() * 1000,
                     'fundingRate': v['fundingRate']}
                res.append(o)
            if len(res) < 2: blnDone = True
            writeBarsToFunding(res, symbol, "bitmex")
            startTime = r[-1]["timestamp"]
            time.sleep(2)    
        print("Done fetching funding info for",symbol,"Samples fetched:", samples_fetched)
    
def fundingDateRange(s, exchange):
    """Returns the earliest, latest timestamps for the symbol and exchange. Returns (0, 0) if no entry in db."""
    cursor = connbars.cursor()
    
    sql = """SELECT ts FROM perpfunding WHERE symbol = ? AND exchange = ? ORDER BY ts"""
    cursor.execute(sql, (s,exchange))
    r = cursor.fetchall()
    if r:
        earliest = r[0][0]
        latest = r[-1][0]
    else:
        earliest = 0
        latest = 0
        
    return earliest, latest
    
def fetchMexSymbols(symbolList = ['XBTZ18', 'ADAM18', 'BCHM18', 'ETHM18', 'XRPM18', 'LTCM18', 'XBTUSD', 'XBTM18', 'XBTU18', 'XBTZ18']):   
    """Fetches bars from bitmex."""
    print('Updating bitmex symbols.')  
    
    #si = ['.BXBT', '.XBTUSDPI','.XBTBON8H', '.XBTBON2H', '.USDBON8H', '.XBTUSDPI8H', '.XBTUSDPI2H', '.XBTBON', '.USDBON'  ]
    
    #symbolList = symbolList + si

    baseURI = "https://www.bitmex.com/api/v1"
    endpoint = "/trade/bucketed" 
    for symbol in symbolList:
        print('Fetching data for', symbol,' from bitmex.')
        #Get latest and earliest bitmex timestamp
        if '8H' in symbol or '2H' in symbol:
            binsize = '1h'
        else:
            binsize = '1m'
        params = {'binSize': binsize, 'symbol': symbol, 'count' : 1, 'start' : 0, 'reverse': 'false'}
        r = requests.get(baseURI + endpoint, params = params)
        r = r.json()
        bitmexEarliest = r[0]['timestamp']          
        params = {'binSize': binsize, 'symbol': symbol, 'count' : 1, 'start' : 0, 'reverse': 'true'}
        r = requests.get(baseURI + endpoint, params = params)
        r = r.json()
        bitmexLatest = r[0]['timestamp']
        
        
        earliestTs, latestTS = bDateRange(symbol, tb = 'bitmex')
        latestDB = misc.timestampToDate(latestTS).isoformat()
        if latestDB < bitmexEarliest:
            latestDB = bitmexEarliest
            
        #calc the estimated number of bars to fetch
        if '+' in latestDB:
            latestDB = latestDB[:latestDB.find('+')]
        numberBars = int((dateutil.parser.parse(bitmexLatest).timestamp() - dateutil.parser.parse(latestDB + 'Z').timestamp())/60)
        print('Estimated number of bars to fetch is ', numberBars)
        
                
        while dateutil.parser.parse(latestDB + 'Z').timestamp() < dateutil.parser.parse(bitmexLatest).timestamp():
            params = {'binSize': binsize, 'symbol': symbol, 'count' : 300, 'startTime' : latestDB}
            r = requests.get(baseURI + endpoint, params = params)
            try:
                r = r.json()
                writeBarsToMex(r, symbol)
                earliestTs, latestTS = bDateRange(symbol, tb = 'bitmex')
                latestDB = misc.timestampToDate(latestTS).isoformat()
                if '+' in latestDB:
                    latestDB = latestDB[:latestDB.find('+')]            
            except:
                pass
            time.sleep(2)
            
def fetchGDAXSymbols(symbolList = ['ETH-USD']):    
    print('Updating gdax symbols.')  
    import GDAX
    gdax = GDAX.GDAX()
    
    for symbol in symbolList:
        print('Fetching data for', symbol,' from GDAX.')
        
        earliestTs, latestTS = bDateRange(symbol, tb = 'gdax')
        stopTime = time.time() - 60
        
        prevEnd = 0
        while latestTS < stopTime:
            start = latestTS
            end = latestTS + 60 * 299
            if end <= prevEnd:
                end += 600; start += 600
            startISO = misc.timestampToDate(start).isoformat()
            endISO = misc.timestampToDate(end).isoformat()
            bars = gdax.getHistoricBars(symbol, startISO, endISO, 60)
            prevEnd = end
            if bars:
                writeBarsToGDAX(bars, symbol)
                earliestTs, latestTS = bDateRange(symbol, tb = 'gdax')
                print('{} bars fetched. Latest date is {}'.format(len(bars), misc.timestampToDate(latestTS).isoformat()))
            else:
                latestTS = end
                print('Zero bars fetched. Latest date is {}'.format(misc.timestampToDate(end).isoformat()))
            time.sleep(1)    
                        
            
def readBarsDB(symbol, exchange = 'binance', startTS = None, endTS = None):
    """Reads bars for 'symbol' from the database. Returns dictionary of (ts, o, h, l, c, v) tuples, indexed by ts
    
    Exchange is 'binance' or 'bitmex'.
    
    Reads all bars if startTS, endTS are None."""
    
    cursor = connbars.cursor()
    if startTS == None:
        startTS = 0
    if endTS == None:
        endTS = int(time.time()) 
       
    bars = {} 
    sql = """SELECT ts, open, high, low, close, volume FROM """ + exchange + """ WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"""
    cursor.execute(sql, (symbol, startTS, endTS))
    res = cursor.fetchall()
    if res:
        for r in res:
            bars[r[0]] = ((r[0], r[1], r[2], r[3], r[4], r[5]))
       
        
    return bars

def resampleBars(df, barSize):
    """Valid barSizes are 5, 10, 30, 60."""
    if not (barSize == 5 or barSize == 10 or barSize == 30 or barSize == 60):
        raise ValueError('Error in resampleBars - unsupported bar size.')
    
    blnStarted = False
    bars = []
    Os = df['O'].values; Hs = df['H'].values; Ls = df['L'].values
    Cs = df['C'].values; Vs = df['V'].values; TSs = df.index
    i = 0
    while i < len(Os):
        if TSs[i].minute % barSize == 0:
            if not blnStarted:
                blnStarted = True
            else:
                bar = {'O': O[0], 'H' : max(H), 'L' : min(L),
                       'C': C[-1], 'V' : sum(V), 'ts': T[-1]}
                bars.append(bar)
            O = [Os[i]]; H= [Hs[i]]; L=[Ls[i]]; C=[Cs[i]]; V= [Vs[i]]; T= [TSs[i]]
        elif blnStarted:
            O.append(Os[i]); H.append(Hs[i]); L.append(Ls[i])
            C.append(Cs[i]); V.append(Vs[i]); T.append(TSs[i])
        i += 1
            
    df = pd.DataFrame(bars)
    df.set_index('ts', inplace = True)
    return df
    

def readBarsDB_pd(symbol, exchange = 'binance', startTS = None, endTS = None, barSize = 1):
    """Reads bars for 'symbol' from the database. Returns pandas df with columns O, H, L, C, V, indexed by ts
    
    Exchange is 'binance' or 'bitmex' or 'gdax'
    
    Reads all bars if startTS, endTS are None.
    
    barSize can return 5min (5), 10, 30, 60 min bars."""
    
    if startTS == None:
        startTS = 0
    else:
        startTS = startTS.timestamp()
    if endTS == None:
        endTS = time.time()
    else:
        endTS = endTS.timestamp()
        

    sql = "SELECT ts, open, high, low, close, volume FROM " + exchange + " WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df = pd.read_sql(sql, connbars, params = (symbol, startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df.columns = ['O', 'H', 'L', 'C', 'V']
    
    if barSize != 1:
        df = resampleBars(df, barSize)
        
    df = df.loc[~df.index.duplicated(keep='first')]

    return df   

def fetchFundingBars(startTS = None, endTS = None):
    if startTS == None:
        startTS = 0
    else:
        startTS = startTS.timestamp()
    if endTS == None:
        endTS = time.time()
    else:
        endTS = endTS.timestamp()
        
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df = pd.read_sql(sql, connbars, params = (".XBTBON8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df.columns = ['IBI']        
    df = df.loc[~df.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df2 = pd.read_sql(sql, connbars, params = (".USDBON8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df2.columns = ['IQI']     
    df2 = df2.loc[~df2.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df3 = pd.read_sql(sql, connbars, params = (".XBTUSDPI", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df3.columns = ['P1M']   
    df3 = df3.loc[~df3.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df4 = pd.read_sql(sql, connbars, params = (".XBTUSDPI8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df4.columns = ['P8H']  
    df4 = df4.loc[~df4.index.duplicated(keep='first')]
    
    #s = set.intersection(set(df.index), set(df2.index), set(df3.index), set(df4.index))
    
    #Todo - build a dictionary with an entry for each date in s.
    
    df5 = pd.concat([df, df2, df4, df3], axis = 1, ignore_index=True)
    df5 = df5.rename({0:'IBI', 1:'IQI', 2:'P1M', 3:'P8H'}, axis = 'columns')

    return df5

def fetchFundingBarsEth(startTS = None, endTS = None):
    if startTS == None:
        startTS = 0
    else:
        startTS = startTS.timestamp()
    if endTS == None:
        endTS = time.time()
    else:
        endTS = endTS.timestamp()
        
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df = pd.read_sql(sql, connbars, params = (".ETHBON8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df.columns = ['IBI'] 
    df = df.loc[~df.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df2 = pd.read_sql(sql, connbars, params = (".USDBON8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df2.columns = ['IQI']     
    df2 = df2.loc[~df2.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df3 = pd.read_sql(sql, connbars, params = (".ETHUSDPI", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df3.columns = ['P1M']       
    df3 = df3.loc[~df3.index.duplicated(keep='first')]
    
    sql = "SELECT ts, close FROM bitmex WHERE symbol = ? AND ts > ? and ts < ? ORDER BY ts"
    df4 = pd.read_sql(sql, connbars, params = (".ETHUSDPI8H", startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df4.columns = ['P8H']   
    df4 = df4.loc[~df4.index.duplicated(keep='first')]
    
    df5 = pd.concat([df, df2, df4, df3], axis = 1, ignore_index=True)
    df5 = df5.rename({0:'IBI', 1:'IQI', 2:'P1M', 3:'P8H'}, axis = 'columns')

    return df5

def fetchFundingData(symbol, exchange, startTS = None, endTS = None):
    """Fetch funding data from the 'perpFunding' table."""
    if startTS == None:
        startTS = 0
    else:
        startTS = startTS.timestamp()
    if endTS == None:
        endTS = time.time()
    else:
        endTS = endTS.timestamp()
        
    sql = "SELECT ts, value FROM perpfunding WHERE symbol = ? AND exchange = ? AND ts > ? and ts < ? ORDER BY ts"
    df = pd.read_sql(sql, connbars, params = (symbol, exchange, startTS, endTS), index_col = 'ts', parse_dates = {'ts': {'unit':'s', 'utc': True}})
    df.columns = ['fund_rate'] 
    
    return df

def ma(v, period):
    """Returns a moving average version of hte list v."""
    v2 = v[:]
    runSum = 0.0
    for idx, x in enumerate(v):
        runSum += x
        if idx >= period:
            runSum = runSum - v[idx - period]
            v2[idx] = runSum / period
        else:
            v2[idx] = runSum / (idx + 1)
    return v2

def fetchAll(b, exchange = 'b'):
    if exchange == 'b':
        fetchBinanceSyms(b)
        fetchMexSymbols()             
    elif exchange == 'm' or exchange == 'mex':
        fetchMexSymbols()
    elif exchange == 'binance':
        fetchBinanceSyms(b)
    
    

if __name__ == "__main__":
    #df= fetchFundingBarsEth()
    #l3 = ['XBTUSD', 'XBTM19', 'ETHM19', 'XBTU19']
    #fetchMexSymbols(l3)
    #import pandas as pd, datetime
    #start_date = datetime.datetime(2018, 1, 1)
    #print("Reading in price feed data.")
    #P = readBarsDB_pd('XBTUSD', 'bitmex',  startTS = start_date, barSize = 5)  
    #print(P.tail(10))
    
    
    #s ='BTCUSDT'
    #b = Binance.binance()
    ##fetchBinanceFunding(b, [s])
    
    #fetchBinanceSymsFutures(b, [s])
    
    fetchBitmexFunding(['XBTUSD'])
    
    
