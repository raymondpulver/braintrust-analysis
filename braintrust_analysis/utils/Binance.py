#bittrex API 
import requests, json, datetime, pytz, time
from threading import Lock
import _thread
import hmac, hashlib, math
from misc import getTimeStamp
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logger.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

class binanceRateLimits:
    def __init__(self, rateLimitJson, lock):
        self.lock = lock
        self.rateLimits = {'REQUESTS': [], 'ORDERS': []}
        for d in rateLimitJson:
            intervalSecs= 0
            if d['interval'] == 'SECOND':
                intervalSecs = 1
            elif d['interval'] == 'MINUTE':
                intervalSecs = 60
            elif d['interval'] == 'DAY':
                intervalSecs = 60 * 60 * 24                                
            d2 = {'interval': intervalSecs, 'limit': None, 'counter': 0, 'lstOfEntries': []} 
            if d['rateLimitType'] == 'REQUESTS':
                d2.update({'limit': d['limit']})
                self.rateLimits['REQUESTS'].append(d2)
            elif d['rateLimitType'] == 'ORDERS':
                d2.update({'limit': d['limit']})
                self.rateLimits['ORDERS'].append(d2)
        logger.info('Binance rate limits: %s', self.rateLimits)
        
                
    def update(self, weightType, weight):
        """Update the counter value of the REQUESTS or ORDERS weight.
        Returns False if the counter is above an acceptable limit."""
        #increment counters
        blnOutput = True
        nowTS = getTimeStamp()
        self.lock.acquire() 
        if weightType == 'REQUESTS':
            for d in self.rateLimits['REQUESTS']:
                #update counter and list of previous entries 
                d['counter'] += weight
                d['lstOfEntries'].append((weight, nowTS))
                
                #delete entries as appropriate
                deleteCount = 0
                totalAdj = 0
                for w, ts in d['lstOfEntries']:
                    if nowTS - ts > d['interval']:
                        totalAdj += w
                        deleteCount += 1
                    else:
                        break
                d['counter'] = d['counter'] - totalAdj
                del d['lstOfEntries'][:deleteCount]   #delete expired entries   
                
                #check if rate violation
                if d['counter'] >= d['limit']:
                    blnOutput = False
                    logger.info("Binance: Rate violation for REQUESTS type")
        elif weightType == 'ORDERS':
            for d in self.rateLimits['ORDERS']:
                d['counter'] += weight
                d['lstOfEntries'].append((weight, nowTS))
                #delete entries as appropriate
                deleteCount = 0
                totalAdj = 0
                for w, ts in d['lstOfEntries']:
                    if nowTS - ts < d['interval']:
                        totalAdj += w
                        deleteCount += 1
                    else:
                        break
                d['counter'] = d['counter'] - totalAdj
                del d['lstOfEntries'][:deleteCount]   #delete expired entries   
                if d['counter'] >= d['limit']:
                    blnOutput = False
                    logger.info("Binance: Rate violation for ORDERS type")
        self.lock.release()        
        
        return blnOutput
        
            

class binance:
    def __init__(self, APIKey = None, Secret = None):
        self.APIKey = APIKey
        self.Secret = Secret
        self.URL = 'https://api.binance.com'
        
        r = self.exchangeInfo()
        self.symbolExchInfo = {}
        for x in r['symbols']:
            self.symbolExchInfo[x['symbol']] = x
        
        tzUTC = pytz.timezone("UTC")
        secDiff = (datetime.datetime.fromtimestamp(r['serverTime']/1000, tzUTC) - datetime.datetime.now(tzUTC)).total_seconds()
        logger.info('Binance server time is %s', datetime.datetime.fromtimestamp(r['serverTime']/1000, tzUTC)) 
        logger.info('Difference from local server time is %s seconds', round(secDiff,3))
        self.timeStampOffset = secDiff
        if secDiff > 4:
            logger.error('Difference between server time and local time is too big!!')
            raise Exception()
        
        self.lock = Lock()
        self.rateLimits = binanceRateLimits(r['rateLimits'], self.lock)                
            
        self.OrderBookQuotes = {}
        
    def getInfoOnSymbol(self, symbol):
        """Returns information on a trading pair. info is returned in a dictionary and includes the keys:
        baseAsset, quoteAsset, quotePrecision, minQty, minNotional
        """
        ba = self.symbolExchInfo[symbol]['baseAsset']
        qa = self.symbolExchInfo[symbol]['quoteAsset']
        qp = self.symbolExchInfo[symbol]['quotePrecision']
        filters = self.symbolExchInfo[symbol]['filters']
        minQty = [v for v in filters if v['filterType'] == 'LOT_SIZE'][0]['minQty']
        minQty = float(minQty)
        minN = [v for v in filters if v['filterType'] == 'MIN_NOTIONAL'][0]['minNotional']
        minN = float(minN)        
        d = {'baseAsset' : ba, 'quoteAsset' : qa, 'quotePrecision' : qp, 'minQty': minQty, 'minNotional': minN}
        return d
        
    def getOrderBook(self, symbol, limit = 100):
        """Get the full OB for a symbol.
        limit -- OB entries to retrieve. Valid values: [5, 10, 20, 50, 100, 500, 1000].
        return -- Two lists of bids, asks tuples, where each tuple is (price, qty). Returns None, None if error.  
                   E.g., [(4, 431), ... (5, 531)], [(10, 431), ... (11, 531)]
        Bids and asks are returned so that index zero is inside value (i.e., bids are in descending order and asks in ascending)    
        
        Return -1 if an error.
        """
        validLimit = [5, 10, 20, 50, 100, 500, 1000]
        weight = 1
        if limit == 500:
            weight = 5
        elif limit == 1000:
            weight = 10
        if not limit in validLimit:
            logger.error("invalid limit value in call to getOrderBook")
            return -1, -1     
        rateLimitOK = self.rateLimits.update("REQUESTS", weight)
        
        if rateLimitOK:                
            payload = {'symbol':symbol, 'limit':limit}
            r = requests.get(self.URL + '/api/v1/depth', params = payload)   
            if r.status_code == 429:
                logger.info('Rate limit violation (429 error) received from Binance in getOrderBook call')
                return -1, -1    
            elif str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                logger.debug('Return code error in getOrderBook call: ' + str(r.status_code))
                return -1, -1    
            elif r.status_code == 200:
                #parse quotes
                res = r.json()     
                bids = res["bids"]
                bidsOut = [(float(x[0]), float(x[1])) for x in bids]
                asks = res["asks"]
                asksOut = [(float(x[0]), float(x[1])) for x in asks]                           
                return bidsOut, asksOut
        else:
            logger.debug("Rate limit violation for call to getOrderBook")
            return -1, -1
        return None, None
                     
    def exchangeInfo(self):
        r = requests.get(self.URL + '/api/v1/exchangeInfo')        
        if r.status_code == 200:
            return r.json()        
        else:
            logger.error('rate limit violation in exchange info')
            return -1        
  
    def getmarketsummaries(self):  
        """Used to get the last 24 hour summary of all active markets.
        Example info for a symbol:
        
        {"symbol": "BNBBTC",
        "priceChange": "-94.99999800",
        "priceChangePercent": "-95.960",
        "weightedAvgPrice": "0.29628482",
        "prevClosePrice": "0.10002000",
        "lastPrice": "4.00000200",
        "lastQty": "200.00000000",
        "bidPrice": "4.00000000",
        "askPrice": "4.00000200",
        "openPrice": "99.00000000",
        "highPrice": "100.00000000",
        "lowPrice": "0.10000000",
        "volume": "8913.30000000",
        "quoteVolume": "15.30000000",
        "openTime": 1499783499040,
        "closeTime": 1499869899040,
        "fristId": 28385,   // First tradeId
        "lastId": 28460,    // Last tradeId
        "count": 76         // Trade count
        }
        """
        r = requests.get(self.URL + '/api/v1/ticker/24hr')        
        res = r.json()    
        
        weight = max(int(len(res)/2), 1)
        
        rateLimitOK = self.rateLimits.update("REQUESTS", weight)

        return res
    
    def getBarData(self, symbol, startTime, endTime):
        """Get 1 minute bar data for the symbol. 500 bars should be returned per call.
        Starttime and endTime are integer timestamps.
        Example response:
            [
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
            ]            
        """
        rateLimitOK = self.rateLimits.update("REQUESTS", 1)
    
        if rateLimitOK:           
            payload = {'symbol': symbol, 'interval': '1m', 'startTime': startTime, 'endTime': endTime}
            r = requests.get(self.URL + '/api/v1/klines', payload)        
            res = r.json()        
            return res        
        else:
            logger.error('rate limit violation in getBarData')
            return -1
        
    def getBarDataFutures(self, symbol, startTime, endTime):
        """Get 1 minute bar data for the symbol. 500 bars should be returned per call.
        Starttime and endTime are integer timestamps.
        Example response:
            [
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
            ]            
        """
        rateLimitOK = self.rateLimits.update("REQUESTS", 1)
        futures_url = "https://fapi.binance.com"        
    
        if rateLimitOK:           
            payload = {'symbol': symbol, 'interval': '1m', 'startTime': int(startTime * 1000), 'limit': 1500}
            r = requests.get(futures_url + '/fapi/v1/klines', payload)        
            res = r.json()        
            return res        
        else:
            logger.error('rate limit violation in getBarData')
            return -1
            
        
    def getFundingRateHistory(self, symbol, startTime = None, endTime= None):
        """  
        Starttime and endTime are integer timestamps. If startTime is not set,
        the most recent 1000 results are returned. 
        Upto 1000 results are returned per call.
        Results are returned in ascending order.
        
        Example response:
            [
                {'symbol': 'BTCUSDT', 
                'fundingTime': 1568102400000,   #Binance ts are in milliseconds.
                'fundingRate': '0.00010000'},
                
                {...}
            ]            
        """
        rateLimitOK = self.rateLimits.update("REQUESTS", 1)
        futures_url = "https://fapi.binance.com"
    
        if rateLimitOK:           
            if startTime == None:
                payload = {'symbol': symbol, 'limit':1000}
            else:
                payload = {'symbol': symbol, 'startTime': int(startTime*1000), 'endTime': int(endTime*1000),  'limit':1000}
            r = requests.get(futures_url + '/fapi/v1/fundingRate', payload)        
            res = r.json()        
            return res        
        else:
            logger.error('rate limit violation in getBarData')
            return -1        
        
    
    ####Account Endpoints - Trade methods
    def getAllOrders(self, symbol, orderId = None):
        """If orderId is set, it will get orders >= that orderId. Otherwise most recent orders are returned."""
        rateLimitOK = self.rateLimits.update("ORDERS", 5)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            
            payload = {'symbol':symbol, 'timestamp': ts}
            if orderId != None:
                payload['orderId'] = orderId
                    
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/allOrders', payload, reqType = 'GET')  
            s = requests.Session()
            r = s.send(authReq)
            
            if r.status_code != 200:
                logger.error('Return code error in placeOrder: ' + str(r.status_code) + "  " + str(r.content))  
                return -1
            
            res = r.json()   
                       
            return res
        else:
            return -1        
        
    def placeOrder(self, symbol, side, ordType, quantity, clientOrderID, asMaker = False, price = 0, otherParams = {}):
        """
        side - BUY or SELL 
        ordtype - LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER
        quanity - required decimlar
        price - decimal (optional). If price = 0, order will be a market order.
        otherParams - optional list of additional key-value pairs to write to db (not otherwise used)
        """
        rateLimitOK = self.rateLimits.update("ORDERS", 1)
        if rateLimitOK:
            if price == 0 and not ordType == 'MARKET':
                logger.error('price indicates Market order order but ordType is not market.' )
                return -1
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            
            #Apply filters to price and quantity
            price, quantity = self._applyFilters(price, quantity, symbol)
            if price == None:
                return -1
            
            if price == 0:
                payload = {'symbol':symbol, 'side': side, 'type': ordType, 'quantity': quantity, 'timestamp': ts, 
                           'newClientOrderId': clientOrderID}
            else:
                price = round(price, self.symbolExchInfo[symbol]['quotePrecision'])
                fmtStr= '{:.' + str(self.symbolExchInfo[symbol]['quotePrecision']) + 'f}'
                payload = {'symbol':symbol, 'side': side, 'type': ordType, 'quantity': quantity, 'timestamp': ts, 'price': fmtStr.format(price), 
                           'timeInForce': 'GTC', 'newClientOrderId': clientOrderID}            
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/order', payload)  
            s = requests.Session()
            r = s.send(authReq)
            
            if r.status_code != 200:
                logger.error('Return code error in placeOrder: ' + str(r.status_code) + "  " + str(r.content))  
                return -1
            
            res = r.json()   
            newD = {'orderID': res['orderId'],
                                'exchange': 'binance',
                                'side': res['side'].lower(),
                                'amt':float(res['origQty']),
                                'amtFilled': float(res['executedQty']),
                                'amtRemaining': float(res['origQty']) - float(res['executedQty']),
                                'timestamp': res['transactTime']/1000,
                                'symbol': res['symbol'],
                                'clientTradeLabel': res['clientOrderId'],
                                'price' : float(res['price']),
                                'asMaker': asMaker}   
            
            #add additional parameters to the dictionary
            for k in otherParams.keys():
                if not k in newD:
                    newD[k] = otherParams[k]            
                    
            return newD                
        else:
            return -1
        
    def queryOrder(self, orderId, symbol):
        """Check an order's status.
        Example response:
        {
        "symbol": "LTCBTC",
        "orderId": 1,
        "clientOrderId": "myOrder1",
        "price": "0.1",
        "origQty": "1.0",
        "executedQty": "0.0",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "stopPrice": "0.0",
        "icebergQty": "0.0",
        "time": 1499827319559,
        "isWorking": true
        }
        
        """
        rateLimitOK = self.rateLimits.update("ORDERS", 1)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            payload = {'symbol':symbol, 'orderId': orderId, 'timestamp': ts}            
            
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/order', payload, reqType  = 'GET')  
            s = requests.Session()
            r = s.send(authReq)
            
            if str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                logger.debug('Return code error in queryOrder: ' + str(r.status_code))  
                return None
            
            res = r.json()  
            logger.info('Query order: ' + str(res))
            return res
        else:
            return None        
        
    def cancelOrder(self, orderID, symbol):
        """Returns a list of the orderIDs of the canceled orders."""
        rateLimitOK = self.rateLimits.update("ORDERS", 1)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            payload = {'symbol':symbol, 'orderId': orderID, 'timestamp': ts}            
            
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/order', payload, reqType  = 'DELETE')  
            s = requests.Session()
            r = s.send(authReq)
            
            if str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                logger.debug('Return code error in cancelOrder: ' + str(r.status_code))  
                return -1
            
            res = r.json()   
            logger.info('Binance: Cancel order executed: ' + str(res))
            return [res['orderId']]
        else:
            return -1          
        
    def getOpenOrders(self, symbol):
        """Get open orders on a symbol. Set symbol to None for all open orders. Returns -1 if an error.
        Example Response, a list of:
        {
        "symbol": "LTCBTC",
        "orderId": 1,
        "clientOrderId": "myOrder1",
        "price": "0.1",
        "origQty": "1.0",
        "executedQty": "0.0",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "stopPrice": "0.0",
        "icebergQty": "0.0",
        "time": 1499827319559,
        "isWorking": trueO
        }      
        """
        rateLimitOK = self.rateLimits.update("ORDERS", 1)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            payload = {'timestamp': ts}            
            if symbol:
                payload.update({'symbol': symbol})
            
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/openOrders', payload, reqType  = 'GET')  
            s = requests.Session()
            r = s.send(authReq)
            
            if str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                logger.error('Return code error in getOpenOrders: ' + str(r.status_code) + ';' + json.loads(r.content)['msg'])  
                return -1
            
            res = r.json()   
            logger.debug('get open orders executed: ' + str(res))
            outL = []
            for d in res:
                newD = {'orderID': d['orderId'],
                        'exchange': 'binance',
                        'side': d['side'].lower(),
                        'amt': float(d['origQty']),
                        'amtFilled': float(d['executedQty']),
                        'amtRemaining': float(d['origQty']) - float(d['executedQty']),
                        'timestamp': d['time']/1000,
                        'symbol': d['symbol'],
                        'clientOrdID' : d['clientOrderId']}  
                if newD['amtFilled'] < newD['amt']:
                    outL.append(newD)            
            return outL
        else:
            return -1            
    def getAccountBalances(self):
        """Returns a list of dictionaries of the balances. Returns -1 if error.
        Example output:
        [
        {
          "asset": "BTC",
          "free": "4723846.89208129",
          "locked": "0.00000000"
        },
        {
          "asset": "LTC",
          "free": "4763368.68006011",
          "locked": "0.00000000"
        }
        ]
      """
        rateLimitOK = self.rateLimits.update("ORDERS", 5)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            payload = {'timestamp': ts}            
                       
            authReq = self._prepareAuthRequest(self.URL + '/api/v3/account', payload, reqType  = 'GET')  
            s = requests.Session()
            r = s.send(authReq)
            
            if str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                logger.error('Return code error in getAccountBalances: ' + str(r.status_code) + ';' + str(r.content))  
                return -1
            
            res = r.json()   
            logger.debug('get account balances executed: ' + str(res))
            return res['balances']
        else:
            return -1    
        
    def getAggTrades(self, symbol, startTime = None, endTime = None):
        """
        Get compressed, aggregate trades. Trades that fill at the time, from the same order, with the same price will have the quantity aggregated.

        Parameters:
        Name 	Type 	Mandatory 	Description
        symbol 	STRING 	YES 	
        startTime 	LONG 	NO 	Timestamp in ms to get aggregate trades from INCLUSIVE.
        endTime 	LONG 	NO 	Timestamp in ms to get aggregate trades until INCLUSIVE.
        
        If both startTime and endTime are sent, limit should not be sent AND the distance between startTime and endTime must be less than 24 hours.
        If frondId, startTime, and endTime are not sent, the most recent aggregate trades will be returned.
        
        Example Response:
        [
        {
          "a": 26129,         // Aggregate tradeId
          "p": "0.01633102",  // Price
          "q": "4.70443515",  // Quantity
          "f": 27781,         // First tradeId
          "l": 27781,         // Last tradeId
          "T": 1498793709153, // Timestamp
          "m": true,          // Was the buyer the maker?
          "M": true           // Was the trade the best price match?
        }
      ]
        """
        rateLimitOK = self.rateLimits.update("ORDERS", 1)
        if rateLimitOK:
            ts = int((getTimeStamp() + self.timeStampOffset) * 1000)
            payload = {'timestamp': ts, 'symbol': symbol}            
            if startTime != None:
                payload['startTime'] = startTime
            if endTime != None:
                payload['endTime'] = endTime
                       
            authReq = self._prepareAuthRequest(self.URL + '/api/v1/aggTrades', payload, reqType  = 'GET')  
            s = requests.Session()
            r = s.send(authReq)
            
            if r.status_code != 200:
                logger.error('Return code error in getAccountBalances: ' + str(r.status_code) + ';' + str(r.content))  
                return -1
            
            res = r.json()   
            logger.debug('get aggTrade executed: ' + str(res))
            return res
        else:
            return -1    
        
     
    def _OBQuote(self,  symbol, requestPerMinute):
        def processQuote(r):
            d = {'bidPrice': float(r['bidPrice']), 'bidQty': float(r['bidQty']), 'askPrice': float(r['askPrice']), 
                 'askQty': float(r['askQty']),
                 'timeStamp': getTimeStamp()}
            return r['symbol'], d
        
        sleepAmount = 60.0/requestPerMinute
        while True:         
            rateLimitOK = self.rateLimits.update("REQUESTS", 1)
            
            if rateLimitOK:                
                if symbol:
                    payload = {'symbol':symbol}
                    r = requests.get(self.URL + '/api/v3/ticker/bookTicker', params = payload)   
                else:
                    r = requests.get(self.URL + '/api/v3/ticker/bookTicker')       
                    
                if r.status_code == 429:
                    logger.info('Rate limit violation (429 error) received from Binance')
                elif str(r.status_code)[0] == '4' or str(r.status_code)[0] == '5': 
                    logger.debug('Return code error ' + str(r.status_code))
                elif r.status_code == 200:
                    #parse quotes
                    res = r.json()     
                    d2 = {}
                    if not isinstance(res, list):
                        res = [res]
                    for q in res:
                        symbolTemp, d = processQuote(q)
                        d2[symbolTemp] = d
                        
                    self.lock.acquire() 
                    self.OrderBookQuotes.update(d2)
                    self.lock.release()
                
            time.sleep(sleepAmount)
            
    def _prepareAuthRequest(self, URL, params, reqType = 'POST'):
        #generate signature as HMAC of queryString and secretkey
        req = requests.Request(reqType, URL, params = params)
        r = req.prepare()
        queryString = r.path_url.split('?')[1]
        
        #generate signature
        dig = hmac.new(self.Secret.encode('utf-8'), msg=queryString.encode('utf-8'), digestmod=hashlib.sha256).digest()
        signature = dig.hex()
        
        #append signature to to query string
        queryString += "&signature=" + signature
        
        req = requests.Request(reqType, URL + '?' + queryString, headers= {'X-MBX-APIKEY': self.APIKey})
        r = req.prepare()
        return r

    def _applyFilters(self, price, quantity, symbol):
        """Applies the binance order placement filters and returns price, quantity."""
        filters = self.symbolExchInfo[symbol]['filters']
        
        #Price Filters
        priceF = [x for x in filters if x['filterType'] == 'PRICE_FILTER'][0]
        #if not (price >= float(priceF['minPrice']) and price <= float(priceF['maxPrice'])): 
        #    logger.error('filter error for price')
        #    return None, None
        a = float(priceF['tickSize'])
        pricePrecision = int(round(math.log(int(1/a), 10), 0))
        price = round(price, pricePrecision)
        
        #Lot size filters
        lotF = [x for x in filters if x['filterType'] == 'LOT_SIZE'][0]
        if not (quantity >= float(lotF['minQty']) and quantity <= float(lotF['maxQty'])):
            logger.error('filter error for quantity')
            return None, None        
        a = float(lotF['stepSize'])
        lotPrecision = int(round(math.log(int(1/a), 10), 0))
        quantity = round(quantity, lotPrecision)
        
        #min notional 
        notF =  [x for x in filters if x['filterType'] == 'MIN_NOTIONAL'][0]
        if price > 0 and price * quantity < float(notF['minNotional']):
            logger.error('filter error; min notional is too low')
            return None, None
        
        return price, quantity

if __name__ == "__main__":
    binance = binance()
    
    st = datetime.datetime.now().timestamp()
    day = 60 * 60 * 24 
    et = datetime.datetime.now().timestamp() - day * 10
    #r = binance.getFundingRateHistory("BTCUSDT", st, et)
    r = binance.getFundingRateHistory("BTCUSDT")
    
    import pprint
    pprint.pprint(r)
    
    #r = binance.getmarketsummaries()
    #import pprint
    #symbols = [v['symbol'] for v in r]
    #pprint.pprint(symbols)
    #raise ValueError
    
    #blnContinue = True
    #while blnContinue:
        #balances = binance.getAccountBalances()
        #BCCBalance = float([x['free'] for x in balances if x['asset'] == 'BCC'][0])
        #print("BCC Balance is ", BCCBalance)
        #bids, asks = binance.getOrderBook(symbol = 'BCCBTC')
        #price, amt = bids[0][0], bids[0][1]        
        #minOrderQty = max(binance.getInfoOnSymbol('BCCBTC')['minQty'], binance.getInfoOnSymbol('BCCBTC')['minNotional'] * price)
        #if BCCBalance > minOrderQty:
            #amt = min(amt, BCCBalance)
            #if amt > minOrderQty:
                #r = binance.placeOrder('BCCBTC', 'SELL', 'LIMIT', amt, price)
                #time.sleep(2)
                
                #r = binance.getOpenOrders('BCCBTC')
                #if r:
                    ##there is an open order
                    #time.sleep(58)
                    #r = binance.getOpenOrders('BCCBTC')
                    #for v in r:
                        #binance.cancelOrder(v['clientOrderId'])
        #else:
            #blnContinue = False
        #time.sleep(10)
        

    
    
    
    #Sell all of my bitcoin cash!
    
    
    #r = binance.startOBQuoteFeedREST(requestPerMinute=4)
    #bids, asks = binance.getOrderBook(symbol = 'BCCBTC')
    #price = bids[0][0]
    #r= binance.placeOrder('BCCBTC', 'SELL', 'LIMIT', 1, bids[0][0] + 0.005 )
    #clientOrderId = r['clientOrderId']
    
    
    #while 1:
        #orderStatus = binance.queryOrder(clientOrderId)
        #print(orderStatus)
        #if getTimeStamp() - orderStatus['time']/1000 > 60:
            #binance.cancelOrder(clientOrderId)
        
        
        #time.sleep(30)
    #pass
