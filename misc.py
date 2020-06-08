import pytz, datetime, dateutil.parser,re, json
from urllib.request import urlopen

def countryIP():
    """Returns the IP and country code."""
    url = 'http://ipinfo.io/json'
    response = urlopen(url)
    data = json.load(response)
    
    IP=data['ip']
    org=data['org']
    city = data['city']
    country=data['country']
    region=data['region']
    
    return IP, country

def getTimeStamp():
    """Returns a UTC timestamp."""
    tzUTC = pytz.timezone("UTC")
    return datetime.datetime.now(tzUTC).timestamp()

def timestampToDate(ts):
    """Returns a datetime object for the ts - whichis assumed to be in UTC time."""
    utc_dt = datetime.datetime.utcfromtimestamp(ts)
    aware_utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    return aware_utc_dt

def stringDateToDT(tsStr):
    return dateutil.parser.parse(tsStr)

def tsToString(ts):
    d = timestampToDate(ts)
    s = d.strftime('%Y-%m-%d %H:%M:%SZ')
    return s

def dtToIso(dt):
    """returns the input datetime object as an ISO 8601 string."""
    return dt.isoformat()

if __name__ == "__main__":
    #a = stringDateToDT('2018-06-30T00:51:49.128Z')
    print(datetime.datetime.now())
    
    