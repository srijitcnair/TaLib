import pandas as pd
from os import path

import pandas_datareader as dr

sp500MemberListFile = 'data/sp500_constituents.csv'
sp500MemberListFundamentalFile = 'data/sp500_constituents_financials.csv'
sp500PickleLocation = 'data/sp500data.pkl'
symbolPriceDataLocation = 'data/symbol/'


def getDailyPrice(tickers, start, end):
    print('Fetching data for {}'.format(tickers))
    return dr.get_data_yahoo(tickers, start, end)['Adj Close']

def getDailyPriceFull(tickers, start, end):
    print('Fetching data for {}'.format(tickers))
    return dr.get_data_yahoo(tickers, start, end)

def readSavedPriceData(ticker):
    return pd.read_pickle('{}{}.pkl'.format(symbolPriceDataLocation, ticker))


def readSavedPriceDataForTickers(tickerList):
    retDF = pd.DataFrame()
    for i in range(len(tickerList)):
        tickerPrice = readSavedPriceData(tickerList[i]).reset_index()
        if (i == 0):
            retDF = tickerPrice
        else:
            retDF = retDF.merge(tickerPrice, on='Date', how='outer')
    retDF = retDF.set_index('Date')
    return retDF

def getPreprocessedSP500Data():
    sp500Fundamentals = pd.read_csv(sp500MemberListFundamentalFile)
    return preprocessSP500Fundamentals(sp500Fundamentals)

def getSP500Details(start, end):
    sp500DF = pd.DataFrame()
    #sp500ComponentDf = pd.read_csv(sp500MemberListFile)
    sp500ComponentDf = pd.read_csv(sp500MemberListFundamentalFile)
    sp500SymbolList = sp500ComponentDf['symbol'].tolist()

    successCount = 0
    for symbol in sp500SymbolList:
        try:
            tickerQuote = dr.get_quote_yahoo(symbol)
            getDailyPrice([symbol], start, end).to_pickle('{}{}.pkl'.format(symbolPriceDataLocation, symbol))
            sp500DF = sp500DF.append(tickerQuote, sort=False)
            successCount += 1
        except Exception as e:
            print('Error {} for symbol {}'.format(e, symbol))

    print('Successfully retrieved data for {} stocks'.format(successCount))
    print(sp500DF)
    sp500DF.to_pickle(sp500PickleLocation)


def readSP500Data(requiredDataFields):
    sp500DFFromPickle = pd.read_pickle(sp500PickleLocation)
    """
    Columnns are : 'language', 'region', 'quoteType', 'triggerable', 'quoteSourceName',
       'currency', 'priceHint', 'sharesOutstanding', 'bookValue',
       'fiftyDayAverage', 'fiftyDayAverageChange',
       'fiftyDayAverageChangePercent', 'twoHundredDayAverage',
       'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent',
       'marketCap', 'forwardPE', 'priceToBook', 'sourceInterval',
       'exchangeDataDelayedBy', 'tradeable', 'exchange', 'shortName',
       'longName', 'messageBoardId', 'exchangeTimezoneName',
       'exchangeTimezoneShortName', 'gmtOffSetMilliseconds', 'market',
       'esgPopulated', 'firstTradeDateMilliseconds', 'postMarketChangePercent',
       'postMarketTime', 'postMarketPrice', 'postMarketChange',
       'regularMarketChange', 'regularMarketChangePercent',
       'regularMarketTime', 'regularMarketPrice', 'regularMarketDayHigh',
       'regularMarketDayRange', 'regularMarketDayLow', 'regularMarketVolume',
       'regularMarketPreviousClose', 'bid', 'ask', 'bidSize', 'askSize',
       'fullExchangeName', 'financialCurrency', 'regularMarketOpen',
       'averageDailyVolume3Month', 'averageDailyVolume10Day',
       'fiftyTwoWeekLowChange', 'fiftyTwoWeekLowChangePercent',
       'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange',
       'fiftyTwoWeekHighChangePercent', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
       'dividendDate', 'earningsTimestamp', 'earningsTimestampStart',
       'earningsTimestampEnd', 'trailingAnnualDividendRate', 'trailingPE',
       'trailingAnnualDividendYield', 'marketState', 'epsTrailingTwelveMonths',
       'epsForward', 'price'    
    """

    sp500DF = sp500DFFromPickle[requiredDataFields]
    sp500DF.index.name = 'symbol'
    sp500DF.reset_index(inplace=True)
    sp500DF = sp500DF.fillna(0)
    return sp500DF

def preprocessSP500Fundamentals(sp500Fundamentals):
    toKeep = [x for x in sp500Fundamentals['symbol'] if path.exists('{}{}.pkl'.format(symbolPriceDataLocation, x))]
    sp500Fundamentals = sp500Fundamentals[sp500Fundamentals['symbol'].isin(toKeep)]

    sectorEncoding = pd.get_dummies(sp500Fundamentals['sector'])
    df = sp500Fundamentals.join(sectorEncoding)
    #df = df.drop(['name','sector','price','52weeklow','52weekhigh','pricesalesratio','pricebookratio'],axis=1)
    df = df.drop(['name', 'sector', '52weeklow', '52weekhigh'], axis=1)
    colsToNormalize = ['price', 'peratio','dividendyield','marketcap','eps','ebitda','pricesalesratio','pricebookratio']
    for colToNormalize in colsToNormalize:
        df[colToNormalize] = df.loc[:, df.columns==colToNormalize].apply(lambda x: x / x.max())

    df.columns = [x.lower().replace(' ','_') for x in df.columns]
    #print(tabulate(df,tablefmt='psql',headers=df.columns))
    return df