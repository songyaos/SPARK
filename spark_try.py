import sys
from stocktick import StockTick
from pyspark import SparkContext

def maxValuesReduce(a, b):
   ### TODO: Return a StockTick object with the maximum value between a and b for each one of its
   ### four fields (price, bid, ask, units)
   price = a.price if a.price > b.price else b.price
   bid = a.bid if a.bid > b.bid else b.bid
   ask = a.ask if a.ask > b.ask else b.ask
   units= a.units if a.units > b.units else b.units
   return StockTick("maxvaluereduce," +"maxvaluereduce,"+ str(price)+',' +str(bid)+ ','+ str(ask)+ ',' +str(units))

def minValuesReduce(a, b):
   ### TODO: Return a StockTick object with the minimum value between a and b for each one of its
   ### four fields (price, bid, ask, units)
   price = a.price if a.price < b.price else b.price
   bid = a.bid if a.bid < b.bid else b.bid
   ask = a.ask if a.ask < b.ask else b.ask
   units= a.units if a.units < b.units else b.units
   return StockTick("minvaluereduce," +"minvaluereduce,"+ str(price)+',' +str(bid)+ ','+ str(ask)+ ',' +str(units))

def generateSpreadsDailyKeys(tick):  ### TODO: Write Me (see below)
   spread = (tick.ask - tick.bid)/(2*(tick.ask + tick.bid))
   tickdate = tick.date.split('/')
   tickdate=[tickdate[-1], tickdate[0],tickdate[1]]
   return ('-'.join(tickdate),[tick.time,spread,1])
   
def generateSpreadsHourlyKeys(tick): ### TODO: Write Me (see below)
   spread = (tick.ask - tick.bid)/(2*(tick.ask + tick.bid))
   tickhour = tick.time.split(':')[0]
   tickdate = tick.date.split('/')
   tickfinal= str(tickdate[-1])+'-'+ str(tickdate[0])+'-'+str(tickdate[1])+':'+tickhour  
   return (tickfinal,[tick.time,spread,1])

def spreadsSumReduce(a, b):          ### TODO: Write Me (see below)
   if a[0]==b[0]:
		#print 'equal case'
		return [b[0],a[1], a[2]]
   else:
		#print a[0]+'-----'+b[0] + 'not equal case'
		return [b[0],a[1]+b[1], a[2]+b[2]]
		
if __name__ == "__main__":
   """
   Usage: stock
   """
   print '*****************************start new run *****************************'
   print '**********************************************************'
   print '********************************************************'
   print '********************************************************'
   print '*************************** *****************************'
   sc = SparkContext(appName="StockTick")

   # rawTickData is a Resilient Distributed Dataset (RDD)
   rawTickData = sc.textFile("/cs/bigdata/datasets/WDC_tickbidask_short.txt")

   ### TODO: use map to convert each line into a StockTick object
   tickData =  rawTickData.map(lambda line: StockTick(line, line.split(",")))

   print '*****************************debug output *****************************'
   print 'Debug: converting to TickData finished..............'
   print '*****************************debug output *****************************'
   #for item in tickData:
#	   print item
   ### TODO: use filter to only keep records for which all fields are > 0
   goodTicks = tickData.filter(lambda tickentry: tickentry.date!="" and tickentry.time!="" and tickentry.price>0 and tickentry.bid>0 and tickentry.ask>0 and tickentry.units>0)
   goodTicks.cache()
   
   print '*****************************debug output *****************************'
   print 'Debug: selecting good ticks finished.............. persisted'
   print '*****************************debug output *****************************'
   #for item in goodTicks:
   	#	print item
   
   numTicks = goodTicks.count()
   print '*****************************debug output *****************************'
   print numTicks
   print '*****************************debug output *****************************'
   
   sumValues = goodTicks.reduce(lambda a,b: a+b).collect()
   print '*****************************debug output *****************************'
   print 'sum units: ' + str(sumValues.units) + ',sum price: '  + str(sumValues.price)
   print '*****************************debug output *****************************'
   
   maxValuesReduce = goodTicks.reduce(maxValuesReduce) ### TODO: write the maxValuesReduce function
   minValuesReduce = goodTicks.reduce(minValuesReduce) ### TODO: write the minValuesReduce function
   print '*****************************debug output *****************************'
   print 'max value reduce: ' + str(maxValuesReduce) 
   print 'mean value reduce: '  + str(minValuesReduce)
   print '*****************************debug output *****************************'
   avgUnits = sumValues.units / float(numTicks)
   avgPrice = sumValues.price / float(numTicks)

   print "Max units %i, avg units %f\n" % (maxValuesReduce.units, avgUnits)
   print "Max price %f, min price %f, avg price %f\n" % (maxValuesReduce.price, minValuesReduce.price, avgPrice)

   #avgDailySpreads = goodTicks.map(generateSpreadsDailyKeys).reduceByKey(spreadsSumReduce);   # (1)
   #avgDailySpreads = avgDailySpreads.map(lambda keyvalue: (keyvalue[0],keyvalue[1][1]/keyvalue[1][2]))                    # (2)
   #avgDailySpreads = avgDailySpreads.sortByKey().map(lambda keyvalue: str(keyvalue[0]) + ', ' + str(keyvalue[1]))   # (3)
   #avgDailySpreads = avgDailySpreads.saveAsTextFile("WDC_daily")                              # (4)
   #print '*****************************debug output *****************************'
   #print 'DAILY SPREADS COMPUTATION FINISHED......................................'
   #print '*****************************debug output *****************************'
   #avgHourlySpreads = goodTicks.map(generateSpreadsHourlyKeys).reduceByKey(spreadsSumReduce); # (1)
   #avgHourlySpreads = avgHourlySpreads.map(lambda keyvalue: (keyvalue[0],keyvalue[1][1]/keyvalue[1][2]))                                    # (2)
   #avgHourlySpreads = avgHourlySpreads.sortByKey().map(lambda keyvalue: str(keyvalue[0]) + ', ' + str(keyvalue[1]))                        # (3)
   #avgHourlySpreads = avgHourlySpreads.saveAsTextFile("WDC_hourly")                           # (4)
   
   sc.stop()

