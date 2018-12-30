from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import apache_access_log
import analysis_plots
import sys


conf = SparkConf().setAppName("Log Analyzer SQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

logFile = 'C:\\Users\\Dell\\Documents\\live_log_analyzer_spark\\apache_log_example.log'


access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

schema_access_logs = sqlContext.createDataFrame(access_logs)
schema_access_logs.registerTempTable("logs")


#Top 10 Endpoints which Transfer Maximum Content
topEndpointsMaxSize = (sqlContext
                .sql("SELECT endpoint,content_size/1024 FROM logs ORDER BY content_size DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())

bar_plot_list_of_tuples_horizontal(topEndpointsMaxSize,'Data Flow - MB','Enpoints','Endpoint Analysis based on Max Content Size')



responseCodeToCount = (sqlContext
                       .sql("SELECT response_code, COUNT(*) AS theCount FROM logs GROUP BY response_code")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
bar_plot_list_of_tuples(responseCodeToCount,'Response Codes','Number of Codes','Response Code Analysis')

# Most Frequent Visitors (Most Frequent IP Address visits).
frequentIpAddressesHits = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: (row[0], row[1]))
               .collect())
bar_plot_list_of_tuples_horizontal(frequentIpAddressesHits,'Number of Hits','IP Address','Most Frequent Visitors (Frequent IP Address Hits)')

topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
bar_plot_list_of_tuples_horizontal(topEndpoints,'Number of Times Accessed','End Points','Most Frequent Endpoints')


Day = '07/Mar/2004'
trafficperDay = (sqlContext
                       .sql("SELECT time,content_size/1024 FROM logs where date='08/Mar/2004'")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
time_series_plot(trafficperDay,Day,'Content Size - MB','Traffic Analysis/Day')



'''
trafficWithTime = (sqlContext
                       .sql("SELECT date_time, content_size/1024 FROM logs")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
print ("Traffic with time: %s" % (trafficWithTime))
time_series_plot(trafficWithTime)

trafficWithTime = (sqlContext
                       .sql("SELECT date_time, content_size/1024 FROM logs")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
print ("Traffic with time: %s" % (trafficWithTime))
#time_series_plot(trafficWithTime)


#Top 10 404 requests with there endpoints and time
NotFoundRequests = (sqlContext
                .sql("SELECT endpoint,date_time FROM logs where response_code='404' ORDER BY date_time DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
bar_plot_list_of_tuples(NotFoundRequests,'Bad Request','Date-Time','404 Bad Request Analysis')


# Calculate statistics based on the content size.            
content_size_stats = (sqlContext
                      .sql("SELECT SUM(content_size),COUNT(*),MIN(content_size), MAX(content_size) FROM logs")
                      .first())                     
print( "Content Size Avg: %i, Min: %i, Max: %s" % (
    content_size_stats[0] / content_size_stats[1],
    content_size_stats[2],
    content_size_stats[3]
))
content_analysis_plot(content_list)

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: row[0])
               .collect())
print ("All IPAddresses > 10 times: %s" % ipAddresses)

# Top Endpoints
topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
print ("Top Endpoints: %s" % (topEndpoints))
'''