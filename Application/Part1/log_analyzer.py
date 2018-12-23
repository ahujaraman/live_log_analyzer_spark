from pyspark import SparkContext, SparkConf

import apache_access_log
import sys
import matplotlib.pyplot as plt
import numpy as np
'''
conf = SparkConf().setAppName("Log Analyzer")
sc = SparkContext(conf=conf)

logFile = 'C:\\Users\\Dell\\Documents\\live_log_analyzer_spark\\apache_log_example.log'

access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())


# Calculate statistics based on the content size.
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print ("Content Size Avg: %i, Min: %i, Max: %s" % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()
    ))

# Response Code to Count
responseCodeToCount = (access_logs.map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print ("Response Code Counts: %s" % (responseCodeToCount))

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (access_logs
               .map(lambda log: (log.ip_address, 1))
               .reduceByKey(lambda a, b : a + b)
               .filter(lambda s: s[1] > 10)
               .map(lambda s: s[0])
               .take(100))
print ("IpAddresses that have accessed more then 10 times: %s" % (ipAddresses))

# Top Endpoints
topEndpoints = (access_logs
                .map(lambda log: (log.endpoint, 1))
                .reduceByKey(lambda a, b : a + b)
                .takeOrdered(10, lambda s: -1 * s[1]))
print ("Top Endpoints: %s" % (topEndpoints))

'''


# Response Code to Count
responseCodeToCount = (access_logs.map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print ("Response Code Counts: %s" % (responseCodeToCount))

print(type(responseCodeToCount))
response_codes = []
response_code_counts = []
for x in responseCodeToCount:
    response_codes.append(x[0])
    response_code_counts.append(x[1])
# Showing the Data Visual Analytics of the Data
print(response_codes)
print(response_code_counts)
plt.bar(response_codes,response_code_counts)
plt.xticks(np.arange(len(response_codes)),response_codes,fontsize=5)
plt.xlabel('Number of Responses')
plt.ylabel('Response Code')
plt.title('Response Code Analysis')
plt.show()