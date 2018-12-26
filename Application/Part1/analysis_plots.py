# -*- coding: utf-8 -*-
"""
Created on Sun Dec 23 21:18:21 2018

@author: Dell
"""

import matplotlib.pyplot as plt

def response_code_analysis_plot(responseCodeToCount):
    response_codes = []
    response_code_counts = []
    response_numbers = []
    count = 1
    for x in responseCodeToCount:
        response_codes.append(x[0])
        response_code_counts.append(x[1])
        response_numbers.append(count)
        count = count+1
# Showing the Data Visual Analytics of the Data
    plt.bar(response_numbers,response_code_counts)
    plt.xticks(response_numbers,response_codes,fontsize=12)
    plt.xlabel('Number of Responses')
    plt.ylabel('Response Code')
    plt.title('Response Code Analysis')
    plt.show()


# Analysis on content    
def content_analysis_plot(content_size):
    counts = [1,2,3]
    LABELS = ["Average Size","Min Size","Max Size"]
    plt.bar(counts,content_size)
    plt.xticks(counts,LABELS)
    plt.show()
    
    