# -*- coding: utf-8 -*-
"""
Created on Sun Dec 23 20:47:14 2018

@author: Dell
"""
import matplotlib.pyplot as plt
import numpy as np
response_codes = [1,2,3]
response_code_counts = [10,5,6]
Labels = [200,404,300]
print(response_codes)
print(response_code_counts)
plt.bar(response_codes,response_code_counts)
plt.xticks(response_numbers,Labels,fontsize=5)
plt.xlabel('Number of Responses')
plt.ylabel('Response Code')
plt.title('Response Code Analysis')
plt.show()