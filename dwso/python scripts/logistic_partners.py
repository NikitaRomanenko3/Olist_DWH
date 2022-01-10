#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd


# In[24]:


# loading df from result of geo.py
df_geo = pd.read_csv('olist_geo.csv')

# extracting cities id
df_geo = df_geo[['city_id']][:5]

# data of logistic partners 
data = {'id': [1, 2, 3, 4, 5], 'name': ['Logistic partner 1', 'Logistic partner 2', 'Logistic partner 3', 'Logistic partner 4',
                                       'Logistic partner 5']}

# creating dataframe using dictionary data
df_lp = pd.DataFrame(data)

# creating column city_id using df_geo
df_lp['city_id'] = df_geo['city_id']

# writing result to csv file
df_lp.to_csv('logistic partners.csv', index=False)


# In[ ]:




