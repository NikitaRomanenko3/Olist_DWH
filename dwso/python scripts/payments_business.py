#!/usr/bin/env python
# coding: utf-8

# In[22]:


import pandas as pd
import random


# In[23]:


# extracting 200k rows from retail payments 
temp_df = pd.read_csv('payments_retail.csv')
temp_df = temp_df[0: 200000]

# assigning companies id
df_companies = pd.read_csv('customers_business.csv')
companies_l = list(df_companies['customer_id'].unique())
temp_df['customer_id'] = temp_df['customer_id'].apply(lambda x: random.choice(companies_l))

# writing result to csv file
temp_df.to_csv('payments_business.csv', index=False)


# In[ ]:




