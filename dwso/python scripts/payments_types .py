#!/usr/bin/env python
# coding: utf-8

# In[22]:


import pandas as pd


# In[23]:


df_payments = pd.read_csv('olist_order_payments_dataset.csv')

# get unique values of payments types 
df_payments_types = pd.DataFrame(df_payments['payment_type'].unique())

# adding payments types id
df_payments_types.rename({0: 'payment_type'},axis=1, inplace=True)
df_payments_types.index += 1
df_payments_types['payment_type_id'] = df_payments_types.index
df_payments_types = df_payments_types[['payment_type_id', 'payment_type']]

# writing result to csv
df_payments_types.to_csv('payments_types.csv', index=False)


# In[ ]:




