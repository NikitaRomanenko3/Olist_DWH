#!/usr/bin/env python
# coding: utf-8

# In[21]:


import pandas as pd


# In[22]:


df_customers = pd.read_csv('olist_customers_dataset.csv')
df_customers = df_customers[:1000] 

# adding company id and name to dataframe
df_customers.index += 1
df_customers['customer_id'] = df_customers.index
df_customers['company_name'] = df_customers['customer_id'].apply(lambda x: 'Company ' + str(x))

# writing result to csv file:
df_customers = df_customers[['customer_id', 'company_name', 'customer_city', 'customer_state']]
df_customers.to_csv('customers_business.csv', index=False)


# In[ ]:




