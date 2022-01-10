#!/usr/bin/env python
# coding: utf-8

# In[93]:


import pandas as pd


# In[94]:


df_customers = pd.read_csv('olist_customers_dataset.csv')

# extracting unique values of customer id
df_customers_temp = pd.DataFrame(df_customers['customer_unique_id'].unique())
df_customers_temp.rename({0: 'customer_unique_id'}, axis=1, inplace=True)

# setting first and last names to customers
df_customers_temp.index += 1
df_customers_temp['customer_unique_id_num'] = df_customers_temp.index
df_customers_temp['first_name'] = df_customers_temp['customer_unique_id_num'].apply(lambda x: 'First name ' + str(x))
df_customers_temp['last_name'] = df_customers_temp['customer_unique_id_num'].apply(lambda x: 'Last name ' + str(x))

# adding first and last names to main dataframe
df_customers = df_customers.merge(df_customers_temp, how='left', on='customer_unique_id')

# adding numeric id to customers
df_customers.index += 1
df_customers['customer_id_num'] = df_customers.index

# writing result to csv file
df_customers = df_customers[['customer_id', 'customer_id_num', 'customer_unique_id', 'customer_unique_id_num', 'customer_city',
                             'customer_state','first_name', 'last_name']]

df_customers.to_csv('customers_retail.csv', index=False)


# In[ ]:




