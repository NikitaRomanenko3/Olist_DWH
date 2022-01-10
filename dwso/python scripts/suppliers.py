#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


df_suppliers = pd.read_csv('olist_sellers_dataset.csv')
df_companies = pd.read_csv('ASX_Listed_Companies_11-07-2021_08-14-22_AEST.csv')


# In[3]:


# set sellers join id as row number id
df_suppliers['seller_join_id'] = df_suppliers.index


# In[4]:


df_companies.rename({'GICs industry group': 'Sector'}, axis=1, inplace=True)


# In[5]:


# Generating 1100 new companies to initial data set

df_companies_temp = df_companies[:1100].copy()
df_companies_temp['Company name'] = df_companies_temp['Company name'].apply(lambda x: str(x) + ' UNITED')

# concatenating two dataframes: initial and temporary
df_companies_result = pd.concat([df_companies, df_companies_temp], ignore_index=True)
df_companies_result['Company_id'] = df_companies_result.index

# join company names and other attributes to df_suppliers
df_result = df_suppliers.merge(df_companies_result, how='left', left_on='seller_join_id', right_on='Company_id')

# drop unnecessary column  
df_result.drop(['seller_zip_code_prefix', 'ASX code', 'Listing date', 'Market Cap', 'Company_id', 'seller_join_id'], axis=1, inplace=True)
df_result.rename({'seller_id': 'supplier_id'}, axis=1, inplace=True)

# write result file to csv
df_result.to_csv('olist_suppliers_dataset.csv', index=False)


# In[ ]:





# In[ ]:




