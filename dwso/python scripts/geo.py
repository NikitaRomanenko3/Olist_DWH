#!/usr/bin/env python
# coding: utf-8

# In[142]:


import pandas as pd


# In[143]:


df_geo_customers = pd.read_csv('olist_customers_dataset.csv')

# get unique combinations of cities and states values from olist_customers_dataset.csv
df_geo_customers_city_state = df_geo_customers.groupby(['customer_city', 'customer_state']).size().reset_index()[['customer_city', 'customer_state']].copy()
df_geo_customers_city_state.rename({'customer_city': 'city', 'customer_state': 'state'}, axis=1, inplace=True)


df_geo_suppliers = pd.read_csv('olist_suppliers_dataset.csv')

# get unique combinations of cities and states values from olist_suppliers_dataset.csv
df_geo_suppliers_city_state = df_geo_suppliers.groupby(['seller_city', 'seller_state']).size().reset_index()[['seller_city', 'seller_state']].copy()
df_geo_suppliers_city_state.rename({'seller_city': 'city', 'seller_state': 'state'}, axis=1, inplace=True)


# concatination of two dataframes 
df_city_state_temp = pd.concat([df_geo_customers_city_state, df_geo_suppliers_city_state], ignore_index=True)

# getting result of combination unique values of cities and states from olist_customers_dataset.csv
# and olist_suppliers_dataset.csv
df_city_state_temp = df_city_state_temp.groupby(['city', 'state']).size().reset_index()[['city', 'state']].copy()

# adding city id attribute to dataframe
df_city_state_temp.index += 1 
df_city_state_temp['city_id'] = df_city_state_temp.index

# adding state id attribute to dataframe, which contain unique state values
df_state_id_temp = pd.DataFrame(df_city_state_temp['state'].unique())
df_state_id_temp.index += 1
df_state_id_temp.rename({0: 'state'}, axis=1, inplace=True)
df_state_id_temp['state_id'] = df_state_id_temp.index

# adding state id attribute to main dataframe
df_geo = df_city_state_temp.merge(df_state_id_temp, how='left', left_on = 'state', right_on = 'state')

# adding country and country id
df_geo['country_name'] = 'Brazil'
df_geo['country_id'] = 1
df_geo.rename({'state': 'state_name', 'city': 'city_name'}, axis=1, inplace=True)

# setting required order of columns
df_geo = df_geo[['city_id', 'city_name', 'state_id', 'state_name', 'country_id', 'country_name']]

# writing result to csv
df_geo.to_csv('olist_geo.csv', index=False)


# In[ ]:




