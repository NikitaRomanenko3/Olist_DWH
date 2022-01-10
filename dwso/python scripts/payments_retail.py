#!/usr/bin/env python
# coding: utf-8

# In[146]:


import pandas as pd
import random


# In[147]:


# required data for fact table are stored into 3 csv files
# extracting and concatenating required data 

df_payments = pd.read_csv('olist_order_payments_dataset.csv')
df_payments = df_payments[['order_id', 'payment_type', 'payment_value']]

df_order_items = pd.read_csv('olist_order_items_dataset.csv')
df_order_items = df_order_items[['order_id', 'product_id', 'seller_id']]

df_orders = pd.read_csv('olist_orders_dataset.csv')
df_orders = df_orders[['order_id', 'customer_id', 'order_purchase_timestamp']]


df_payments = df_payments.merge(df_order_items, how='left', on='order_id')
df_payments = df_payments.merge(df_orders, how='left', on='order_id')

# adding margin rate of category to product
df_product_temp = pd.read_csv('products.csv')
df_product_temp = df_product_temp[['product_id', 'product_category_margin_rate']]
df_payments = df_payments.merge(df_product_temp, how='left', on='product_id')

# adding cost value to main dataframe
df_payments['cost_value'] = round(df_payments['payment_value'] / df_payments['product_category_margin_rate'], 2)

# convert datetime of purchase to date
df_payments['order_purchase_timestamp'] = pd.to_datetime(df_payments['order_purchase_timestamp']).dt.date

# adding logistic partners to main dataframe
df_logistic_partners = pd.read_csv('logistic partners.csv')
lp_id = list(df_logistic_partners['id'])
df_payments['logistic_partner_id'] = df_payments['order_id'].apply(lambda x: random.choice(lp_id))

# generating rows to main dataframe
for i in range(3):
    temp_df = df_payments.copy()
    temp_df['payment_value'] = temp_df['payment_value'].apply(lambda x: round(x / 1.10,2))
    temp_df['cost_value'] = temp_df['cost_value'].apply(lambda x: round(x / 1.10,2))
    df_payments = pd.concat([df_payments, temp_df])
    
# writing result to csv file
df_payments.reset_index(drop=True, inplace=True)
df_payments = df_payments[:800000]
df_payments = df_payments[['order_id', 'payment_type', 'product_id', 'customer_id', 'seller_id', 'logistic_partner_id',
                          'payment_value', 'cost_value', 'order_purchase_timestamp']]

df_payments.rename({'seller_id': 'supplier_id', 'order_purchase_timestamp': 'purchase_date'}, axis=1, inplace=True)
df_payments.to_csv('payments_retail.csv', index=False)


# In[ ]:




