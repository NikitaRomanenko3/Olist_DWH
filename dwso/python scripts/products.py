#!/usr/bin/env python
# coding: utf-8

# In[98]:


import pandas as pd
from random import uniform


# In[99]:


# main file with products
df_products = pd.read_csv('olist_products_dataset.csv')

# file contains translation of products categories
df_products_category_name = pd.read_csv('product_category_name_translation.csv')

df_products = df_products.merge(df_products_category_name, how='left', on='product_category_name')


# extracting unique values of product categories
df_category_temp = pd.DataFrame(df_products['product_category_name_english'].unique()).rename({0: 'product_category_name_english'}
                                                                                    , axis=1)
# setting product category id
df_category_temp.index += 1
df_category_temp['product_category_id'] = df_category_temp.index

# adding products categories margin rate
df_category_temp['product_category_margin_rate'] = df_category_temp['product_category_id'].apply(lambda x: round(uniform(1.1, 1.75),2))

# join product category id to main dataframe
df_products = df_products.merge(df_category_temp, how='left', on='product_category_name_english')

# adding numeric product id 
df_products.index += 1
df_products['product_id_num'] = df_products.index

# adding products names
df_products['product_name'] = df_products['product_id_num'].apply(lambda x: 'Product #' + str(x))

# extracting required columns
df_products = df_products[['product_id', 'product_id_num', 'product_name', 'product_category_id', 'product_category_name_english',
               'product_category_margin_rate', 'product_weight_g', 'product_length_cm', 'product_height_cm',
               'product_width_cm']]

# writing result to csv file
df_products.to_csv('products.csv', index=False)


# In[ ]:




