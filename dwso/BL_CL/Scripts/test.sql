---------------------------------- CITIES --------------------------------------
---- testing of consistency

with test_result as (
select count(*) as result_ 
from sa_olist_geo sa
left join ce_cities ce on ce.city_srcid = sa.city_id
where ce.city_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_CITIES 
from test_result;

---- target table doesn’t contain duplicates

with test_result as (
select sa.city_id, count(*) as result_ 
from sa_olist_geo sa
left join ce_cities ce on ce.city_srcid = sa.city_id
group by sa.city_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_CITIES
from test_result;

----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_cities ce
left join dim_geolocations dim on dim.geolocation_city_id = ce.city_id
where dim.geolocation_city_id is null)

select case when result_ = 0 then 'All cities from ce layer are represented'
            else 'Not all cities from ce layer are represented'
            end as testing_of_consistency_DIM_GEOLOCATIONS 
from test_result;


---------------------------------- STATES --------------------------------------
---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_olist_geo sa
left join ce_states ce on ce.state_srcid = sa.state_id
where ce.state_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_STATES 
from test_result;


---- target table doesn’t contain duplicates
with test_result as (
select sa.state_id, sa.city_id, count(*) as result_ 
from sa_olist_geo sa
left join ce_states ce on ce.state_srcid = sa.state_id
group by sa.state_id, sa.city_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_STATES
from test_result;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_states ce
left join dim_geolocations dim on dim.geolocation_state_id = ce.state_id
where dim.geolocation_city_id is null)

select case when result_ = 0 then 'All states from ce layer are represented'
            else 'Not all states from ce layer are represented'
            end as testing_of_consistency_DIM_GEOLOCATIONS 
from test_result;


---------------------------------- COUNTRIES -----------------------------------
---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_olist_geo sa
left join ce_countries ce on ce.country_srcid = sa.country_id
where ce.country_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_COUNTRIES 
from test_result;

---- target table doesn’t contain duplicates
with test_result as (
select sa.country_id, sa.state_id, sa.city_id, count(*) as result_ 
from sa_olist_geo sa
left join ce_countries ce on ce.country_srcid = sa.country_id
group by sa.country_id, sa.state_id, sa.city_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_COUNTRIES
from test_result;

----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_countries ce
left join dim_geolocations dim on dim.geolocation_country_id = ce.country_id
where dim.geolocation_country_id is null)

select case when result_ = 0 then 'All countries from ce layer are represented'
            else 'Not all countries from ce layer are represented'
            end as testing_of_consistency_DIM_GEOLOCATIONS 
from test_result;

---------------------------------- CUSTOMERS -----------------------------------
---- testing of consistency

with sa_cust_id as (
select 
    customer_id
from sa_customers_retail

union all 

select 
    to_char(customer_id)
from sa_customers_business)

select 
    case 
    when count(*) = 0 then 'All rows from sa layer are represented'
    else 'Not all rows from sa layer are represented'
    end as testing_of_consistency_CUSTOMERS 
from sa_cust_id sa
left join ce_customers ce on ce.customer_srcid = sa.customer_id
where ce.customer_srcid is null;


---- target table doesn’t contain duplicates
with sa_cust_id as (
select 
    customer_id
from sa_customers_retail

union all 

select 
    to_char(customer_id)
from sa_customers_business)

select 
    count(*)
from sa_cust_id sa
left join ce_customers ce on ce.customer_srcid = sa.customer_id
group by sa.customer_id
having count(*) > 1;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_customers ce
left join dim_customers dim on dim.customer_surr_id = ce.customer_id
where dim.customer_surr_id is null and ce.customer_id <> -1)

select case when result_ = 0 then 'All customers from ce layer are represented'
            else 'Not all customers from ce layer are represented'
            end as testing_of_consistency_DIM_CUSTOMERS 
from test_result;


---------------------------------- LOGISTIC_PARTNERS ---------------------------
---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_logistic_partners sa
left join ce_logistic_partners ce on ce.logistic_partner_srcid = sa.logistic_partner_id
where ce.logistic_partner_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_LOGISTIC_PARTNERS 
from test_result;


---- target table doesn’t contain duplicates
with test_result as (
select sa.logistic_partner_id, count(*) as result_ 
from sa_logistic_partners sa
left join ce_logistic_partners ce on ce.logistic_partner_srcid = sa.logistic_partner_id
group by sa.logistic_partner_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_LOGISTIC_PARTNERS
from test_result;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_logistic_partners ce
left join dim_logistic_partners dim on dim.logistic_partners_id = ce.logistic_partner_id
where dim.logistic_partners_id is null)

select case when result_ = 0 then 'All logistic_partners from ce layer are represented'
            else 'Not all logistic_partners from ce layer are represented'
            end as testing_of_consistency_LOGISTIC_PARTNERS 
from test_result;


---------------------------------- PAYMENT_TYPES -------------------------------
---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_payments_types sa
left join ce_payment_types ce on ce.payment_type_srcid = sa.payment_type_id
where ce.payment_type_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_PAYMENT_TYPES 
from test_result;


---- target table doesn’t contain duplicates
with test_result as (
select sa.payment_type_id, count(*) as result_ 
from sa_payments_types sa
left join ce_payment_types ce on ce.payment_type_srcid = sa.payment_type_id
group by sa.payment_type_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_PAYMENT_TYPES
from test_result;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_payment_types ce
left join dim_payment_types dim on dim.payment_type_id = ce.payment_type_id
where dim.payment_type_id is null)

select case when result_ = 0 then 'All payment_types from ce layer are represented'
            else 'Not all payment_types from ce layer are represented'
            end as testing_of_consistency_payment_types 
from test_result;


---------------------------------- SUPPLIERS -----------------------------------

---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_olist_suppliers_dataset sa
left join ce_suppliers ce on ce.supplier_srcid = sa.supplier_id
where ce.supplier_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_CE_SUPPLIERS 
from test_result;


---- target table doesn’t contain duplicates
with test_result as (
select sa.supplier_id, count(*) as result_ 
from sa_olist_suppliers_dataset sa
left join ce_suppliers ce on ce.supplier_srcid = sa.supplier_id
group by sa.supplier_id
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_CE_SUPPLIERS 
from test_result;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_suppliers ce
left join dim_suppliers dim on dim.supplier_id = ce.supplier_id
where dim.supplier_id is null)

select case when result_ = 0 then 'All suppliers from ce layer are represented'
            else 'Not all suppliers from ce layer are represented'
            end as testing_of_consistency_dim_suppliers 
from test_result;


---------------------------------- PRODUCTS_SCD --------------------------------
---- testing of consistency
with test_result as (
select count(*) as result_ 
from sa_products sa
left join ce_products_scd ce1 on ce1.product_srcid = sa.product_id
left join ce_products_categories_scd ce2 on ce2.product_category_srcid = sa.product_category_id
where ce1.product_srcid is null and ce2.product_category_srcid is null)

select case when result_ = 0 then 'All rows from sa layer are represented'
            else 'Not all rows from sa layer are represented'
            end as testing_of_consistency_PRODUCTS_SCD 
from test_result;


---- target table doesn’t contain duplicates
with test_result as (
select ce2.product_category_srcid, ce1.product_srcid, ce2.start_dt, ce1.start_dt, count(*) as result_ 
from sa_products sa
left join ce_products_scd ce1 on ce1.product_srcid = sa.product_id --and ce1.is_active = 'Y'
left join ce_products_categories_scd ce2 on ce2.product_category_srcid = sa.product_category_id --and ce2.is_active = 'Y'
group by ce2.product_category_srcid, ce1.product_srcid, ce2.start_dt, ce1.start_dt
having count(*) > 1)

select case when count(*) = 0 then 'There are no duplicates into sa layer'
            else 'There are duplicates into sa layer'
            end testing_of_containing_duplicates_PRODUCTS_SCD 
from test_result;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_products_scd ce1
join ce_products_categories_scd ce2 on ce2.product_category_srcid = ce1.product_category_id
left join dim_products_scd dim on dim.product_id = ce1.product_id and dim.product_category_id = ce2.product_category_id and dim.start_dt = ce1.start_dt
where dim.product_id is null)

select case when result_ = 0 then 'All products from ce layer are represented'
            else 'Not all products from ce layer are represented'
            end as testing_of_consistency_PRODUCTS_SCD 
from test_result;


---------------------------------- SALES ---------------------------------------
---- testing of consistency
with sa_order_id as (
select 
    order_id
from sa_sales sa
left join ce_sales ce on ce.sale_srcid = sa.order_id and ce.source_table = 'SA_SALES'
where ce.sale_srcid is null

union all 

select 
    order_id
from sa_sales_business sa
left join ce_sales ce on ce.sale_srcid = sa.order_id and ce.source_table = 'SA_SALES_BUSINESS'
where ce.sale_srcid is null)

select 
    case 
    when count(*) = 0 then 'All rows from sa layer are represented'
    else 'Not all rows from sa layer are represented'
    end as testing_of_consistency_CE_SALES 
from sa_order_id sa;



---- target table doesn’t contain duplicates
with sa_order_id as (
select 
    sa.order_id, ce.source_table
from sa_sales sa
left join ce_sales ce on ce.sale_srcid = sa.order_id and ce.source_table = 'SA_SALES'
where ce.sale_srcid is not null

union all 

select 
    sa.order_id, ce.source_table
from sa_sales_business sa
left join ce_sales ce on ce.sale_srcid = sa.order_id and ce.source_table = 'SA_SALES_BUSINESS'
where ce.sale_srcid is not null)

select 
    count(*)
from sa_order_id sa
group by sa.order_id, sa.source_table
having count(*) > 1;


----  consistency of DIM layer
with test_result as (
select count(*) as result_
from ce_sales ce
left join fct_sales_dd fct on fct.order_id = ce.sale_id
where fct.order_id is null and ce.sale_id <> -1)

select case when result_ = 0 then 'All facts from ce layer are represented'
            else 'Not all facts from ce layer are represented'
            end as testing_of_consistency_FCT_SALES_DD 
from test_result;


