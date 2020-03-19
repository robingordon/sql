-----------------------------------------------------------------------------------
--
-- Filename      :       cdslm10020_fact_population.sql
-- Author        :       Robin gordon
-- Date Created  :       19 March 2020
--
-----------------------------------------------------------------------------------
--
-- Description   :       Update WH table FACT_ORDER_PRODUCT
--
-- Comments      :       N/A
--
-- Usage         :       Standard SQLPLUS Call
--
-- Called By     :       Manually
--
-- Calls         :       None
--
-- Parameters    :       N/A
--
-- Exit codes    :       0 - Success
--                       1 - Failure
--
-- Revisions
-- ================================================================================
-- Date       Userid  MR#         Comments                                      Ver.
-- ---------  ------  ---------   --------------------------------------------  ----
-- 1910320    rgo15   INC2490586  GCP Reflection of MIDAS change                1.0
-----------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------
-- Updating FACT_ORDER_PRODUCT
-----------------------------------------------------------------------------------
uk_pub_customer_is.fact_order_product:UPDATE:
update uk_pub_customer_is.fact_order_product a
   set a.account_number      = td.account_number,
       a.dw_last_modified_dt = current_timestamp
  from (select order_id,
               max(account_number) as account_number
          from uk_pre_customer_is.td_order_product_p3_1 
		  group by 1) td
 where a.order_id = td.order_id
   and a.account_number != td.account_number;
