--------------------------------------------------------------------------------
--
-- file-name    : cdslm10030.sql
-- author       : Ross MacLean
-- date created : 27th May 2015
--
--------------------------------------------------------------------------------
-- Description  : This script is the initial SQL to group the orderline data
--                together for the sales mart
--
-- Comments     : This replaces the old Sales Mart in EXN_DW_DB as part of CR27
--
-- Usage        : NZSQL Call
--
-- Called By    : CDSLM10000.scr
--
-- Calls        : None
--
-- Parameters   : N/A
--
-- Exit codes   : 0 - Success
--                1 - Failure
--
-- Revisions
-- =============================================================================
-- Date     user id  MR#       Comments                                     Ver.
-- ------   -------  ------    -------------------------------------------  ----
-- 270515   ROM19    CR27         Initial version                            1.0
-- 310316   ASU03    NOWTV        Changed to include NOWTV 2.0               1.1
-- 010616   FHU35    Movies       Amended for movies name change             1.2
-- 110816	NSA19	 Mobile	      Add column types 61 to 70					 1.3 
-- 210617   FHU35    Lima         Add column types 71 to 90                  1.4
-- 140917   CDN10    INC2078724   Fix on Fail                                1.5
-- 011018   AFS01    Netflix      Changes to TPESS 82, 83 and 84             1.6 
-- 250919   ANP52	 GCP          Update for migration to BQ 				 2.0
-- 170320   RGO15    INC2490586   Update to account_number population        2.1
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Populate td_order_product_p4 - This is the first step to aggregate all of the 
--order line data. We join out the CC_BSBORDER first to get the main order 
--attributes we need and we then group by these in the aggregates. For each 
--sale_type there will be an entry in REF_PROD_DIM the join out to 
--here is to allow the process to be dynamic. Each sale_type if wanted by the
--business will be added to the ref data and it will flow into the corresponding 
--fields for the type_x it is given. Separate logic for Premium 
--products and these are hard coded in the last 6 fields.
--------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p4:WRITE_TRUNCATE:
select o.id                                                   as order_id,
          o.ordernumber                                          as order_number,
          o.created                                              as order_created_dt,
          o.lastupdate                                           as order_last_updated_dt,
          o.createdby                                            as order_created_by,
          o.comtypecode                                          as order_communication_type,
          o.updatedby                                            as order_last_updated_by,
          o.partyroleid                                          as contactor_role,
          ol.portfolio_id                                        as portfolio_id,
          max(ol.account_number)                                 as account_number,
          max(ol.billing_account_id)                             as owning_cust_account_id,
          o.currency                                             as order_currency,
          max(ol.account_currency_code)                          as account_currency_code,
          max(ol.account_type_code)                              as account_type_code,
          max(ol.account_sub_type_code)                          as account_sub_type_code,
          o.diallednumber                                        as dialled_number,
          max(ol.interest_source)                                as interest_source_id,
          o.referringcustomerid                                  as referring_customer_party_id,
          o.urn                                                  as order_urn,
          max(ol.rpop_retailer_id)                               as sale_retailer_id,
          max(case 
               when ol.cat_prod_type_desc in ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') and ol.bol_action_type = 'ADD' then 
                  ol.technology_code 
              else 
               null 
              end)                                                as bb_technology_code,
          max(case 
                when ol.cat_prod_type_desc in ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL') and ol.bol_action_type = 'ADD' then 
                ol.technology_code 
              else 
                null 
              end)                                                as lr_technology_code,
          max(case 
                when ol.cat_prod_type_desc in ('SKY TALK SELECT','NOW_TV_2.0_TALK') and ol.bol_action_type = 'ADD' then 
                ol.technology_code 
              else 
                null 
              end)                                                as st_technology_code,
          o.status                                                as order_status,
          o.statuschangeddate                                     as order_status_start_dt,
          o.statusreasoncode                                      as order_status_reason_code,
          max(ol.systemfix_flag)                                  as order_system_fix,
          max(ol.salespeed)                                       as order_sale_speed,
          null                                                    as order_sale_type,
          max(ol.bundle_id)                                       as bundle_id,
          max(ol.telephony_order_type)                            as telephony_order_type,
          case 
            when ol.account_created_date = cast(o.created as date) then
               'Y' 
               else 
               'N' 
          end                                                    as new_customer_flag,
          case 
            when ol.account_created_date = cast(o.created as date) and row_number() over (partition by ol.portfolio_id 
                                                                    order by o.created asc) = 1 then 
            'Y' 
               else 
            'N' 
          end                                                    as first_order_flag,
          min(case 
            when rpd.column_name = 'type_1'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_1_saletype,
          sum(case 
            when rpd.column_name = 'type_1'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_1_adds,
          sum(case 
            when rpd.column_name = 'type_1'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_1_removes,
          max(case 
            when rpd.column_name = 'type_1'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_1_added_sk,
          max(case 
            when rpd.column_name = 'type_1'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_1_removed_sk,
          max(case 
             when rpd.column_name = 'type_1' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_1_add_sub,
          min(case 
            when rpd.column_name = 'type_2'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_2_saletype,
          sum(case 
            when rpd.column_name = 'type_2'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_2_adds,
          sum(case 
            when rpd.column_name = 'type_2'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_2_removes,
          max(case 
            when rpd.column_name = 'type_2'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_2_added_sk,
          max(case 
            when rpd.column_name = 'type_2'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_2_removed_sk,
          max(case 
             when rpd.column_name = 'type_2' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_2_add_sub,
          min(case 
            when rpd.column_name = 'type_3'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_3_saletype,
          sum(case 
            when rpd.column_name = 'type_3'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_3_adds,
          sum(case 
            when rpd.column_name = 'type_3'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_3_removes,
          max(case 
            when rpd.column_name = 'type_3'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_3_added_sk,
          max(case 
            when rpd.column_name = 'type_3'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_3_removed_sk,
          max(case 
             when rpd.column_name = 'type_3' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_3_add_sub,
          min(case 
            when rpd.column_name = 'type_4'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_4_saletype,
          sum(case 
            when rpd.column_name = 'type_4'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_4_adds,
          sum(case 
            when rpd.column_name = 'type_4'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_4_removes,
          max(case 
            when rpd.column_name = 'type_4'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_4_added_sk,
          max(case 
            when rpd.column_name = 'type_4'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_4_removed_sk,
          max(case 
             when rpd.column_name = 'type_4' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_4_add_sub,
          min(case 
            when rpd.column_name = 'type_5'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_5_saletype,
          sum(case 
            when rpd.column_name = 'type_5'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_5_adds,
          sum(case 
            when rpd.column_name = 'type_5'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_5_removes,
          max(case 
            when rpd.column_name = 'type_5'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_5_added_sk,
          max(case 
            when rpd.column_name = 'type_5'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_5_removed_sk,
          max(case 
             when rpd.column_name = 'type_5' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_5_add_sub,
          min(case 
            when rpd.column_name = 'type_6'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_6_saletype,
          sum(case 
            when rpd.column_name = 'type_6'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_6_adds,
          sum(case 
            when rpd.column_name = 'type_6'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_6_removes,
          max(case 
            when rpd.column_name = 'type_6'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_6_added_sk,
          max(case 
            when rpd.column_name = 'type_6'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_6_removed_sk,
          max(case 
             when rpd.column_name = 'type_6' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_6_add_sub,
          min(case 
            when rpd.column_name = 'type_7'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_7_saletype,
          sum(case 
            when rpd.column_name = 'type_7'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_7_adds,
          sum(case 
            when rpd.column_name = 'type_7'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_7_removes,
          max(case 
            when rpd.column_name = 'type_7'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_7_added_sk,
          max(case 
            when rpd.column_name = 'type_7'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_7_removed_sk,
          max(case 
             when rpd.column_name = 'type_7' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_7_add_sub,
                    min(case 
            when rpd.column_name = 'type_8'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_8_saletype,
          sum(case 
            when rpd.column_name = 'type_8'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_8_adds,
          sum(case 
            when rpd.column_name = 'type_8'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_8_removes,
          max(case 
            when rpd.column_name = 'type_8'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_8_added_sk,
          max(case 
            when rpd.column_name = 'type_8'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_8_removed_sk,
          max(case 
             when rpd.column_name = 'type_8' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_8_add_sub,
          min(case 
            when rpd.column_name = 'type_9'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_9_saletype,
          sum(case 
            when rpd.column_name = 'type_9'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_9_adds,
          sum(case 
            when rpd.column_name = 'type_9'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_9_removes,
          max(case 
            when rpd.column_name = 'type_9'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_9_added_sk,
          max(case 
            when rpd.column_name = 'type_9'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_9_removed_sk,
          max(case 
             when rpd.column_name = 'type_9' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_9_add_sub,
          min(case 
            when rpd.column_name = 'type_10'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_10_saletype,
          sum(case 
            when rpd.column_name = 'type_10'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_10_adds,
          sum(case 
            when rpd.column_name = 'type_10'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_10_removes,
          max(case 
            when rpd.column_name = 'type_10'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_10_added_sk,
          max(case 
            when rpd.column_name = 'type_10'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_10_removed_sk,
          max(case 
             when rpd.column_name = 'type_10' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_10_add_sub,
          min(case 
            when rpd.column_name = 'type_11'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_11_saletype,
          sum(case 
            when rpd.column_name = 'type_11'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_11_adds,
          sum(case 
            when rpd.column_name = 'type_11'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_11_removes,
          max(case 
            when rpd.column_name = 'type_11'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_11_added_sk,
          max(case 
            when rpd.column_name = 'type_11'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_11_removed_sk,
          max(case 
             when rpd.column_name = 'type_11' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_11_add_sub,
          min(case 
            when rpd.column_name = 'type_12'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_12_saletype,
          sum(case 
            when rpd.column_name = 'type_12'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_12_adds,
          sum(case 
            when rpd.column_name = 'type_12'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_12_removes,
          max(case 
            when rpd.column_name = 'type_12'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_12_added_sk,
          max(case 
            when rpd.column_name = 'type_12'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_12_removed_sk,
          max(case 
             when rpd.column_name = 'type_12' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_12_add_sub,
          min(case 
            when rpd.column_name = 'type_13'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_13_saletype,
          sum(case 
            when rpd.column_name = 'type_13'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_13_adds,
          sum(case 
            when rpd.column_name = 'type_13'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_13_removes,
          max(case 
            when rpd.column_name = 'type_13'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_13_added_sk,
          max(case 
            when rpd.column_name = 'type_13'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_13_removed_sk,
          max(case 
             when rpd.column_name = 'type_13' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_13_add_sub,
          min(case 
            when rpd.column_name = 'type_14'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_14_saletype,
          sum(case 
            when rpd.column_name = 'type_14'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_14_adds,
          sum(case 
            when rpd.column_name = 'type_14'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_14_removes,
          max(case 
            when rpd.column_name = 'type_14'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_14_added_sk,
          max(case 
            when rpd.column_name = 'type_14'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_14_removed_sk,
          max(case 
             when rpd.column_name = 'type_14' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_14_add_sub,
          min(case 
            when rpd.column_name = 'type_15'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_15_saletype,
          sum(case 
            when rpd.column_name = 'type_15'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_15_adds,
          sum(case 
            when rpd.column_name = 'type_15'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_15_removes,
          max(case 
            when rpd.column_name = 'type_15'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_15_added_sk,
          max(case 
            when rpd.column_name = 'type_15'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_15_removed_sk,
          max(case 
             when rpd.column_name = 'type_15' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_15_add_sub,
          min(case 
            when rpd.column_name = 'type_16'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_16_saletype,
          sum(case 
            when rpd.column_name = 'type_16'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_16_adds,
          sum(case 
            when rpd.column_name = 'type_16'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_16_removes,
          max(case 
            when rpd.column_name = 'type_16'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_16_added_sk,
          max(case 
            when rpd.column_name = 'type_16'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_16_removed_sk,
          max(case 
             when rpd.column_name = 'type_16' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_16_add_sub,
          min(case 
            when rpd.column_name = 'type_17'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_17_saletype,
          sum(case 
            when rpd.column_name = 'type_17'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_17_adds,
          sum(case 
            when rpd.column_name = 'type_17'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_17_removes,
          max(case 
            when rpd.column_name = 'type_17'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_17_added_sk,
          max(case 
            when rpd.column_name = 'type_17'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_17_removed_sk,
          max(case 
             when rpd.column_name = 'type_17' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_17_add_sub,
          min(case 
            when rpd.column_name = 'type_18'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_18_saletype,
          sum(case 
            when rpd.column_name = 'type_18'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_18_adds,
          sum(case 
            when rpd.column_name = 'type_18'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_18_removes,
          max(case 
            when rpd.column_name = 'type_18'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_18_added_sk,
          max(case 
            when rpd.column_name = 'type_18'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_18_removed_sk,
          max(case 
             when rpd.column_name = 'type_18' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_18_add_sub,
          min(case 
            when rpd.column_name = 'type_19'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_19_saletype,
          sum(case 
            when rpd.column_name = 'type_19'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_19_adds,
          sum(case 
            when rpd.column_name = 'type_19'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_19_removes,
          max(case 
            when rpd.column_name = 'type_19'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_19_added_sk,
          max(case 
            when rpd.column_name = 'type_19'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_19_removed_sk,
          max(case 
             when rpd.column_name = 'type_19' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_19_add_sub,
          min(case 
            when rpd.column_name = 'type_20'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_20_saletype,
          sum(case 
            when rpd.column_name = 'type_20'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_20_adds,
          sum(case 
            when rpd.column_name = 'type_20'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_20_removes,
          max(case 
            when rpd.column_name = 'type_20'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_20_added_sk,
          max(case 
            when rpd.column_name = 'type_20'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_20_removed_sk,
          max(case 
             when rpd.column_name = 'type_20' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_20_add_sub,
          min(case 
            when rpd.column_name = 'type_21'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_21_saletype,
          sum(case 
            when rpd.column_name = 'type_21'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_21_adds,
          sum(case 
            when rpd.column_name = 'type_21'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_21_removes,
          max(case 
            when rpd.column_name = 'type_21'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_21_added_sk,
          max(case 
            when rpd.column_name = 'type_21'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_21_removed_sk,
          max(case 
             when rpd.column_name = 'type_21' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_21_add_sub,
          min(case 
            when rpd.column_name = 'type_22'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_22_saletype,
          sum(case 
            when rpd.column_name = 'type_22'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_22_adds,
          sum(case 
            when rpd.column_name = 'type_22'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_22_removes,
          max(case 
            when rpd.column_name = 'type_22'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_22_added_sk,
          max(case 
            when rpd.column_name = 'type_22'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_22_removed_sk,
          max(case 
             when rpd.column_name = 'type_22' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_22_add_sub,
          min(case 
            when rpd.column_name = 'type_23'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_23_saletype,
          sum(case 
            when rpd.column_name = 'type_23'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_23_adds,
          sum(case 
            when rpd.column_name = 'type_23'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_23_removes,
          max(case 
            when rpd.column_name = 'type_23'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_23_added_sk,
          max(case 
            when rpd.column_name = 'type_23'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_23_removed_sk,
          max(case 
             when rpd.column_name = 'type_23' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_23_add_sub,
          min(case 
            when rpd.column_name = 'type_24'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_24_saletype,
          sum(case 
            when rpd.column_name = 'type_24'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_24_adds,
          sum(case 
            when rpd.column_name = 'type_24'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_24_removes,
          max(case 
            when rpd.column_name = 'type_24'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_24_added_sk,
          max(case 
            when rpd.column_name = 'type_24'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_24_removed_sk,
          max(case 
             when rpd.column_name = 'type_24' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_24_add_sub,
          min(case 
            when rpd.column_name = 'type_25'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_25_saletype,
          sum(case 
            when rpd.column_name = 'type_25'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_25_adds,
          sum(case 
            when rpd.column_name = 'type_25'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_25_removes,
          max(case 
            when rpd.column_name = 'type_25'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_25_added_sk,
          max(case 
            when rpd.column_name = 'type_25'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_25_removed_sk,
          max(case 
             when rpd.column_name = 'type_25' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_25_add_sub,
          min(case 
            when rpd.column_name = 'type_26'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_26_saletype,
          sum(case 
            when rpd.column_name = 'type_26'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_26_adds,
          sum(case 
            when rpd.column_name = 'type_26'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_26_removes,
          max(case 
            when rpd.column_name = 'type_26'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_26_added_sk,
          max(case 
            when rpd.column_name = 'type_26'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_26_removed_sk,
          max(case 
             when rpd.column_name = 'type_26' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_26_add_sub,
          min(case 
            when rpd.column_name = 'type_27'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_27_saletype,
          sum(case 
            when rpd.column_name = 'type_27'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_27_adds,
          sum(case 
            when rpd.column_name = 'type_27'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_27_removes,
          max(case 
            when rpd.column_name = 'type_27'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_27_added_sk,
          max(case 
            when rpd.column_name = 'type_27'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_27_removed_sk,
          max(case 
             when rpd.column_name = 'type_27' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_27_add_sub,
          min(case 
            when rpd.column_name = 'type_28'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_28_saletype,
          sum(case 
            when rpd.column_name = 'type_28'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_28_adds,
          sum(case 
            when rpd.column_name = 'type_28'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_28_removes,
          max(case 
            when rpd.column_name = 'type_28'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_28_added_sk,
          max(case 
            when rpd.column_name = 'type_28'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_28_removed_sk,
          max(case 
             when rpd.column_name = 'type_28' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_28_add_sub,
          min(case 
            when rpd.column_name = 'type_29'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_29_saletype,
          sum(case 
            when rpd.column_name = 'type_29'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_29_adds,
          sum(case 
            when rpd.column_name = 'type_29'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_29_removes,
          max(case 
            when rpd.column_name = 'type_29'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_29_added_sk,
          max(case 
            when rpd.column_name = 'type_29'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_29_removed_sk,
          max(case 
             when rpd.column_name = 'type_29' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_29_add_sub,
          min(case 
            when rpd.column_name = 'type_30'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_30_saletype,
          sum(case 
            when rpd.column_name = 'type_30'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_30_adds,
          sum(case 
            when rpd.column_name = 'type_30'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_30_removes,
          max(case 
            when rpd.column_name = 'type_30'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_30_added_sk,
          max(case 
            when rpd.column_name = 'type_30'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_30_removed_sk,
          max(case 
             when rpd.column_name = 'type_30' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_30_add_sub,
          min(case 
            when rpd.column_name = 'type_31'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_31_saletype,
          sum(case 
            when rpd.column_name = 'type_31'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_31_adds,
          sum(case 
            when rpd.column_name = 'type_31'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_31_removes,
          max(case 
            when rpd.column_name = 'type_31'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_31_added_sk,
          max(case 
            when rpd.column_name = 'type_31'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_31_removed_sk,
          max(case 
             when rpd.column_name = 'type_31' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_31_add_sub,
          min(case 
            when rpd.column_name = 'type_32'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_32_saletype,
          sum(case 
            when rpd.column_name = 'type_32'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_32_adds,
          sum(case 
            when rpd.column_name = 'type_32'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_32_removes,
          max(case 
            when rpd.column_name = 'type_32'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_32_added_sk,
          max(case 
            when rpd.column_name = 'type_32'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_32_removed_sk,
          max(case 
             when rpd.column_name = 'type_32' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_32_add_sub,
          min(case 
            when rpd.column_name = 'type_33'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_33_saletype,
          sum(case 
            when rpd.column_name = 'type_33'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_33_adds,
          sum(case 
            when rpd.column_name = 'type_33'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_33_removes,
          max(case 
            when rpd.column_name = 'type_33'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_33_added_sk,
          max(case 
            when rpd.column_name = 'type_33'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_33_removed_sk,
          max(case 
             when rpd.column_name = 'type_33' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_33_add_sub,
          min(case 
            when rpd.column_name = 'type_34'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_34_saletype,
          sum(case 
            when rpd.column_name = 'type_34'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_34_adds,
          sum(case 
            when rpd.column_name = 'type_34'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_34_removes,
          max(case 
            when rpd.column_name = 'type_34'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_34_added_sk,
          max(case 
            when rpd.column_name = 'type_34'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_34_removed_sk,
          max(case 
             when rpd.column_name = 'type_34' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_34_add_sub,
          min(case 
            when rpd.column_name = 'type_35'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_35_saletype,
          sum(case 
            when rpd.column_name = 'type_35'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_35_adds,
          sum(case 
            when rpd.column_name = 'type_35'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_35_removes,
          max(case 
            when rpd.column_name = 'type_35'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_35_added_sk,
          max(case 
            when rpd.column_name = 'type_35'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_35_removed_sk,
          max(case 
             when rpd.column_name = 'type_35' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_35_add_sub,
          min(case 
            when rpd.column_name = 'type_36'  and ol.rownum = 1  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_36_saletype,
          sum(case 
            when rpd.column_name = 'type_36'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_36_adds,
          sum(case 
            when rpd.column_name = 'type_36'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_36_removes,
          max(case 
            when rpd.column_name = 'type_36'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_36_added_sk,
          max(case 
            when rpd.column_name = 'type_36'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_36_removed_sk,
          max(case 
             when rpd.column_name = 'type_36' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_36_add_sub,
                    min(case 
            when rpd.column_name = 'type_37'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_37_saletype,
          sum(case 
            when rpd.column_name = 'type_37'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_37_adds,
          sum(case 
            when rpd.column_name = 'type_37'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_37_removes,
          max(case 
            when rpd.column_name = 'type_37'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_37_added_sk,
          max(case 
            when rpd.column_name = 'type_37'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_37_removed_sk,
          max(case 
             when rpd.column_name = 'type_37' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_37_add_sub,
                    min(case 
            when rpd.column_name = 'type_38'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_38_saletype,
          sum(case 
            when rpd.column_name = 'type_38'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_38_adds,
          sum(case 
            when rpd.column_name = 'type_38'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_38_removes,
          max(case 
            when rpd.column_name = 'type_38'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_38_added_sk,
          max(case 
            when rpd.column_name = 'type_38'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_38_removed_sk,
          max(case 
             when rpd.column_name = 'type_38' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_38_add_sub,
                    min(case 
            when rpd.column_name = 'type_39'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_39_saletype,
          sum(case 
            when rpd.column_name = 'type_39'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_39_adds,
          sum(case 
            when rpd.column_name = 'type_39'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_39_removes,
          max(case 
            when rpd.column_name = 'type_39'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_39_added_sk,
          max(case 
            when rpd.column_name = 'type_39'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_39_removed_sk,
          max(case 
             when rpd.column_name = 'type_39' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_39_add_sub,
                    min(case 
            when rpd.column_name = 'type_40'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_40_saletype,
          sum(case 
            when rpd.column_name = 'type_40'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_40_adds,
          sum(case 
            when rpd.column_name = 'type_40'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_40_removes,
          max(case 
            when rpd.column_name = 'type_40'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_40_added_sk,
          max(case 
            when rpd.column_name = 'type_40'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_40_removed_sk,
          max(case 
             when rpd.column_name = 'type_40' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_40_add_sub,
                    min(case 
            when rpd.column_name = 'type_41'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_41_saletype,
          sum(case 
            when rpd.column_name = 'type_41'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_41_adds,
          sum(case 
            when rpd.column_name = 'type_41'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_41_removes,
          max(case 
            when rpd.column_name = 'type_41'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_41_added_sk,
          max(case 
            when rpd.column_name = 'type_41'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_41_removed_sk,
          max(case 
             when rpd.column_name = 'type_41' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_41_add_sub,
                    min(case 
            when rpd.column_name = 'type_42'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_42_saletype,
          sum(case 
            when rpd.column_name = 'type_42'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_42_adds,
          sum(case 
            when rpd.column_name = 'type_42'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_42_removes,
          max(case 
            when rpd.column_name = 'type_42'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_42_added_sk,
          max(case 
            when rpd.column_name = 'type_42'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_42_removed_sk,
          max(case 
             when rpd.column_name = 'type_42' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_42_add_sub,
                    min(case 
            when rpd.column_name = 'type_43'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_43_saletype,
          sum(case 
            when rpd.column_name = 'type_43'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_43_adds,
          sum(case 
            when rpd.column_name = 'type_43'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_43_removes,
          max(case 
            when rpd.column_name = 'type_43'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_43_added_sk,
          max(case 
            when rpd.column_name = 'type_43'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_43_removed_sk,
          max(case 
             when rpd.column_name = 'type_43' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_43_add_sub,
                    min(case 
            when rpd.column_name = 'type_44'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_44_saletype,
          sum(case 
            when rpd.column_name = 'type_44'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_44_adds,
          sum(case 
            when rpd.column_name = 'type_44'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_44_removes,
          max(case 
            when rpd.column_name = 'type_44'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_44_added_sk,
          max(case 
            when rpd.column_name = 'type_44'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_44_removed_sk,
          max(case 
             when rpd.column_name = 'type_44' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_44_add_sub,
                    min(case 
            when rpd.column_name = 'type_45'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_45_saletype,
          sum(case 
            when rpd.column_name = 'type_45'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_45_adds,
          sum(case 
            when rpd.column_name = 'type_45'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_45_removes,
          max(case 
            when rpd.column_name = 'type_45'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_45_added_sk,
          max(case 
            when rpd.column_name = 'type_45'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_45_removed_sk,
          max(case 
             when rpd.column_name = 'type_45' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_45_add_sub,
                    min(case 
            when rpd.column_name = 'type_46'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_46_saletype,
          sum(case 
            when rpd.column_name = 'type_46'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_46_adds,
          sum(case 
            when rpd.column_name = 'type_46'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_46_removes,
          max(case 
            when rpd.column_name = 'type_46'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_46_added_sk,
          max(case 
            when rpd.column_name = 'type_46'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_46_removed_sk,
          max(case 
             when rpd.column_name = 'type_46' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_46_add_sub,
                    min(case 
            when rpd.column_name = 'type_47'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_47_saletype,
          sum(case 
            when rpd.column_name = 'type_47'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_47_adds,
          sum(case 
            when rpd.column_name = 'type_47'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_47_removes,
          max(case 
            when rpd.column_name = 'type_47'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_47_added_sk,
          max(case 
            when rpd.column_name = 'type_47'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_47_removed_sk,
          max(case 
             when rpd.column_name = 'type_47' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_47_add_sub,
                    min(case 
            when rpd.column_name = 'type_48'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_48_saletype,
          sum(case 
            when rpd.column_name = 'type_48'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_48_adds,
          sum(case 
            when rpd.column_name = 'type_48'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_48_removes,
          max(case 
            when rpd.column_name = 'type_48'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_48_added_sk,
          max(case 
            when rpd.column_name = 'type_48'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_48_removed_sk,
          max(case 
             when rpd.column_name = 'type_48' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_48_add_sub,
                    min(case 
            when rpd.column_name = 'type_49'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_49_saletype,
          sum(case 
            when rpd.column_name = 'type_49'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_49_adds,
          sum(case 
            when rpd.column_name = 'type_49'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_49_removes,
          max(case 
            when rpd.column_name = 'type_49'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_49_added_sk,
          max(case 
            when rpd.column_name = 'type_49'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_49_removed_sk,
          max(case 
             when rpd.column_name = 'type_49' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_49_add_sub,
                    min(case 
            when rpd.column_name = 'type_50'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_50_saletype,
          sum(case 
            when rpd.column_name = 'type_50'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_50_adds,
          sum(case 
            when rpd.column_name = 'type_50'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_50_removes,
          max(case 
            when rpd.column_name = 'type_50'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_50_added_sk,
          max(case 
            when rpd.column_name = 'type_50'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_50_removed_sk,
          max(case 
             when rpd.column_name = 'type_50' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_50_add_sub,
                    min(case 
            when rpd.column_name = 'type_51'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_51_saletype,
          sum(case 
            when rpd.column_name = 'type_51'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_51_adds,
          sum(case 
            when rpd.column_name = 'type_51'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_51_removes,
          max(case 
            when rpd.column_name = 'type_51'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_51_added_sk,
          max(case 
            when rpd.column_name = 'type_51'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_51_removed_sk,
          max(case 
             when rpd.column_name = 'type_51' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_51_add_sub,
                    min(case 
            when rpd.column_name = 'type_52'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_52_saletype,
          sum(case 
            when rpd.column_name = 'type_52'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_52_adds,
          sum(case 
            when rpd.column_name = 'type_52'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_52_removes,
          max(case 
            when rpd.column_name = 'type_52'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_52_added_sk,
          max(case 
            when rpd.column_name = 'type_52'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_52_removed_sk,
          max(case 
             when rpd.column_name = 'type_52' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_52_add_sub,
                    min(case 
            when rpd.column_name = 'type_53'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_53_saletype,
          sum(case 
            when rpd.column_name = 'type_53'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_53_adds,
          sum(case 
            when rpd.column_name = 'type_53'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_53_removes,
          max(case 
            when rpd.column_name = 'type_53'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_53_added_sk,
          max(case 
            when rpd.column_name = 'type_53'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_53_removed_sk,
          max(case 
             when rpd.column_name = 'type_53' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_53_add_sub,
                    min(case 
            when rpd.column_name = 'type_54'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_54_saletype,
          sum(case 
            when rpd.column_name = 'type_54'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_54_adds,
          sum(case 
            when rpd.column_name = 'type_54'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_54_removes,
          max(case 
            when rpd.column_name = 'type_54'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_54_added_sk,
          max(case 
            when rpd.column_name = 'type_54'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_54_removed_sk,
          max(case 
             when rpd.column_name = 'type_54' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_54_add_sub,
                    min(case 
            when rpd.column_name = 'type_55'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_55_saletype,
          sum(case 
            when rpd.column_name = 'type_55'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_55_adds,
          sum(case 
            when rpd.column_name = 'type_55'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_55_removes,
          max(case 
            when rpd.column_name = 'type_55'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_55_added_sk,
          max(case 
            when rpd.column_name = 'type_55'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_55_removed_sk,
          max(case 
             when rpd.column_name = 'type_55' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_55_add_sub,
                    min(case 
            when rpd.column_name = 'type_56'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_56_saletype,
          sum(case 
            when rpd.column_name = 'type_56'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_56_adds,
          sum(case 
            when rpd.column_name = 'type_56'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_56_removes,
          max(case 
            when rpd.column_name = 'type_56'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_56_added_sk,
          max(case 
            when rpd.column_name = 'type_56'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_56_removed_sk,
          max(case 
             when rpd.column_name = 'type_56' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_56_add_sub,
                    min(case 
            when rpd.column_name = 'type_57'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_57_saletype,
          sum(case 
            when rpd.column_name = 'type_57'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_57_adds,
          sum(case 
            when rpd.column_name = 'type_57'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_57_removes,
          max(case 
            when rpd.column_name = 'type_57'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_57_added_sk,
          max(case 
            when rpd.column_name = 'type_57'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_57_removed_sk,
          max(case 
             when rpd.column_name = 'type_57' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_57_add_sub,
                    min(case 
            when rpd.column_name = 'type_58'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_58_saletype,
          sum(case 
            when rpd.column_name = 'type_58'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_58_adds,
          sum(case 
            when rpd.column_name = 'type_58'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_58_removes,
          max(case 
            when rpd.column_name = 'type_58'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_58_added_sk,
          max(case 
            when rpd.column_name = 'type_58'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_58_removed_sk,
          max(case 
             when rpd.column_name = 'type_58' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_58_add_sub,
                    min(case 
            when rpd.column_name = 'type_59'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_59_saletype,
          sum(case 
            when rpd.column_name = 'type_59'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_59_adds,
          sum(case 
            when rpd.column_name = 'type_59'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_59_removes,
          max(case 
            when rpd.column_name = 'type_59'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_59_added_sk,
          max(case 
            when rpd.column_name = 'type_59'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_59_removed_sk,
          max(case 
             when rpd.column_name = 'type_59' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_59_add_sub,
                    min(case 
            when rpd.column_name = 'type_60'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_60_saletype,
          sum(case 
            when rpd.column_name = 'type_60'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_60_adds,
          sum(case 
            when rpd.column_name = 'type_60'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_60_removes,
          max(case 
            when rpd.column_name = 'type_60'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_60_added_sk,
          max(case 
            when rpd.column_name = 'type_60'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_60_removed_sk,
          max(case 
             when rpd.column_name = 'type_60' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_60_add_sub,
	    min(case 
            when rpd.column_name = 'type_61'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_61_saletype,
          sum(case 
            when rpd.column_name = 'type_61'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_61_adds,
          sum(case 
            when rpd.column_name = 'type_61'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_61_removes,
          max(case 
            when rpd.column_name = 'type_61'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_61_added_sk,
          max(case 
            when rpd.column_name = 'type_61'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_61_removed_sk,
          max(case 
             when rpd.column_name = 'type_61' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_61_add_sub,
	    min(case 
            when rpd.column_name = 'type_62'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_62_saletype,
          sum(case 
            when rpd.column_name = 'type_62'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_62_adds,
          sum(case 
            when rpd.column_name = 'type_62'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_62_removes,
          max(case 
            when rpd.column_name = 'type_62'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_62_added_sk,
          max(case 
            when rpd.column_name = 'type_62'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_62_removed_sk,
          max(case 
             when rpd.column_name = 'type_62' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_62_add_sub,
	    min(case 
            when rpd.column_name = 'type_63'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_63_saletype,
          sum(case 
            when rpd.column_name = 'type_63'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_63_adds,
          sum(case 
            when rpd.column_name = 'type_63'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_63_removes,
          max(case 
            when rpd.column_name = 'type_63'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_63_added_sk,
          max(case 
            when rpd.column_name = 'type_63'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_63_removed_sk,
          max(case 
             when rpd.column_name = 'type_63' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_63_add_sub,
	        min(case 
            when rpd.column_name = 'type_64'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_64_saletype,
          sum(case 
            when rpd.column_name = 'type_64'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_64_adds,
          sum(case 
            when rpd.column_name = 'type_64'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_64_removes,
          max(case 
            when rpd.column_name = 'type_64'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_64_added_sk,
          max(case 
            when rpd.column_name = 'type_64'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_64_removed_sk,
          max(case 
             when rpd.column_name = 'type_64' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_64_add_sub,
	        min(case 
            when opt.column_name = 'type_65' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_65_saletype,
          sum(case 
            when opt.column_name = 'type_65'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_65_adds,
          sum(case 
            when opt.column_name = 'type_65'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_65_removes,
          max(case 
            when opt.column_name = 'type_65'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_65_added_sk,
          max(case 
            when opt.column_name = 'type_65'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_65_removed_sk,
          max(case 
             when opt.column_name = 'type_65' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_65_add_sub,
	        min(case 
            when opt.column_name = 'type_66' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_66_saletype,
          sum(case 
            when opt.column_name = 'type_66'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_66_adds,
          sum(case 
            when opt.column_name = 'type_66'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_66_removes,
          max(case 
            when opt.column_name = 'type_66'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_66_added_sk,
          max(case 
            when opt.column_name = 'type_66'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_66_removed_sk,
          max(case 
             when opt.column_name = 'type_66' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_66_add_sub,
	        min(case 
            when opt.column_name = 'type_67'  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_67_saletype,
          sum(case 
            when opt.column_name = 'type_67'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_67_adds,
          sum(case 
            when opt.column_name = 'type_67'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_67_removes,
          max(case 
            when opt.column_name = 'type_67'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_67_added_sk,
          max(case 
            when opt.column_name = 'type_67'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_67_removed_sk,
          max(case 
             when opt.column_name = 'type_67' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_67_add_sub,
		      min(case 
            when opt.column_name = 'type_68'  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_68_saletype,
          sum(case 
            when opt.column_name = 'type_68'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_68_adds,
          sum(case 
            when opt.column_name = 'type_68'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_68_removes,
          max(case 
            when opt.column_name = 'type_68'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_68_added_sk,
          max(case 
            when opt.column_name = 'type_68'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_68_removed_sk,
          max(case 
             when opt.column_name = 'type_68' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_68_add_sub,
	        min(case 
            when opt.column_name = 'type_69'  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_69_saletype,
          sum(case 
            when opt.column_name = 'type_69'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_69_adds,
          sum(case 
            when opt.column_name = 'type_69'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_69_removes,
          max(case 
            when opt.column_name = 'type_69'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_69_added_sk,
          max(case 
            when opt.column_name = 'type_69'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_69_removed_sk,
          max(case 
             when opt.column_name = 'type_69' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_69_add_sub,
	        min(case 
            when opt.column_name = 'type_70'  then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_70_saletype,
          sum(case 
            when opt.column_name = 'type_70'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_70_adds,
          sum(case 
            when opt.column_name = 'type_70'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_70_removes,
          max(case 
            when opt.column_name = 'type_70'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_70_added_sk,
          max(case 
            when opt.column_name = 'type_70'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_70_removed_sk,
          max(case 
             when opt.column_name = 'type_70' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_70_add_sub,
	        min(case 
            when opt.column_name = 'type_71' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_71_saletype,
          sum(case 
            when opt.column_name = 'type_71'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_71_adds,
          sum(case 
            when opt.column_name = 'type_71'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_71_removes,
          max(case 
            when opt.column_name = 'type_71'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_71_added_sk,
          max(case 
            when opt.column_name = 'type_71'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_71_removed_sk,
          max(case 
             when opt.column_name = 'type_71' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_71_add_sub,
          min(case 
            when opt.column_name = 'type_72'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_72_saletype,
          sum(case 
            when opt.column_name = 'type_72'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_72_adds,
          sum(case 
            when opt.column_name = 'type_72'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_72_removes,
          max(case 
            when opt.column_name = 'type_72'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_72_added_sk,
          max(case 
            when opt.column_name = 'type_72'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_72_removed_sk,
          max(case 
             when opt.column_name = 'type_72' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_72_add_sub,
	        min(case 
            when rpd.column_name = 'type_73'   then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_73_saletype,
          sum(case 
            when rpd.column_name = 'type_73'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_73_adds,
          sum(case 
            when rpd.column_name = 'type_73'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_73_removes,
          max(case 
            when rpd.column_name = 'type_73'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_73_added_sk,
          max(case 
            when rpd.column_name = 'type_73'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_73_removed_sk,
          max(case 
             when rpd.column_name = 'type_73' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_73_add_sub,
	        min(case 
            when rpd.column_name = 'type_74'   then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_74_saletype,
          sum(case 
            when rpd.column_name = 'type_74'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_74_adds,
          sum(case 
            when rpd.column_name = 'type_74'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_74_removes,
          max(case 
            when rpd.column_name = 'type_74'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_74_added_sk,
          max(case 
            when rpd.column_name = 'type_74'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_74_removed_sk,
          max(case 
             when rpd.column_name = 'type_74' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_74_add_sub,
	        min(case 
            when rpd.column_name = 'type_75'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_75_saletype,
          sum(case 
            when rpd.column_name = 'type_75'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_75_adds,
          sum(case 
            when rpd.column_name = 'type_75'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_75_removes,
          max(case 
            when rpd.column_name = 'type_75'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_75_added_sk,
          max(case 
            when rpd.column_name = 'type_75'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_75_removed_sk,
          max(case 
             when rpd.column_name = 'type_75' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_75_add_sub,
	        min(case 
            when rpd.column_name = 'type_76'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_76_saletype,
          sum(case 
            when rpd.column_name = 'type_76'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_76_adds,
          sum(case 
            when rpd.column_name = 'type_76'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_76_removes,
          max(case 
            when rpd.column_name = 'type_76'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_76_added_sk,
          max(case 
            when rpd.column_name = 'type_76'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_76_removed_sk,
          max(case 
             when rpd.column_name = 'type_76' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_76_add_sub,
	        min(case 
            when rpd.column_name = 'type_77'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_77_saletype,
          sum(case 
            when rpd.column_name = 'type_77'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_77_adds,
          sum(case 
            when rpd.column_name = 'type_77'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_77_removes,
          max(case 
            when rpd.column_name = 'type_77'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_77_added_sk,
          max(case 
            when rpd.column_name = 'type_77'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_77_removed_sk,
          max(case 
             when rpd.column_name = 'type_77' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_77_add_sub,
	        min(case 
            when rpd.column_name = 'type_78'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_78_saletype,
          sum(case 
            when rpd.column_name = 'type_78'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_78_adds,
          sum(case 
            when rpd.column_name = 'type_78'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_78_removes,
          max(case 
            when rpd.column_name = 'type_78'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_78_added_sk,
          max(case 
            when rpd.column_name = 'type_78'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_78_removed_sk,
          max(case 
             when rpd.column_name = 'type_78' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_78_add_sub,
	        min(case 
            when rpd.column_name = 'type_79'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_79_saletype,
          sum(case 
            when rpd.column_name = 'type_79'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_79_adds,
          sum(case 
            when rpd.column_name = 'type_79'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_79_removes,
          max(case 
            when rpd.column_name = 'type_79'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_79_added_sk,
          max(case 
            when rpd.column_name = 'type_79'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_79_removed_sk,
          max(case 
             when rpd.column_name = 'type_79' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_79_add_sub,
	   min(case 
            when rpd.column_name = 'type_80'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_80_saletype,
          sum(case 
            when rpd.column_name = 'type_80'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_80_adds,
          sum(case 
            when rpd.column_name = 'type_80'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_80_removes,
          max(case 
            when rpd.column_name = 'type_80'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_80_added_sk,
          max(case 
            when rpd.column_name = 'type_80'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_80_removed_sk,
          max(case 
             when rpd.column_name = 'type_80' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_80_add_sub,
	   min(case 
            when rpd.column_name = 'type_81'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_81_saletype,
          sum(case 
            when rpd.column_name = 'type_81'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_81_adds,
          sum(case 
            when rpd.column_name = 'type_81'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_81_removes,
          max(case 
            when rpd.column_name = 'type_81'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_81_added_sk,
          max(case 
            when rpd.column_name = 'type_81'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_81_removed_sk,
          max(case 
             when rpd.column_name = 'type_81' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_81_add_sub,
	   min(case 
            when opt.column_name = 'type_82' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_82_saletype,
          sum(case 
            when opt.column_name = 'type_82'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_82_adds,
          sum(case 
            when opt.column_name = 'type_82'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_82_removes,
          max(case 
            when opt.column_name = 'type_82'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_82_added_sk,
          max(case 
            when opt.column_name = 'type_82'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_82_removed_sk,
          max(case 
             when opt.column_name = 'type_82' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_82_add_sub,
	   min(case 
            when opt.column_name = 'type_83' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_83_saletype,
          sum(case 
            when opt.column_name = 'type_83'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_83_adds,
          sum(case 
            when opt.column_name = 'type_83'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_83_removes,
          max(case 
            when opt.column_name = 'type_83'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_83_added_sk,
          max(case 
            when opt.column_name = 'type_83'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_83_removed_sk,
          max(case 
             when opt.column_name = 'type_83' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_83_add_sub,
	   min(case 
            when opt.column_name = 'type_84' then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_84_saletype,
          sum(case 
            when opt.column_name = 'type_84'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_84_adds,
          sum(case 
            when opt.column_name = 'type_84'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_84_removes,
          max(case 
            when opt.column_name = 'type_84'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_84_added_sk,
          max(case 
            when opt.column_name = 'type_84'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_84_removed_sk,
          max(case 
             when opt.column_name = 'type_84' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_84_add_sub,
	   min(case 
            when rpd.column_name = 'type_85'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_85_saletype,
          sum(case 
            when rpd.column_name = 'type_85'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_85_adds,
          sum(case 
            when rpd.column_name = 'type_85'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_85_removes,
          max(case 
            when rpd.column_name = 'type_85'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_85_added_sk,
          max(case 
            when rpd.column_name = 'type_85'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_85_removed_sk,
          max(case 
             when rpd.column_name = 'type_85' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_85_add_sub,
	   min(case 
            when rpd.column_name = 'type_86'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_86_saletype,
          sum(case 
            when rpd.column_name = 'type_86'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_86_adds,
          sum(case 
            when rpd.column_name = 'type_86'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_86_removes,
          max(case 
            when rpd.column_name = 'type_86'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_86_added_sk,
          max(case 
            when rpd.column_name = 'type_86'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_86_removed_sk,
          max(case 
             when rpd.column_name = 'type_86' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_86_add_sub,
	   min(case 
            when rpd.column_name = 'type_87'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_87_saletype,
          sum(case 
            when rpd.column_name = 'type_87'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_87_adds,
          sum(case 
            when rpd.column_name = 'type_87'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_87_removes,
          max(case 
            when rpd.column_name = 'type_87'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_87_added_sk,
          max(case 
            when rpd.column_name = 'type_87'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_87_removed_sk,
          max(case 
             when rpd.column_name = 'type_87' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_87_add_sub,
	   min(case 
            when rpd.column_name = 'type_88'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_88_saletype,
          sum(case 
            when rpd.column_name = 'type_88'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_88_adds,
          sum(case 
            when rpd.column_name = 'type_88'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_88_removes,
          max(case 
            when rpd.column_name = 'type_88'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_88_added_sk,
          max(case 
            when rpd.column_name = 'type_88'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_88_removed_sk,
          max(case 
             when rpd.column_name = 'type_88' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_88_add_sub,
	   min(case 
            when rpd.column_name = 'type_89'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_89_saletype,
          sum(case 
            when rpd.column_name = 'type_89'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_89_adds,
          sum(case 
            when rpd.column_name = 'type_89'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_89_removes,
          max(case 
            when rpd.column_name = 'type_89'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_89_added_sk,
          max(case 
            when rpd.column_name = 'type_89'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_89_removed_sk,
          max(case 
             when rpd.column_name = 'type_89' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_89_add_sub,
	   min(case 
            when opt.column_name = 'type_90'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_90_saletype,
          sum(case 
            when opt.column_name = 'type_90'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_90_adds,
          sum(case 
            when opt.column_name = 'type_90'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_90_removes,
          max(case 
            when opt.column_name = 'type_90'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_90_added_sk,
          max(case 
            when opt.column_name = 'type_90'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_90_removed_sk,
          max(case 
             when opt.column_name = 'type_90' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_90_add_sub,
          min(case  
			when opt.column_name = 'type_91'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_91_saletype,
          sum(case 
            when opt.column_name = 'type_91'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_91_adds,
          sum(case 
            when opt.column_name = 'type_91'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_91_removes,
          max(case 
            when opt.column_name = 'type_91'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_91_added_sk,
          max(case 
            when opt.column_name = 'type_91'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_91_removed_sk,
          max(case 
             when opt.column_name = 'type_91' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_91_add_sub,
       min(case 
            when opt.column_name = 'type_92'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_92_saletype,
          sum(case 
            when rpd.column_name = 'type_92'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_92_adds,
          sum(case 
            when rpd.column_name = 'type_92'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_92_removes,
          max(case 
            when rpd.column_name = 'type_92'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_92_added_sk,
          max(case 
            when rpd.column_name = 'type_92'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_92_removed_sk,
          max(case 
             when rpd.column_name = 'type_92' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_92_add_sub,
       min(case 
            when rpd.column_name = 'type_93'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_93_saletype,
          sum(case 
            when rpd.column_name = 'type_93'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_93_adds,
          sum(case 
            when rpd.column_name = 'type_93'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_93_removes,
          max(case 
            when rpd.column_name = 'type_93'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_93_added_sk,
          max(case 
            when rpd.column_name = 'type_93'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_93_removed_sk,
          max(case 
             when rpd.column_name = 'type_93' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_93_add_sub,
       min(case 
            when rpd.column_name = 'type_94'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_94_saletype,
          sum(case 
            when rpd.column_name = 'type_94'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_94_adds,
          sum(case 
            when rpd.column_name = 'type_94'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_94_removes,
          max(case 
            when rpd.column_name = 'type_94'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_94_added_sk,
          max(case 
            when rpd.column_name = 'type_94'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_94_removed_sk,
          max(case 
             when rpd.column_name = 'type_94' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_94_add_sub,
        min(case 
            when rpd.column_name = 'type_95'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_95_saletype,
          sum(case 
            when rpd.column_name = 'type_95'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_95_adds,
          sum(case 
            when rpd.column_name = 'type_95'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_95_removes,
          max(case 
            when rpd.column_name = 'type_95'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_95_added_sk,
          max(case 
            when rpd.column_name = 'type_95'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_95_removed_sk,
          max(case 
             when rpd.column_name = 'type_95' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_95_add_sub,
        min(case 
            when rpd.column_name = 'type_96'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_96_saletype,
          sum(case 
            when rpd.column_name = 'type_96'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_96_adds,
          sum(case 
            when rpd.column_name = 'type_96'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_96_removes,
          max(case 
            when rpd.column_name = 'type_96'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_96_added_sk,
          max(case 
            when rpd.column_name = 'type_96'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_96_removed_sk,
          max(case 
             when rpd.column_name = 'type_96' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_96_add_sub,
        min(case 
            when rpd.column_name = 'type_97'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_97_saletype,
          sum(case 
            when rpd.column_name = 'type_97'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_97_adds,
          sum(case 
            when rpd.column_name = 'type_97'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_97_removes,
          max(case 
            when rpd.column_name = 'type_97'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_97_added_sk,
          max(case 
            when rpd.column_name = 'type_97'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_97_removed_sk,
          max(case 
             when rpd.column_name = 'type_97' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_97_add_sub,
        min(case 
            when rpd.column_name = 'type_98'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_98_saletype,
          sum(case 
            when rpd.column_name = 'type_98'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_98_adds,
          sum(case 
            when rpd.column_name = 'type_98'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_98_removes,
          max(case 
            when rpd.column_name = 'type_98'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_98_added_sk,
          max(case 
            when rpd.column_name = 'type_98'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_98_removed_sk,
          max(case 
             when rpd.column_name = 'type_98' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_98_add_sub,
         min(case 
            when rpd.column_name = 'type_99'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_99_saletype,
          sum(case 
            when rpd.column_name = 'type_99'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_99_adds,
          sum(case 
            when rpd.column_name = 'type_99'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_99_removes,
          max(case 
            when rpd.column_name = 'type_99'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_99_added_sk,
          max(case 
            when rpd.column_name = 'type_99'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_99_removed_sk,
          max(case 
             when rpd.column_name = 'type_99' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_99_add_sub,
         min(case 
            when rpd.column_name = 'type_100'  and ol.rownum = 1 then 
            ol.bol_action_sale_type 
          else 
            NULL 
          end)   as type_100_saletype,
          sum(case 
            when rpd.column_name = 'type_100'  and ol.bol_action_type = 'ADD' then 
            1
          else 
            0
          end)   as type_100_adds,
          sum(case 
            when rpd.column_name = 'type_100'  and ol.bol_action_type = 'REMOVE' then 
            1
          else 
            0
          end)   as type_100_removes,
          max(case 
            when rpd.column_name = 'type_100'  and ol.bol_action_type = 'ADD' then 
            ol.product_sk
          else 
            NULL
          end)   as type_100_added_sk,
          max(case 
            when rpd.column_name = 'type_100'  and ol.bol_action_type = 'REMOVE' then 
            ol.product_sk
          else 
            NULL
          end)   as type_100_removed_sk,
          max(case 
             when rpd.column_name = 'type_100' then 
             ol.bol_add_sub_flag
          else 
             NULL
          end)   as type_100_add_sub,
          min(case 
                 when upper(ol.cat_prod_type_desc) in ('DTV PV PREMIUM',
                                                      'SPORTS',
                                                      'CINEMA')
                                                  and ol.rownum = 1 then 
                ol.bol_action_sale_type 
              else 
                null 
              end) as prem_sale_type,
          sum(case 
                when upper(ol.cat_prod_type_desc) in ('DTV PV PREMIUM',
                                                      'SPORTS',
                                                      'CINEMA')
                                                  and ol.bol_action_type = 'ADD' then 
                1 
              else 
                0 
              end) as prem_adds,
          sum(case 
                when upper(ol.cat_prod_type_desc) in ('DTV PV PREMIUM',
                                                      'SPORTS',
                                                      'CINEMA')
                                                  and ol.bol_action_type = 'REMOVE' then 
                1 
              else 
                0 
              end) as prem_removes,
          sum(case 
                when ol.cat_prod_type_desc = 'DTV PV Premium' and ol.bol_action_type = 'ADD' then 
                case 
                  when prod_desc in ('Sports') then 
                  2 
                  when prod_desc in ('Sky Sports 1','Sky Sports 2') then 
                  1 
                else 
                  0 
                end
               when  upper(ol.cat_prod_type_desc) = ('SPORTS') and ol.bol_action_type = 'ADD' then 
               1    
              else 
                0 
              end) as prem_sports_added,
          sum(case 
                when ol.cat_prod_type_desc = 'DTV PV Premium' and ol.bol_action_type = 'ADD' then 
                case 
                  when (   prod_desc in ('Movies') 
                        or prod_desc in ('Cinema')) then 
                  2 
                  when (   prod_desc in ('Sky Movies 1','Sky Movies 2')
				        or prod_desc in ('Sky Cinema 1','Sky Cinema 2'))then 
                  1
                else 
                  0 
                end
               when  upper(ol.cat_prod_type_desc) = ('CINEMA') and ol.bol_action_type = 'ADD' then             
               1                                  
              else 
                0 
              end) as prem_movies_added,
          sum(case 
                when ol.cat_prod_type_desc = 'DTV PV Premium' and ol.bol_action_type = 'REMOVE' then 
                case 
                  when prod_desc in ('Sports') then 
                  2 
                  when prod_desc in ('Sky Sports 1','Sky Sports 2') then 
                  1 
                else 
                  0 
                end
               when  upper(ol.cat_prod_type_desc) = ('SPORTS') and ol.bol_action_type = 'REMOVE' then 
               1   
              else
                0 
              end) as prem_sports_removed,
          sum(case 
                when ol.cat_prod_type_desc = 'DTV PV Premium' and ol.bol_action_type = 'REMOVE' then 
                case 
                  when (   prod_desc in ('Movies') 
                        or prod_desc in ('Cinema')) then  
                  2 
                  when (   prod_desc in ('Sky Movies 1','Sky Movies 2')
				        or prod_desc in ('Sky Cinema 1','Sky Cinema 2'))then 
                  1 
                else
                  0 
                end
               when  upper(ol.cat_prod_type_desc) = ('CINEMA') and ol.bol_action_type = 'REMOVE' then   
               1      
              else 
                0 
              end) as prem_movies_removed
    from uk_pre_customer_is.td_order_product_p3_1 ol
    inner join uk_inp_tds_chordiant_eod_is.cc_bsborder o
     on (    o.id = ol.order_id
         and o.logically_deleted = 0)
    left outer join uk_pub_customer_is.dim_order_product_type rpd
     on ( upper(ol.cat_prod_type_desc) = upper(rpd.column_value))
    left outer join uk_pub_customer_is.dim_order_product_type opt
     on ( upper(ol.prod_desc) = upper(opt.column_value)) 
  group by id,ordernumber,created,lastupdate,createdby,comtypecode,
         updatedby,partyroleid,referringcustomerid,
         urn,currency,portfolio_id,diallednumber,status,statuschangeddate,statusreasoncode,new_customer_flag,account_created_date;
		 