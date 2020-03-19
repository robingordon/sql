--------------------------------------------------------------------------------
--
-- file-name    : cdslm10020.sql
-- author       : Ross MacLean
-- date created : 27th May 2015
--
--------------------------------------------------------------------------------
-- Description  : This script is used to populate temporary tables with the Base 
--                Order lines that will be used to populate the new Sales Mart.
--
-- Comments     : This replaces the old Sales Mart in EXN_DW_DB as part of CR27
--
-- Usage        : NZSQL Call
--
-- Called By    : CDSLM10000.scr
--
-- Calls        : None
--
-- Parameters   : :lower_date_bound
--                :upper_date_bound
--
-- Exit codes   : 0 - Success
--                1 - Failure
--
-- Revisions
-- =============================================================================
-- Date     user id  MR#       Comments                                     Ver.
-- ------   -------  ------    -------------------------------------------  ----
-- 270515   ROM19    CR27      Initial version                               1.0
-- 070817   NSA19    LIMA      Priceable units Fuzzy Logic                   1.1
-- 231117   LFO05    ROSE      Sales Mart timestamp fix                      1.2
-- 250919   ANP52    GCP       Update for migration to BQ                    2.0
-- 170320   RGO15   INC2490586 Update to account_number population           2.1
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Populate td_order_PRODUCT_P1 - Uses CC_BSBORDERLINE as the driver to get any 
--order-line from a given day that joins to CC_BSBPORTFOLIOPRODUCT on id and 
--matches the criteria set out in join condition. It then joins to reference 
--data tables REF_ACTION_SALETYPE_PCO (which was provided by the business to
--give a more use-able sale type and ranks them). Also uses WH_PROUCT_DIM to get 
--what product the order-line corresponds too. Any which come back from the 
--business definition as 'IGNORE' we remove from the delta. rownum columns will 
--be used to pick the correct type in the Sales mart step 1.
-------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p1:WRITE_TRUNCATE:
select bol.orderid                                          as order_id,
       bol.id                                               as order_line_id,
       bpp.portfolioid                                      as portfolio_id,
       bol.createdby                                        as agent_id,
       case 
         when bol.effectivedate < timestamp_trunc(bol.created, day) then 
         bol.effectivedate 
         else 
         bol.created  
       end                                                  as olp_created_dt,                
       case 
         when bol.effectivedate < timestamp_trunc(bol.created, day) then 
         1 
         else 
         0 
       end                                                  as systemfix_flag,
       bol.salespeed                                        as salespeed,    
       bol.interestsourceid                                 as interest_source,
       bpp.subscriptionid                                   as subscription_id,
       pd.description                                       as prod_desc,
       pd.product_sk                                        as product_sk,
       pd.product_type                                      as cat_prod_type_desc,
       das.action_type                                      as bol_action_type,
       das.action_sale_type                                 as bol_action_sale_type,
       das.priority                                         as saletyperanking,
       row_number() over (partition by orderid, 
                                       pd.product_type,
                                       pd.description									   
                             order by das.priority asc) as rownum,
       row_number() over (partition by orderid, 
                                       das.action_type 
                              order by bol.created asc)     as rownum2,
       bol.serviceinstanceid                                as service_instance_id,
       bol.retailproofofpurchaseid                          as retailproofofpurchaseid,
       bol.fulfilmentitemid                                 as fulfilmentitemid,
       bol.action                                           as ol_action,
       bol.created                                          as ol_created,
       bol.bundleid                                         as ol_bundle_id,
       bpp.id                                               as portfolio_product_id
  from uk_inp_tds_chordiant_eod_is.cc_bsborderline bol
  inner join uk_inp_tds_chordiant_eod_is.cc_bsbportfolioproduct bpp
    on (    bol.portfolioproductid = bpp.id
        and bol.created >= parse_timestamp('%d-%b-%Y %H:%M:%S', @lower_date_bound)
        and bol.created <= parse_timestamp('%d-%b-%Y %H:%M:%E6S', @upper_date_bound)
        and upper(bol.orderlinetype) not in ('DISCOUNT','DISCOUNTAMEND','PPVADD')
        and not (upper(bol.status) = 'CNCLED' 
        and bol.statuschangeddate <= bol.created)
        and upper(bol.action) not in ('MO')
        and bol.action is not null
        and bpp.logically_deleted = 0
        and bol.logically_deleted = 0)
  inner join uk_pub_customer_is.dim_action_sale_type das
    on (    das.action   = bol.action 
        and das.saletype = bol.saletype)
  inner join uk_pub_customer_is.wh_product_dim pd 
    on (    bpp.catalogueproductid = pd.fo_src_system_catalogue_id 
        and upper(pd.product_type) not in ('DTV PV BONUS','SKY TALK USAGE'))
   where upper(das.action_type) != 'IGNORE' or das.action_type is null;
   
---------------------------------------------------------------------------------
-- get all priceable units (fuzzy logic)
---------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p2_1:WRITE_TRUNCATE:
select td.order_line_id,
       pu.id,
       pu.created,
       ifnull(substr( pu.priceableunitreferenceid,1,5),'?') cat_prod_id
  from uk_pre_customer_is.td_order_product_p1 td
 inner join uk_inp_tds_chordiant_eod_is.cc_bsbpriceableunitportfolioprod pupp 
    on (td.portfolio_product_id  = pupp.portfolioproductid)
 inner join uk_inp_tds_chordiant_eod_is.cc_bsbpriceableunit pu 
    on (    pu.id = pupp.priceableunitid 
        and pu.created <= timestamp_add(td.olp_created_dt, interval 3 second));

---------------------------------------------------------------------------------
-- get latest priceable units
---------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p2_2:WRITE_TRUNCATE:
select order_line_id,
       cat_prod_id
 from (select order_line_id,
               cat_prod_id,
               row_number() over (partition by order_line_id
                                      order by created desc, 
                                               id      desc) row_num
          from uk_pre_customer_is.td_order_product_p2_1
       ) foo
 where row_num = 1;

-----------------------------------------------------------------------------------
--Populate td_order_PRODUCT_P2 - We now join out the further tables to obtain the
--telephony_order_type & retailer_id. We have also added the field bol_add_sub_flag
--which is a boolean to determine whether a subscription was added as part of the 
--OL. This will be used to reconcile old sales mart reports.
------------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p2:WRITE_TRUNCATE:
select td.order_id                                     as order_id,
       td.order_line_id                                as order_line_id,
       td.portfolio_id                                 as portfolio_id,
       td.agent_id                                     as agent_id,
       td.olp_created_dt                               as olp_created_dt,                
       td.systemfix_flag                               as systemfix_flag,
       rpop.retailerid                                 as rpop_retailer_id,
       td.salespeed                                    as salespeed,    
       td.interest_source                              as interest_source,
       s.technologycode                                as technology_code,
       td.subscription_id                              as subscription_id,
       td.prod_desc                                    as prod_desc,
       case
         when (rpd.column_name in ('type_73',
                                   'type_74')) then
           ifnull(pd.product_sk,
               td.product_sk)
         else
           td.product_sk 
       end                                             as product_sk,
       td.cat_prod_type_desc                           as cat_prod_type_desc,
       td.bol_action_type                              as bol_action_type,
       td.bol_action_sale_type                         as bol_action_sale_type,
       td.saletyperanking                              as saletyperanking,
       td.rownum                                       as rownum,
       td.rownum2                                      as rownum2,
       td.service_instance_id                          as service_instance_id,
       tel.ordertype                                   as telephony_order_type,
       case 
         when td.ol_action in ('INDIRECT','VISITEXIST','UK') then
           0
         when (    s.created between timestamp_sub(td.ol_created, interval 3 second)
               and timestamp_add(td.ol_created, interval 3 second)) then
           1
       else 
           0 
       end                                             as bol_add_sub_flag,
       td.ol_bundle_id                                 as bundle_id
  from uk_pre_customer_is.td_order_product_p1 td
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbsubscription s    
    on (    s.id = td.subscription_id
        and s.logically_deleted = 0)
  left outer join uk_pre_customer_is.td_order_product_p2_2 p2_2
    on (td.order_line_id = p2_2.order_line_id)
  left join uk_pub_customer_is.wh_product_dim pd 
    on (    p2_2.cat_prod_id = pd.fo_src_system_catalogue_id 
        and upper(pd.product_type) not in ('DTV PV BONUS','SKY TALK USAGE'))
   left outer join uk_pub_customer_is.dim_order_product_type rpd
     on ( upper(td.cat_prod_type_desc) = upper(rpd.column_value))
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbretailproofofpurchase rpop 
    on (    rpop.id = td.retailproofofpurchaseid
        and rpop.logically_deleted = 0)
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbtelephonyrequirement tel
    on (    td.fulfilmentitemid = tel.telephonyfulfilmentid
        and tel.logically_deleted = 0);
	
-------------------------------------------------------------------------------------
--Populate td_order_PRODUCT_P3 - This joins out to CC_BSBSERVICEINSTANCE on two levels.
--Firstly on id and then parentserviceinstanceid. It then uses whatever one brings
--back a serviceinstanceid to then join to CC_BSBBILLINGACCOUNT using this field. if
--the first join to CC_BSBBILLINGACCOUNT produces no result when then try portfolioid.
--Also a simple join to WH_PRODUCT_DIM to get the currency_code.  
-------------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p3:WRITE_TRUNCATE:
select td.order_id                                                   as order_id,
       td.order_line_id                                              as order_line_id,
       td.portfolio_id                                               as portfolio_id,
       td.agent_id                                                   as agent_id,
       td.olp_created_dt                                             as olp_created_dt,
       td.systemfix_flag                                             as systemfix_flag,
       td.rpop_retailer_id                                           as rpop_retailer_id,
       td.salespeed                                                  as salespeed,
       td.interest_source                                            as interest_source,
       td.technology_code                                            as technology_code,
       td.subscription_id                                            as subscription_id,
       td.prod_desc                                                  as prod_desc,
       td.product_sk                                                 as product_sk,
       td.cat_prod_type_desc                                         as cat_prod_type_desc,
       td.bol_action_type                                            as bol_action_type,
       td.bol_action_sale_type                                       as bol_action_sale_type,
       td.saletyperanking                                            as saletyperanking,
       td.rownum                                                     as rownum,
       td.rownum2                                                    as rownum2,
       td.telephony_order_type                                       as telephony_order_type,
       ifnull(bas.id,bap.id)                                            as billing_account_id,
       ifnull(bas.accountnumber,bap.accountnumber)                      as account_number,
       ifnull(cast(bas.created as date),cast(bap.created as date))                      as account_created_date,
       ifnull(cls.code_desc,ifnull(clp.code_desc,'?'))                     as account_currency_code,
       ifnull(bas.customertypecode,ifnull(bap.customertypecode,'?'))       as account_type_code,
       ifnull(bas.customersubtypecode,ifnull(bap.customersubtypecode,'?')) as account_sub_type_code,
       td.bol_add_sub_flag                                           as bol_add_sub_flag,
       td.bundle_id                                                  as bundle_id
  from uk_pre_customer_is.td_order_product_p2 td
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbserviceinstance si 
    on (    si.id = td.service_instance_id
        and si.logically_deleted = 0)
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbserviceinstance psi 
    on (    psi.id = si.parentserviceinstanceid
        and psi.logically_deleted = 0)
  left outer join uk_inp_tds_chordiant_eod_is.cc_bsbbillingaccount bas
    on (    bas.serviceinstanceid = ifnull(psi.parentserviceinstanceid,ifnull(si.parentserviceinstanceid,td.service_instance_id))
        and bas.logically_deleted = 0
        and not bas.serviceinstanceid = 'NULL')
  left outer join (select id,
                          accountnumber,
                          created,
                          customertypecode,
                          customersubtypecode,
                          portfolioid,
                          currencycode,
                          logically_deleted,
                          rn
                     from (select id,
                                  accountnumber,
                                  created,
                                  customertypecode,
                                  customersubtypecode,
                                  portfolioid,
                                  currencycode,
                                  logically_deleted,
                                  row_number() over (partition by portfolioid
                                                         order by lastupdate desc) as rn
                             from uk_inp_tds_chordiant_eod_is.cc_bsbbillingaccount
							where logically_deleted = 0) foo
                  ) bap
    on (    bap.portfolioid = td.portfolio_id
        and bap.rn = 1)
  left outer join uk_pub_customer_is.wh_code_lookup_dim cls
    on (    bas.currencycode    = cls.code_as_char
        and cls.code_applies_to = 'WAREHOUSE/CURRENCY/SHORT_DISPLAY')
  left outer join uk_pub_customer_is.wh_code_lookup_dim clp
    on (    bap.currencycode    = clp.code_as_char
        and clp.code_applies_to = 'WAREHOUSE/CURRENCY/SHORT_DISPLAY');
	
--------------------------------------------------------------------------------
--Update to resolve incorrect Account Number being selected for Order INC2490586
--------------------------------------------------------------------------------
uk_pre_customer_is.td_order_product_p3_1:WRITE_TRUNCATE:
select td.order_id                                                   as order_id,
       td.order_line_id                                              as order_line_id,
       td.portfolio_id                                               as portfolio_id,
       td.agent_id                                                   as agent_id,
       td.olp_created_dt                                             as olp_created_dt,
       td.systemfix_flag                                             as systemfix_flag,
       td.rpop_retailer_id                                           as rpop_retailer_id,
       td.salespeed                                                  as salespeed,
       td.interest_source                                            as interest_source,
       td.technology_code                                            as technology_code,
       td.subscription_id                                            as subscription_id,
       td.prod_desc                                                  as prod_desc,
       td.product_sk                                                 as product_sk,
       td.cat_prod_type_desc                                         as cat_prod_type_desc,
       td.bol_action_type                                            as bol_action_type,
       td.bol_action_sale_type                                       as bol_action_sale_type,
       td.saletyperanking                                            as saletyperanking,
       td.rownum                                                     as rownum,
       td.rownum2                                                    as rownum2,
       td.telephony_order_type                                       as telephony_order_type,
       td.billing_account_id                                         as billing_account_id,
       ifnull(def.accountnumber,td.account_number)                   as account_number,
       ifnull(td.account_created_date)                               as account_created_date,
       ifnull(td.account_currency_code)                              as account_currency_code,
       ifnull(td.account_type_code)                                  as account_type_code,
       ifnull(td.account_sub_type_code)                              as account_sub_type_code,
       td.bol_add_sub_flag                                           as bol_add_sub_flag,
       td.bundle_id                                                  as bundle_id
  from uk_pre_customer_is.td_order_product_p3 td
  left outer join (select distinct a.order_id,
                          bap.accountnumber
                     from uk_pre_customer_is.td_order_product_p2 as a
                    inner join (select id,
                                       accountnumber,
                                       portfolioid,
                                       rn
                                  from (select id,
                                               a.accountnumber,
                                               a.portfolioid,
                                               row_number() over (partition by a.portfolioid
                                                                      order by a.lastupdate asc) as rn
                                          from uk_inp_tds_chordiant_eod_is.cc_bsbbillingaccount as a
                                         inner join uk_pub_customer_is.wh_cust_account_fo b 
                                            on (a.accountnumber  =b.account_number)
                                         inner join uk_pub_customer_is.wh_cust_account_bo c 
                                            on (b.src_system_id= c.fo_src_system_id)
                                         where a.logically_deleted = 0
                                           and upper(c.account_category) = 'RESIDENTIAL') foo) bap 
                                    on (    bap.portfolioid = a.portfolio_id 
                                        and bap.rn = 1)
                                 where a.order_id in(select order_id
                                                       from(select order_id,
                                                                   count(distinct account_number)
                                                              from uk_pre_customer_is.td_order_product_p3
                                                             group by 1
                                                            having count(distinct account_number) > 1) abc))def
    on td.order_id = def.order_id;	