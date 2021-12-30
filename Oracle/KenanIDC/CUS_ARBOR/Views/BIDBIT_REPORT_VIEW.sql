CREATE OR REPLACE VIEW CUS_ARBOR.BIDBIT_REPORT_VIEW AS
SELECT /*+ parallel (bid 8 8 )  */
    bi.account_no                                                                                                              AS kenan_account_no,
    ciam.external_id                                                                                                           AS primary_account_id,
    c.bill_company                                                                                                             AS bill_company,
    c.owning_cost_ctr                                                                                                          AS occ,
    occv.display_value                                                                                                         AS occ_description,
    cus_arbor.bidbit_package.get_geocode_description(c.cust_geocode)                                                           AS customer_location,
    c.cust_geocode                                                                                                             AS customer_geocode,
    cus_arbor.bidbit_package.get_geocode_description(c.bill_geocode)                                                           AS billing_location,
    c.bill_geocode                                                                                                             AS billing_geocode,
    bi.statement_date                                                                                                          AS statement_date,
    bi.bill_ref_no                                                                                                             AS bill_ref_no,
    bi.bill_ref_resets                                                                                                         AS bill_ref_resets,
    rcv.display_value                                                                                                          AS invoice_currency_description,
    bid.subscr_no                                                                                                              AS kenan_service_id,
    bid.bill_class                                                                                                             AS bill_class,
    ciem1.external_id                                                                                                          AS primary_service_id,
    ciem5.external_id                                                                                                          AS piid,
    ciem221.external_id                                                                                                        AS container_piid,
    ciem222.external_id                                                                                                        AS trunk_group_id,
    cus_arbor.bidbit_package.get_geocode_description(s.service_geocode)                                                        AS service_a_location,
    s.service_geocode                                                                                                          AS service_a_geocode,
    cus_arbor.bidbit_package.get_geocode_description(s.b_service_geocode)                                                      AS service_b_location,
    s.b_service_geocode                                                                                                        AS service_b_geocode,
    bid.bill_invoice_row                                                                                                       AS bill_invoice_row,
    DECODE (bid.type_code, 2,'RC', 3,'NRC', 7,'USAGE', 4,'ADJUSTMENT', 'UNKNOWN' )                                             AS charge_type,
    bid.component_Id                                                                                                           AS component_Id,
    d.description_text                                                                                                         AS bid_description,
    NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code)                                                   AS charge_id,
    CASE WHEN bid.type_code = 2 THEN
           (SELECT d.description_text
              FROM product_elements pe JOIN descriptions d ON d.description_code = pe.description_code
             WHERE pe.element_id = NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code)
               AND d.language_code = 1)
         WHEN bid.type_code = 3 THEN
           (SELECT d.description_text
              FROM nrc_trans_descr ntd JOIN descriptions d ON d.description_code = ntd.description_code
             WHERE ntd.type_id_nrc = NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code)
               AND d.language_code = 1)
         WHEN bid.type_code = 7 THEN
           (SELECT d.description_text
              FROM usage_types ut JOIN descriptions d ON d.description_code = ut.description_code
             WHERE ut.type_id_usg = NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code)
               AND d.language_code = 1)
    END                                                                                                                        AS charge_description,
    cus_arbor.bidbit_package.get_taxable_location(bid.type_code,NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id)
      ,bid.subtype_code),bid.billing_level)                                                                                    AS charge_taxable_location,
    NVL(TO_CHAR(crs.revenue_percent/(POWER(10, crs.implied_decimals))),'N/A')                                                  AS revenue_percent,
    CASE
      WHEN (SELECT COUNT(1) FROM cus_arbor.qvr_mapping qvr WHERE qvr.component_id = bid.component_id) = 0
        THEN 'N/A'
      ELSE TO_CHAR(crs.qvr_value)
    END                                                                                                                        AS qvr_value,
    bidbit_package.get_commtax_codes(bid.type_code,
                                     NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code),
                                     bid.rate_type)                                                                            AS charge_commtax_coding,
    bidbit_package.get_line_count(bid.type_code,bid.subtype_code,bid.rate_type,bid.tracking_id,bid.tracking_id_serv)           AS charge_line_count,
    CASE WHEN bid.type_code = 2 THEN DECODE(bid.prorate_code, 0,'UNPRORATED ADVANCE CYCLE',  
                                                              1,'PRORATED FROM CYCLE START',
                                                              2,'PRORATED TO CYCLE END',
                                                              3,'PRORATED WITHIN CYCLE',
                                                              4,'UNPRORATED ARREARS CYCLE',
                                                              'UNKNOWN')
         ELSE 'N/A' END                                                                                                        AS rc_proration,
    DECODE(bid.billing_level, 0,'ACCOUNT', 1,'SERVICE', 2,'SERVICE_INSTANCE_GROUP', 'UNKNOWN')                                 AS charge_billing_level,
    bid.rate_type                                                                                                              AS usage_jurisdiction_id,
    d2.description_text                                                                                                        AS usage_jurisdiction_description,
    bid.amount/(POWER(10,rcr.implied_decimal))                                                                                 AS charge_amount,
    bid.discount/(POWER(10,rcr.implied_decimal))                                                                               AS discount_amount,
    bid.from_date                                                                                                              AS charge_from_date,
    bid.to_date                                                                                                                AS charge_to_date,
    NVL(bit.geocode,bid.geocode)                                                                                               AS taxed_geocode,
    cus_arbor.bidbit_package.get_geocode_description(NVL(bit.geocode,bid.geocode))                                             AS taxed_location,
    UPPER(NVL(TRIM(tpiiv_bit.display_value),TRIM(tpiiv_bid.display_value)))                                                    AS taxing_package,
    NVL(bit.tax_type_code,bid.tax_type_code)                                                                                   AS tax_type_code,
    NVL(bit.federal_tax,bid.federal_tax)/POWER(10,rcr.implied_decimal)                                                         AS federal_tax,
    NVL(bit.state_tax,bid.state_tax)/POWER(10,rcr.implied_decimal)                                                             AS state_tax,
    NVL(bit.county_tax,bid.county_tax)/POWER(10,rcr.implied_decimal)                                                           AS county_tax,
    NVL(bit.city_tax,bid.city_tax)/POWER(10,rcr.implied_decimal)                                                               AS city_tax,
    NVL(bit.other_tax,bid.other_tax) / POWER(10,rcr.implied_decimal)                                                           AS other_tax,
    bid.tracking_id                                                                                                            AS tracking_id,
    bid.tracking_id_serv                                                                                                       AS tracking_id_serv,
    bid.subscr_no                                                                                                              AS subscr_no,
    bid.subscr_no_resets                                                                                                       AS subscr_no_resets
  FROM bill_invoice bi
    INNER JOIN bill_invoice_detail bid ON bid.bill_ref_no = bi.bill_ref_no
      AND bid.bill_ref_resets = bi.bill_ref_resets
    INNER JOIN descriptions d ON d.description_code = bid.description_code AND d.language_code = 1
    LEFT OUTER JOIN bill_invoice_tax bit ON bit.bill_ref_no = bid.bill_ref_no
      AND bit.bill_ref_resets = bid.bill_ref_resets
      AND bit.bill_invoice_row = bid.bill_invoice_row
    LEFT OUTER JOIN service s ON s.subscr_no = bid.subscr_no
      AND s.subscr_no_resets = bid.subscr_no_resets
    INNER JOIN cmf c ON c.account_no = bi.account_no
    INNER JOIN customer_id_acct_map ciam ON ciam.account_no = c.account_no
      AND ciam.external_id_type = 1
      AND ciam.is_current = 1
    LEFT OUTER JOIN customer_id_acct_map ciam150 ON ciam150.account_no = c.account_no
      AND ciam150.external_id_type = 150
      AND ciam150.is_current = 1
    LEFT OUTER JOIN customer_id_equip_map ciem1 ON ciem1.subscr_no = s.subscr_no
      AND ciem1.subscr_no_resets = s.subscr_no_resets
      AND ciem1.external_id_type = 1
      AND ciem1.is_current = 1
    LEFT OUTER JOIN customer_id_equip_map ciem5 ON ciem5.subscr_no = s.subscr_no
      AND ciem5.subscr_no_resets = s.subscr_no_resets
      AND ciem5.external_id_type = 5
      AND ciem5.is_current = 1
    LEFT OUTER JOIN customer_id_equip_map ciem221 ON ciem221.subscr_no = s.subscr_no
      AND ciem221.subscr_no_resets = s.subscr_no_resets
      AND ciem221.external_id_type = 221
      AND ciem221.is_current = 1
    LEFT OUTER JOIN customer_id_equip_map ciem222 ON ciem222.subscr_no = s.subscr_no
      AND ciem222.subscr_no_resets = s.subscr_no_resets
      AND ciem222.external_id_type = 222
      AND ciem222.is_current = 1
    INNER JOIN owning_cost_ctr_values occv ON occv.owning_cost_ctr = c.owning_cost_ctr
      AND occv.language_code = 1
    INNER JOIN rate_currency_ref rcr ON rcr.currency_code = bi.currency_code
    INNER JOIN rate_currency_values rcv ON rcv.currency_code = rcr.currency_code
      AND rcv.language_code = 1
    INNER JOIN tax_pkg_inst_id_values tpiiv_bid ON tpiiv_bid.tax_pkg_inst_id = bid.tax_pkg_inst_id
      AND tpiiv_bid.language_code = 1
    LEFT OUTER JOIN tax_pkg_inst_id_values tpiiv_bit ON tpiiv_bit.tax_pkg_inst_id = bit.tax_pkg_inst_id
      AND tpiiv_bit.language_code = 1
    LEFT OUTER JOIN jurisdictions j ON j.jurisdiction = bid.rate_type
    LEFT OUTER JOIN descriptions d2 ON d2.description_code = j.description_code
      AND d2.language_code = 1
    LEFT OUTER JOIN charge_revenue_split crs ON crs.charge_elt_id = bid.subtype_code
      AND  DECODE(bid.type_code,2,1,3,2,7,3,0) = crs.charge_elt_type
      AND ((crs.subscr_no = bid.subscr_no AND crs.subscr_no_resets = bid.subscr_no_resets
              AND NVL(bit.rev_split_charge_id,crs.child_charge_id) = crs.child_charge_id) OR
           (crs.subscr_no = 0 AND crs.subscr_no_resets = 0
              AND NVL(bit.rev_split_charge_id,crs.child_charge_id) = crs.child_charge_id))
    LEFT OUTER JOIN adj_trans_descr atd ON atd.adj_trans_code = NVL(NVL(bit.rev_split_charge_id, crs.child_charge_id), bid.subtype_code)
WHERE bid.bill_ref_resets = 0
  AND (bid.type_code IN (2, 3, 7) OR (bid.type_code = 4 AND atd.adj_trans_category NOT IN (6,7) AND atd.is_negative_bill_adj != 1 ));
