USE `hcris`;
DROP procedure IF EXISTS `sp_list_hospitals`;

DELIMITER $$
USE `hcris`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_list_hospitals`()
BEGIN
    set @sql = CONCAT("
        SELECT 
            `RPT`.`PRVDR_NUM`,
            `RPT`.`name`
            address,
            city,
            state,
            zip
        FROM `hcris`.`RPT`
            GROUP BY PRVDR_NUM
            ORDER BY name
    ");
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;

DROP procedure IF EXISTS `sp_get_available_cost_reports_by_hospital`;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_available_cost_reports_by_hospital`(IN hosp int)
BEGIN
    set @sql = CONCAT("
        SELECT 
            `RPT`.`id`,
            `RPT`.`FY_END_DT`
        FROM `hcris`.`RPT`
        where PRVDR_NUM = ",hosp,"
    ");
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;


DROP procedure IF EXISTS `sp_get_provider_and_sub_provider_by_rpt_id`;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_provider_and_sub_provider_by_rpt_id`(in rpt_id int)
BEGIN
    set @sql = CONCAT("
        SELECT 
            `RPT_ALPHA`.`ALPHNMRC_ITM_TXT`
        FROM `hcris`.`RPT_ALPHA`

        WHERE id = ",rpt_id,"
        AND WKSHT_CD ='S200001'
        AND CLMN_NUM = '00100'
        AND LINE_NUM between 300 and 1999
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_get_inpatient_areas_by_rpt_id; 
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_inpatient_areas_by_rpt_id`(IN rpt_id INT)
BEGIN
    set @sql = CONCAT("
SELECT 
    CASE n.`LINE_NUM`
        WHEN '00100' THEN 'HOSPITAL ADULTS & PEDS'
        WHEN '00800' THEN 'INTENSIVE CARE UNIT'
        WHEN '01600' THEN 'SUBPROVIDER - IPF'
        WHEN '01900' THEN 'SKILLED NURSING FACILITY'
        ELSE ''
    END as inpatient_area,
    n.`ITM_VAL_NUM`
FROM `hcris`.`RPT_NMRC` n

WHERE n.rpt_id = ",rpt_id,"
AND n.WKSHT_CD = 'S300001'
AND n.CLMN_NUM = '00200'
AND n.LINE_NUM between 100 and 2600
AND n.LINE_NUM not in
(
    '00700',    -- toal
    '01400',    -- total
    '02700'
)
    ");
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_get_ip_payor_mix_by_rpt_id;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_ip_payor_mix_by_rpt_id`(IN rpt_id INT)
BEGIN
    
    set @sql = CONCAT("
    SELECT
        n.`ITM_VAL_NUM` into @medicare_days
        
        FROM `hcris`.`RPT_NMRC` n

        WHERE n.rpt_id = ",rpt_id,"
        AND n.WKSHT_CD = 'S300001'
        AND n.CLMN_NUM = '00600'
        AND n.LINE_NUM = '01400'
    ");
    
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    set @sql = CONCAT("
        SELECT
        n.`ITM_VAL_NUM` into @medicaid_days
        
        FROM `hcris`.`RPT_NMRC` n

        WHERE n.rpt_id = ",rpt_id,"
        AND n.WKSHT_CD = 'S300001'
        AND n.CLMN_NUM = '00700'
        AND n.LINE_NUM = '01400'
    ");
    
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    set @sql = CONCAT("
        SELECT
        n.`ITM_VAL_NUM` into @total_days
        
        FROM `hcris`.`RPT_NMRC` n

        WHERE n.rpt_id = ",rpt_id,"
        AND n.WKSHT_CD = 'S300001'
        AND n.CLMN_NUM = '00800'
        AND n.LINE_NUM = '01400'
    ");

    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    set @medicare_percent = round(@medicare_days / @total_days,4);
    set @medicaid_percent = round(@medicaid_days / @total_days,4);
    select @medicare_percent as medicare_percent,  @medicaid_percent as medicaid_percent, @total_days as total_days  ;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_get_ip_allowable_bad_debt;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_ip_allowable_bad_debt`(IN rpt_id INT)
BEGIN
    set @sql = CONCAT("
    SELECT
    CASE `LINE_NUM`
        WHEN '06400' THEN 'ip_allowable'
        WHEN '06600' THEN 'ip_de'
        ELSE ''
    END as description,
        `ITM_VAL_NUM`
    FROM `hcris`.`RPT_NMRC`
    where rpt_id = ",rpt_id,"
    and 
    WKSHT_CD = 'E00A18A'
    and LINE_NUM in ('06400','06600')
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_get_op_allowable_bad_debt;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_op_allowable_bad_debt`(IN rpt_id INT)
BEGIN
    set @sql = CONCAT("
    SELECT
    CASE `LINE_NUM`
        WHEN '03400' THEN 'op_allowable'
        WHEN '03600' THEN 'op_de'
        ELSE ''
    END as description,
        `ITM_VAL_NUM`
    FROM `hcris`.`RPT_NMRC`
    where rpt_id = ",rpt_id,"
    and 
    WKSHT_CD = 'E00A18B'
    and LINE_NUM in ('03400','03600')
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS sp_get_revenue_mix_by_rpt_id;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_revenue_mix_by_rpt_id`(IN rpt_id INT)
BEGIN
    set @sql = CONCAT("
    SELECT
        `ITM_VAL_NUM` into @ip_charges
    FROM `hcris`.`RPT_NMRC`

    where rpt_id = ",rpt_id,"
    AND WKSHT_CD = 'G200000'
    AND LINE_NUM = '02800'
    AND CLMN_NUM = '00100'
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @sql = CONCAT("
    SELECT
        `ITM_VAL_NUM` into @op_charges
    FROM `hcris`.`RPT_NMRC`

    where rpt_id = ",rpt_id,"
    AND WKSHT_CD = 'G200000'
    AND LINE_NUM = '02800'
    AND CLMN_NUM = '00200'
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @sql = CONCAT("
    SELECT
        `ITM_VAL_NUM` into @total_charges
    FROM `hcris`.`RPT_NMRC`

    where rpt_id = ",rpt_id,"
    AND WKSHT_CD = 'G200000'
    AND LINE_NUM = '02800'
    AND CLMN_NUM = '00300'
    ");
    
    prepare stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    set @ip_percent = round(@ip_charges / @total_charges,4);
    set @op_percent = round(@op_charges / @total_charges,4);
    select @ip_percent as ip_percent,  @op_percent as op_percent, @total_charges as total_charges  ;
END$$
DELIMITER ;
