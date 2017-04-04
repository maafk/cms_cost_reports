CREATE TABLE if not exists hcris.`bd_analysis` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `rpt_id` int(10) NOT NULL,
  `ideal_ip_bd` double default 0,
  `ideal_op_bd` double default 0,
  `bd_allowed_ip` double default 0,
  `bd_allowed_ip_de` double default 0,
  `bd_allowed_op` double default 0,
  `bd_allowed_op_de` double default 0,
  `mcare_ip_percent` decimal(5,4) default 0,
  `mcaid_ip_percent` decimal(5,4) default 0,
  `total_medicare_ip_days` int(11) default 0,
  `total_medicaid_ip_days` int(11) default 0,
  `total_ip_days` int(11) default 0,
  `ip_rev_percent` decimal(5,4) default 0,
  `op_rev_percent` decimal(5,4) default 0,
  `ip_charges` double default 0,
  `op_charges` double default 0,
  `total_charges` double default 0,
  PRIMARY KEY (`id`),
  KEY `idx_rpt_rec_num` (`rpt_id`),
  CONSTRAINT `fk_bd_analysis` FOREIGN KEY (`rpt_id`) REFERENCES `RPT` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB  DEFAULT CHARSET=latin1;

INSERT INTO `hcris`.`bd_analysis`
(`rpt_id`)
select id from hcris.RPT 
group by id;

/* add bd_allowed_ip */
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.bd_allowed_ip = n.ITM_VAL_NUM
WHERE
	b.rpt_id = n.rpt_id
AND
	n.WKSHT_CD = 'E00A18A'
AND
	n.LINE_NUM = '06400';

/* add bd_allowed_ip_de */
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.bd_allowed_ip_de = n.ITM_VAL_NUM
WHERE
	b.rpt_id = n.rpt_id
AND
	n.WKSHT_CD = 'E00A18A'
AND
	n.LINE_NUM = '06600';

/* add bd_allowed_op */
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.bd_allowed_op = n.ITM_VAL_NUM
WHERE
	b.rpt_id = n.rpt_id
AND
	n.WKSHT_CD = 'E00A18B'
AND
	n.LINE_NUM = '03400';

/* add bd_allowed_op_de */ 
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.bd_allowed_op_de = n.ITM_VAL_NUM
WHERE
	b.rpt_id = n.rpt_id
AND
	n.WKSHT_CD = 'E00A18B'
AND
	n.LINE_NUM = '03600';

/* **************************************** 
	add ip payor mix
*******************************************/
/* **************************************** 
	add total days
*******************************************/ 	
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.total_ip_days = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	n.WKSHT_CD = 'S300001'
AND 
	n.CLMN_NUM = '00800'
AND 
	n.LINE_NUM = '01400';

/* **************************************** 
	add medicare days
*******************************************/ 	
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.total_medicare_ip_days = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	n.WKSHT_CD = 'S300001'
AND 
	n.CLMN_NUM = '00600'
AND 
	n.LINE_NUM = '01400';

/* **************************************** 
	add medicaid days
*******************************************/ 	
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.total_medicaid_ip_days = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	n.WKSHT_CD = 'S300001'
AND 
	n.CLMN_NUM = '00700'
AND 
	n.LINE_NUM = '01400';
/* **************************************** 
	add medicare ip day percent
*******************************************/ 	
UPDATE 
	`hcris`.`bd_analysis`
SET
	mcare_ip_percent = if(total_ip_days = 0,0,round(total_medicare_ip_days / total_ip_days,4)),
	mcaid_ip_percent = if(total_ip_days = 0,0,round(total_medicaid_ip_days / total_ip_days,4));

/* **************************************** 
	add revenue mix
*******************************************/

/* **************************************** 
	Add ip charges
*******************************************/
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.ip_charges = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	WKSHT_CD = 'G200000'
AND 
	LINE_NUM = '02800'
AND 
	CLMN_NUM = '00100';

/* **************************************** 
	Add op charges
*******************************************/
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.op_charges = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	WKSHT_CD = 'G200000'
AND 
	LINE_NUM = '02800'
AND 
	CLMN_NUM = '00200';

/* **************************************** 
	Add total charges
*******************************************/
UPDATE 
	`hcris`.`bd_analysis` b,
	`hcris`.`RPT_NMRC` n
SET
	b.total_charges = n.ITM_VAL_NUM
WHERE 
	b.rpt_id = n.rpt_id
AND 
	WKSHT_CD = 'G200000'
AND 
	LINE_NUM = '02800'
AND 
	CLMN_NUM = '00300';

/* **************************************** 
	calculate percent
*******************************************/

UPDATE 
	`hcris`.`bd_analysis`
SET
	ip_rev_percent = if(total_charges = 0,0,round(ip_charges / total_charges,4)),
	op_rev_percent = if(total_charges = 0,0, round(op_charges / total_charges,4));