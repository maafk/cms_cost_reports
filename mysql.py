import MySQLdb
import time

class DbHcris(object):
    """docstring for DB"""
    def __init__(self, user="root", passwd="", host="127.0.0.1", port=8889):
        
        self.db = MySQLdb.connect(
            host = host,
            user = user,
            passwd = passwd,
            db = "hcris",
            port = port,
            local_infile = 1
        )

        self._user        = user
        self._pass        = passwd
        self._host        = host
        self._port        = port  
        # sub in mysql variables
        self._schema      = 'hcris'
        self._rpt         = '{0}{1}'.format(self._schema,'.RPT')
        self._rpt_aplha   = '{0}{1}'.format(self._schema,'.RPT_ALPHA')
        self._rpt_nmrc    = '{0}{1}'.format(self._schema,'.RPT_NMRC')
        self._rollup      = '{0}{1}'.format(self._schema,'.ROLLUP')
        self._bd          = '{0}{1}'.format(self._schema,'.bd_analysis')

        self.cur = self.db.cursor()

    # Table creation
    def create_hcris_tables(self):
        self.create_RPT()
        self.create_RPT_ALPHA()
        self.create_RPT_NMRC()
        self.create_ROLLUP()

    def create_RPT(self):
        sql = """
        CREATE TABLE {0} (
          id int(10) NOT NULL,
          PRVDR_CTRL_TYPE_CD char(2) DEFAULT NULL,
          PRVDR_NUM char(6) NOT NULL,
          NPI char(10) DEFAULT NULL,
          RPT_STUS_CD char(1) NOT NULL,
          FY_BGN_DT date DEFAULT NULL,
          FY_END_DT date DEFAULT NULL,
          PROC_DT date DEFAULT NULL,
          INITL_RPT_SW char(1) DEFAULT NULL,
          LAST_RPT_SW char(1) DEFAULT NULL,
          TRNSMTL_NUM char(2) DEFAULT NULL,
          FI_NUM char(5) DEFAULT NULL,
          ADR_VNDR_CD char(1) DEFAULT NULL,
          FI_CREAT_DT date DEFAULT NULL,
          UTIL_CD char(1) DEFAULT NULL,
          NPR_DT date DEFAULT NULL,
          SPEC_IND char(1) DEFAULT NULL,
          FI_RCPT_DT date DEFAULT NULL,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """.format(self._rpt)
        # execute query to create
        self.cur.execute(sql)
        self.db.commit()

    def create_RPT_ALPHA(self):
        sql = """
        CREATE TABLE {0} (
          id int(10) NOT NULL AUTO_INCREMENT,
          rpt_id int(10) NOT NULL,
          WKSHT_CD char(7) NOT NULL,
          LINE_NUM char(5) NOT NULL,
          CLMN_NUM char(5) NOT NULL,
          ALPHNMRC_ITM_TXT varchar(40) NOT NULL,
          PRIMARY KEY (id),
          KEY idx_rpt_rec_num (rpt_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """.format(self._rpt_aplha)
        # execute query to create
        self.cur.execute(sql)
        self.db.commit()

    def create_RPT_NMRC(self):
        sql = """
        CREATE TABLE {0} (
          id int(10) NOT NULL AUTO_INCREMENT,
          rpt_id int(10) NOT NULL,
          WKSHT_CD char(7) NOT NULL,
          LINE_NUM char(5) NOT NULL,
          CLMN_NUM char(5) NOT NULL,
          ITM_VAL_NUM double NOT NULL,
          PRIMARY KEY (id),
          KEY idx_rpt_rec_num (rpt_id,WKSHT_CD)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """.format(self._rpt_nmrc)
        # execute query to create
        self.cur.execute(sql)
        self.db.commit()

    def create_ROLLUP(self):
        sql = """
        CREATE TABLE {0} (
          id int(10) NOT NULL AUTO_INCREMENT,
          rpt_id int(10) NOT NULL,
          LABEL varchar(30) NOT NULL,
          ITEM decimal(14,2) NOT NULL,
          PRIMARY KEY (id),
          KEY idx_rpt_rec_num (rpt_id),
          CONSTRAINT fk_ROLLUP_RPT FOREIGN KEY (rpt_id) REFERENCES RPT (id) ON DELETE NO ACTION ON UPDATE NO ACTION
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """.format(self._rollup)
        # execute query to create
        self.cur.execute(sql)
        self.db.commit()

    def drop_hcris_tables(self):
        self.drop_table(self._rollup)
        self.drop_table(self._rpt_nmrc)
        self.drop_table(self._rpt_aplha)
        self.drop_table(self._bd)
        self.drop_table(self._rpt)

    def drop_table(self,table):
        sql = 'drop table if exists {}'.format(table)
        self.cur.execute(sql)
        self.db.commit()    

    def load_hcris_RPT(self,path_to_file):
        sql = """
        load data local infile '{0}'
        into table {1}
        fields terminated by ','
        lines terminated by '\\r\\n'
        (
            id,
            PRVDR_CTRL_TYPE_CD,
            PRVDR_NUM,
            NPI,
            RPT_STUS_CD,
            @FY_BGN_DT,
            @FY_END_DT,
            @PROC_DT,
            INITL_RPT_SW,
            LAST_RPT_SW,
            TRNSMTL_NUM,
            FI_NUM,
            ADR_VNDR_CD,
            @FI_CREAT_DT,
            UTIL_CD,
            @NPR_DT,
            SPEC_IND,
            @FI_RCPT_DT
        )
        set
            FY_BGN_DT = str_to_date(@FY_BGN_DT,'%m/%d/%Y'),
            FY_END_DT = str_to_date(@FY_END_DT,'%m/%d/%Y'),
            PROC_DT = str_to_date(@PROC_DT,'%m/%d/%Y'),
            FI_CREAT_DT = str_to_date(@FI_CREAT_DT,'%m/%d/%Y'),
            NPR_DT = str_to_date(@NPR_DT,'%m/%d/%Y'),
            FI_RCPT_DT = str_to_date(@FI_RCPT_DT,'%m/%d/%Y')
        """.format(path_to_file,self._rpt)

        # start_time = time.time()
        self.cur.execute(sql)
        self.db.commit()
        # duration = time.time()-start_time
        print '{0} loaded sucessfully to {1}\\n'.format(path_to_file,self._rpt.split('/')[-1])

    def load_hcris_ALPHA(self, path_to_file):
        sql = """
        load data local infile '{0}'
        into table {1}
        fields terminated by ','
        lines terminated by '\\r\\n'
        (
            rpt_id,
            WKSHT_CD,
            LINE_NUM,
            CLMN_NUM,
            ALPHNMRC_ITM_TXT
        )
        """.format(path_to_file,self._rpt_aplha)
        
        self.cur.execute(sql)
        self.db.commit()
        print '{0} loaded sucessfully to {1}'.format(path_to_file,self._rpt_aplha.split('/')[-1])

    def load_hcris_NMRC(self, path_to_file):
        sql = """
        load data local infile '{0}'
        ignore into table {1}
        fields terminated by ','
        lines terminated by '\\r\\n'
        (
            rpt_id,
            WKSHT_CD,
            LINE_NUM,
            CLMN_NUM,
            ITM_VAL_NUM
        )
        """.format(path_to_file,self._rpt_nmrc)
        
        self.cur.execute(sql)
        self.db.commit()
        print '{0} loaded sucessfully to {1}'.format(path_to_file,self._rpt_nmrc.split('/')[-1])

    def load_hcris_ROLLUP(self, path_to_file):
        print 'Check out the rollup file because we actually have one'

    def add_identifier_to_RPT(self):
        
        sql = """
            ALTER TABLE {0}
            ADD COLUMN `name` VARCHAR(75) NOT NULL DEFAULT '' AFTER `PRVDR_NUM`,
            ADD COLUMN `address` VARCHAR(75) NOT NULL DEFAULT '' AFTER `name`,
            ADD COLUMN `city` VARCHAR(45) NOT NULL DEFAULT '' AFTER `address`,
            ADD COLUMN `state` VARCHAR(5) NOT NULL DEFAULT '' AFTER `city`,
            ADD COLUMN `zip` VARCHAR(15) NOT NULL DEFAULT '' AFTER `state`
        """.format(self._rpt)
        self.cur.execute(sql)
        self.db.commit()

        self.update_RPT_name()
        self.update_RPT_address()        
        self.update_RPT_city()
        self.update_RPT_state()
        self.update_RPT_zip()
        print 'Updated RPT table with facility identifiers'

    def update_RPT_name(self):
        sql = """
            UPDATE  
            {0} r,
            {1} a
            SET
                r.name = ALPHNMRC_ITM_TXT
            WHERE
                r.id = a.rpt_id            
            AND a.WKSHT_CD = 'S200001'
            AND a.LINE_NUM = '00300' 
            AND a.CLMN_NUM = '00100'
        """.format(self._rpt,self._rpt_aplha)
        self.cur.execute(sql)
        self.db.commit()

    def update_RPT_address(self):
        sql = """
            UPDATE  
            {0} r,
            {1} a
            SET
                r.address = ALPHNMRC_ITM_TXT
            WHERE
                r.id = a.rpt_id            AND a.WKSHT_CD = 'S200001'
            AND a.LINE_NUM = '00100' 
            AND a.CLMN_NUM = '00100'
        """.format(self._rpt,self._rpt_aplha)
        self.cur.execute(sql)
        self.db.commit()

    def update_RPT_city(self):
        sql = """
            UPDATE  
            {0} r,
            {1} a
            SET
                r.city = ALPHNMRC_ITM_TXT
            WHERE
                r.id = a.rpt_id            AND a.WKSHT_CD = 'S200001'
            AND a.LINE_NUM = '00200' 
            AND a.CLMN_NUM = '00100'
        """.format(self._rpt,self._rpt_aplha)
        self.cur.execute(sql)
        self.db.commit()

    def update_RPT_state(self):
        sql = """
            UPDATE  
            {0} r,
            {1} a
            SET
                r.state = ALPHNMRC_ITM_TXT
            WHERE
                r.id = a.rpt_id            AND a.WKSHT_CD = 'S200001'
            AND a.LINE_NUM = '00200' 
            AND a.CLMN_NUM = '00200'
        """.format(self._rpt,self._rpt_aplha)
        self.cur.execute(sql)
        self.db.commit()

    def update_RPT_zip(self):
        sql = """
            UPDATE  
            {0} r,
            {1} a
            SET
                r.zip = ALPHNMRC_ITM_TXT
            WHERE
                r.id = a.rpt_id            AND a.WKSHT_CD = 'S200001'
            AND a.LINE_NUM = '00200' 
            AND a.CLMN_NUM = '00300'
        """.format(self._rpt,self._rpt_aplha)
        self.cur.execute(sql)
        self.db.commit()