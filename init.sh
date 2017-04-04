#!/bin/bash
#	a - clear databases and start from scratch
#	b - delete zip and csv files
#	c - download hcris zip
#	d - unzip csvs
#	e - load data from csv files
#	f - add stored procedures
#	g - add identifiers to rpt table#
#	h - add bd analysis table
#
while getopts "abcdefghi:" o; do
    case "${o}" in
        a)
			read -p "mysql user: " user
			read -s -p "mysql password for $user: " pass
			read -p "host: " host
			read -p "mysql port to use: " port
			`mysql -h $host -u $user -p$pass -P $port -e "create schema if not exists hcris"`
            echo `python HCRIS_downloader.py create_clean_hcris_tables $user $pass $host $port`
            ;;
        b)
            echo `python HCRIS_downloader.py delete_zip_and_csv`
            ;;
        c)
            echo `python HCRIS_downloader.py get_hcris_data`
            ;;
        d)
            echo `python HCRIS_downloader.py unzip`
            ;;
        e)
			if [ -z "${user+xxx}" ];
			then
				read -p "mysql user: " user
			fi
			if [ -z "${pass+xxx}" ];
			then
				read -s -p "mysql password for $user: " pass
			fi	
			if [ -z "${host+xxx}" ];
			then
				read -p "host: " host
			fi	
			if [ -z "${port+xxx}" ];
			then
				read -p "mysql port to use: " port
				`mysql -h $host -u $user -p$pass -P $port -e "create schema if not exists hcris"`
			fi	
            echo `python HCRIS_downloader.py load_csvs_to_db $user $pass $host $port`
            ;;
        f)
			if [ -z "${user+xxx}" ];
			then
				read -p "mysql user: " user
			fi
			if [ -z "${pass+xxx}" ];
			then
				read -s -p "mysql password for $user: " pass
			fi	
			if [ -z "${host+xxx}" ];
			then
				read -p "host: " host
			fi	
			if [ -z "${port+xxx}" ];
			then
				read -p "mysql port to use: " port
			fi	
            `mysql -h $host -u $user -p$pass -P $port < add_stored_procedures.sql`
            ;;
        g)
			if [ -z "${user+xxx}" ];
			then
				read -p "mysql user: " user
			fi
			if [ -z "${pass+xxx}" ];
			then
				read -s -p "mysql password for $user: " pass
			fi	
			if [ -z "${host+xxx}" ];
			then
				read -p "host: " host
			fi	
			if [ -z "${port+xxx}" ];
			then
				read -p "mysql port to use: " port
			fi	
            echo `python HCRIS_downloader.py add_identifier $user $pass $host $port`
            ;;
        h)
			if [ -z "${user+xxx}" ];
			then
				read -p "mysql user: " user
			fi
			if [ -z "${pass+xxx}" ];
			then
				read -s -p "mysql password for $user: " pass
			fi	
			if [ -z "${host+xxx}" ];
			then
				read -p "host: " host
			fi	
			if [ -z "${port+xxx}" ];
			then
				read -p "mysql port to use: " port
			fi	
        `mysql -h $host -u $user -p$pass -P $port hcris < bd_table.sql`
        ;;
    esac
done