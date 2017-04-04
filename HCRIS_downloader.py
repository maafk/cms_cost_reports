from datetime import date
import os
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import zipfile

# import mysql as m
from mysql import DbHcris

HOME_URL = "http://downloads.cms.gov/FILES/HCRIS/"
ZIP_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'ZIP/'))
CSV_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'CSV/'))

if not os.path.exists(ZIP_PATH):
    os.makedirs(ZIP_PATH)
if not os.path.exists(CSV_PATH):
    os.makedirs(CSV_PATH)

def main():
    
    # delete and data in zip or csv dir
    if sys.argv[1] == "delete_zip_and_csv":
        delete_zip_and_csv_files()
  
    # download hcris data
    if sys.argv[1] == "get_hcris_data":
        get_hcris_data_from_cms_website()

    # Unzip files just downloaded to the CSV path
    if sys.argv[1] == "unzip":
        unzip_files_in_dir(ZIP_PATH,CSV_PATH)

    # drop old hcris tables and create fresh ones
    if sys.argv[1] == "create_clean_hcris_tables":
        create_clean_hcris_tables()

    # Load csv data into mysql database
    if sys.argv[1] == "load_csvs_to_db":
        load_csvs_to_db()

    if sys.argv[1] == "add_procedures": 
        m = DbHcris(sys.argv[2], sys.argv[3],sys.argv[4], int(sys.argv[5]))
        m.add_stored_procedures()

    if sys.argv[1] == "add_identifier": 
        m = DbHcris(sys.argv[2], sys.argv[3],sys.argv[4], int(sys.argv[5]))
        m.add_identifier_to_RPT()
        
def delete_zip_and_csv_files():
    # clear Zip files and start fresh
    remove_files_in_dir(ZIP_PATH)

    # clear CSV files and start fresh
    remove_files_in_dir(CSV_PATH)

def get_hcris_data_from_cms_website():

    # Start from where user said they wanted to
    for y in range(2012,date.today().year):
        
        post_10 = 'HOSP10FY{0}.zip'.format(y)
        
        # Attempt to open the post_10 url for download
        r = requests.get('{0}{1}'.format(HOME_URL,post_10), stream = True)
        
        # make sure the link is valid
        if r.status_code == 200:
            
            # If link is valid, download
            download_file(r,post_10)
            
            # show it worked
            print '{0} Was downloaded'.format(post_10)
    # Alert everything is finished
    print 'All finished downloading'
        
def download_file(r,local_filename):
    with open("{0}/{1}".format(ZIP_PATH,local_filename), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

def unzip_files_in_dir(source_dir,destination_dir):

    for subdir, dirs, files in os.walk(source_dir):
        for file in files:
            if file[:1] == ".":
                continue
            with zipfile.ZipFile(os.path.join(subdir, file), "r") as z:
                z.extractall(destination_dir)
                print 'extracted {0}'.format(file)

def remove_files_in_dir(dir):
    filelist = [ f for f in os.listdir(dir)]
    for f in filelist:
        if f.startswith('.'):
            continue
        os.remove(os.path.join(dir,f))
        print 'deleted {0}'.format(f)

def create_clean_hcris_tables():
    m = DbHcris(sys.argv[2], sys.argv[3],sys.argv[4], int(sys.argv[5]))  
    m.drop_hcris_tables()
    m.create_hcris_tables()

def load_csvs_to_db():
    m = DbHcris(sys.argv[2], sys.argv[3],sys.argv[4], int(sys.argv[5]))
    filelist = [ f for f in os.listdir(CSV_PATH)]
    # Sort the csvs by substr reverse alphabetical order
    # We do this so that the RPT table is loaded first
    filelist.sort(key=lambda x:(x.split('_')[-1]),reverse=True)

    for f in filelist:
        if f.startswith('.'):
            continue
        path = os.path.join(CSV_PATH,f)
        hcris_table = f.split('_')[-1]
        # returns table to be loaded
        print hcris_table
        hcris_table = hcris_table.split('.')[0]
        func = 'load_hcris_{0}'.format(hcris_table)
        result = getattr(m, func)(path)

if __name__ == '__main__':
    main()