Every Hospital in the United States must file a Medicare Cost Report once a year. This data is then provided to the public. It is updated once a quarter.

[Info on the form](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Hospital-2010-form.html)
[Info on the data](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Cost-Reports-by-Fiscal-Year.html)

This project aims to simplify the process of pulling this public data and storing in a database (currently only MySQL)

This is most easily reproducible when in a Python [Virtual Environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)

    mkvirtualenv cms_cost_reports
    workon cms_cost_reports

Clone this project, and install the required packages

    git clone https://github.com/maafk/cms_cost_reports.git
    cd cms_cost_reports
    pip install -r requirements.txt

The entry point is a bash scrpt `init.sh`
Inside you'll find the following flags. Note options `f` through `h` are bleeding edge and meant more for analysis 

    a - clear databases and start from scratch
    b - elete zip and csv files
    c - download hcris zip
    d - unzip csvs
    e - load data from csv files
    f - add stored procedures
    g - add identifiers to rpt table
    h - add bd analysis tabled
 
 Example
 
     ./init.sh -abcde
 You'll be prompted for MySQL credentials. At this point the user must have full privileges.