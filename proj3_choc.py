import sqlite3
import csv
import json

import sys
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'



conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

## create Countries table
statement = '''
CREATE TABLE Countries (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Alpha2 TEXT,
    Alpha3 TEXT,
    EnglishName TEXT,
    Region TEXT,
    Subregion TEXT,
    Population INTEGER,
    Area REAL
)
'''
try:
    cur.execute(statement)
except:
    pass

## populate Countries Table
f = open(COUNTRIESJSON, 'r')
f_text = f.read()
countries_json = json.loads(f_text)
f.close()
fields = ['alpha2Code', 'alpha3Code', 'name', 'region', 'subregion', 'population', 'area']

for item in countries_json:
    item_values = []
    for field in fields:
        if field in item.keys():
            item_values.append(item[field])
        else:
            item_values.append('')
    statement = '''
    INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)
    VALUES (?,?,?,?,?,?,?)
    '''
    cur.execute(statement, item_values)
    

# ## create Bars table
# statement = '''
# CREATE TABLE Bars (
#     Id INTEGER PRIMARY KEY AUTOINCREMENT,
#     Company TEXT,
#     SpecificBeanBarName TEXT,
#     REF TEXT,
#     ReviewDate TEXT,
#     CocoaPercent REAL,
#     CompanyLocationId INTEGER,
#     Rating REAL,
#     BeanType TEXT,
#     BroadBeanOriginId INTEGER
#     )
# '''
# cur.execute(statement)

# ## populate Bars table
# with open(BARSCSV) as csvDataFile:
#     csvReader = csv.reader(csvDataFile)
#     for row in csvReader:
#         items = []
#         for i in range()

#         statement = '''
#         INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#         '''
#         cur.execute(statement, items)
#         print("added!")
#     csvDataFile.close()

conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    return []


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()
