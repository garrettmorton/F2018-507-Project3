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

conn.commit()

## populate Countries Table
# check whether Countries is already populated
statement = '''SELECT Id FROM Countries'''
cur.execute(statement)
countrieslength = cur.fetchall()
if len(countrieslength) == 250:
    pass
    #print("Countries already imported!")

# if not alrady populated, populate Countries table
else:
    statement = '''DELETE FROM Countries'''
    cur.execute(statement)
    conn.commit()
    #print("Countries records deleted!")

    f = open(COUNTRIESJSON, 'r', encoding='utf8')
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
        #sql_input = tuple(item_values)
        statement = '''
        INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)
        VALUES (?,?,?,?,?,?,?)
        '''
        #print(sql_input)
        cur.execute(statement, item_values)

    conn.commit()
    

## create Bars table
statement = '''
CREATE TABLE Bars (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Company TEXT,
    SpecificBeanBarName TEXT,
    REF TEXT,
    ReviewDate TEXT,
    CocoaPercent REAL,
    CompanyLocationId INTEGER,
    Rating REAL,
    BeanType TEXT,
    BroadBeanOriginId INTEGER
    )
'''
try:
    cur.execute(statement)
except:
    pass

conn.commit()

## populate Bars table
# check whether Bars is already populated
statement = '''SELECT Id FROM Bars'''
cur.execute(statement)
barslength = cur.fetchall()
if len(barslength) == 1795:
    pass
    #print("Bars already imported!")

# if not alrady populated, populate Bars table
else:
    statement = '''DELETE FROM Bars'''
    cur.execute(statement)
    conn.commit()
    #print("Bars records deleted!")

    with open(BARSCSV) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            if row[0] != "Company":
                conn = sqlite3.connect(DBNAME)
                cur = conn.cursor()

                item_values = []
                for i in range(4):
                    item_values.append(row[i])

                #remove % from cocoa percentage
                percent = row[4][:-1]
                item_values.append(percent)

                #find CompanyLocationId
                statement = '''SELECT Id FROM Countries WHERE EnglishName="{}"'''.format(row[5])
                cur.execute(statement)
                result = cur.fetchone()
                if result != None:
                    locationid = result[0]
                else:
                    locationid = ''
                item_values.append(locationid)

                item_values.append(row[6])
                item_values.append(row[7])

                #find BroadBeanOriginId
                if row[8] == "Unknown":
                    item_values.append("")
                else:
                    statement = '''SELECT Id FROM Countries WHERE EnglishName="{}"'''.format(row[8])
                    cur.execute(statement)
                    result = cur.fetchone()
                    if result != None:
                        originid = result[0]
                    else:
                        originid = ''
                    item_values.append(originid)

                statement = '''
                INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                print(item_values)
                cur.execute(statement, item_values)
                conn.commit()
                conn.close()
        csvDataFile.close()



# Part 2: Implement logic to process user commands

## construct dictionary of possible commands
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

#get list of alpha2 codes
statement = '''SELECT Alpha2 FROM Countries'''
cur.execute(statement)
result = cur.fetchall()
alpha2 = []
for item in result:
    alpha2.append(item[0].lower())

#get list of regions
statement = '''SELECT Region FROM Countries'''
cur.execute(statement)
result = cur.fetchall()
regions = []
for item in result:
    regions.append(item[0].lower())

conn.close()


COMMAND_DICT = {
    "bars" : {
        "sellcountry" : alpha2,
        "sourcecountry" : alpha2,
        "sellregion" : regions,
        "sourceregion" : regions,
        "ratings" : "",
        "cocoa" : "",
        "top" : "",
        "bottom" : ""
    },
    "companies" : {
        "country" : alpha2,
        "region" : regions,
        "ratings" : "",
        "cocoa" : "",
        "bars_sold" : "",
        "top" : "",
        "bottom" : ""
    },
    "countries" : {
        "region" : regions,
        "sellers" : "",
        "sources" : "",
        "ratings" : "",
        "cocoa" : "",
        "bars_sold" : "",
        "top" : "",
        "bottom" : ""
    },
    "regions" : {
        "sellers" : "",
        "sources" : "",
        "ratings" : "",
        "cocoa" : "",
        "bars_sold" : "",
        "top" : "",
        "bottom" : ""
    }
}

def check_no_duplicates(command, command_list):
    matrix = [["sellcountry", "sourcecountry", "sellregion", "sourceregion"],
        ["ratings", "cocoa", "bars_sold"],
        ["top", "bottom"],
        ["country", "region"],
        ["sellers", "sources"]
    ]
    for row in matrix:
        if command in row:
            for item in command_list:
                if item in row:
                    return False
    return True

def check_valid(command, command_key, command_value):
    if command not in COMMAND_DICT.keys():
        return False
    else:
        if command_key not in COMMAND_DICT[command].keys():
            return False
        else:
            if command_value not in COMMAND_DICT[command][command_key]:
                return False
            else:
                return True


def process_command(command):
    command_list = command.lower().split()
    primary_command = command_list[0]
    params = {}
    for item in command_list[1:]:
        item_split = item.split("=")
        if len(item_split) > 1:
            params[item_split[0]] = item_split[1]
        else:
            params[item_split[0]] = ""
    #print(params)

    #check whether command parameters are valid with no duplicate parameters
    for item in params.keys():
        if check_valid(primary_command, item, params[item]) == False:
            return False
        else:
            for i in range(len(params.keys()) - 1):
                if check_no_duplicates(list(params.keys())[i], list(params.keys())[i+1:]) == False:
                    return False

    #open database connection
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = ''
    #construct and execute SQL query for bars commands
    if primary_command == "bars":
        statement = '''
            SELECT b.SpecificBeanBarName, b.Company, company.EnglishName, b.Rating, b.CocoaPercent, beans.EnglishName
            FROM Bars AS b JOIN Countries AS company ON b.CompanyLocationId = company.Id
                JOIN Countries AS beans ON b.BroadBeanOriginId = beans.Id '''
        where = ''
        order = 'ORDER BY b.Rating '
        direction = 'DESC '
        limit = 'LIMIT 10 '
        for item in params.keys():
            if item == "sellcountry":
                where = 'WHERE company.Alpha2="{}" '.format(params[item].upper())
            if item =="sourcecountry":
                where = 'WHERE beans.Alpha2="{}" '.format(params[item].upper())
            if item == "sellregion":
                where = 'WHERE company.Region="{}" '.format(params[item].capitalize())
            if item == "sourceregion":
                where = 'WHERE beans.Region="{}" '.format(params[item].capitalize())
            if item == "ratings":
                order = 'ORDER BY b.Rating '
            if item == "cocoa":
                order = 'ORDER BY b.CocoaPercent '
            if item == "top":
                direction = 'DESC '
                limit = 'LIMIT {}'.format(params[item])
            if item == "bottom":
                direction = 'ASC '
                limit = 'LIMIT {}'.format(params[item])

        statement = statement + where + order + direction + limit
        cur.execute(statement)
        results = cur.fetchall()


    #construct and execute SQL query for companies commands
    elif primary_command == "companies":
        statement1 = 'SELECT b.Company, c.EnglishName, '
        agg = '(SELECT AVG(Rating) FROM Bars AS b2 WHERE b2.Company=b.Company) AS AvgRating '
        statement2 = 'FROM Bars AS b JOIN Countries AS c ON b.CompanyLocationId = c.Id '
        where = ''
        group = 'GROUP BY b.Company '
        having = 'HAVING COUNT(b.Id) > 4 '
        order = 'ORDER BY AvgRating '
        direction = 'DESC '
        limit = 'LIMIT 10 '

        for item in params.keys():
            if item == "country":
                where = 'WHERE c.Alpha2="{}" '.format(params[item].upper())
            if item == "region":
                where = 'WHERE c.Region="{}" '.format(params[item].capitalize())
            if item == "ratings":
                agg = '(SELECT AVG(b2.Rating) FROM Bars AS b2 WHERE b2.Company=b.Company) AS AvgRating '
                order = 'ORDER BY AvgRating '
            if item == "cocoa":
                agg = '(SELECT AVG(b2.CocoaPercent) FROM Bars AS b2 WHERE b2.Company=b.Company) AS AvgCocoaPercent '
                order = 'ORDER BY AvgCocoaPercent '
            if item == "bars_sold":
                agg = '(SELECT COUNT(b2.Id) FROM Bars AS b2 WHERE b2.Company=b.Company) AS BarsSold '
                order = 'ORDER BY BarsSold '
            if item == "top":
                direction = 'DESC '
                limit = 'LIMIT {}'.format(params[item])
            if item == "bottom":
                direction = 'ASC '
                limit = 'LIMIT {}'.format(params[item])

        statement = statement1 + agg + statement2 + where + group + having + order + direction + limit
        cur.execute(statement)
        results = cur.fetchall()

    #construct and execute SQL query for countries commands
    elif primary_command == "countries":
        pass

    #construct and execute SQL query for regions commands
    elif primary_command == "regions":
        pass

    else:
        pass

    #close database connection and return results list
    conn.close()
    return results



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

commandstring = "companies region=europe"
results = process_command(commandstring)
for line in results:
    print(line)

# Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()
