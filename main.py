# MD ROKIBUL HASAN
# Purpose: Read in 3 csv files with sample data, clean the data, push cleaned data to sql database table

# import pandas
import pandas as pd
# import from other python files
from files import *
from functions import *
# import mysql info
import pymysql.cursors
from cred import *
import re

# import files
print("Beginning import")
# use pandas to read files
file1 = pd.read_csv(housingFile)
file2 = pd.read_csv(incomeFile)
file3 = pd.read_csv(zipFile)

# Remove 'guid' column from all datasets
file1 = file1.drop(columns=['guid'], errors='ignore')
file2 = file2.drop(columns=['guid'], errors='ignore')
file3 = file3.drop(columns=['guid'], errors='ignore')

# FILE 1 - HOUSING FILE DATA
print("Cleaning Housing File data")

# remaining columns - clean by replacing corrupt data with random numbers in specified ranges
cleanRandom(file1, 'housing_median_age', 10, 51)
cleanRandom(file1, 'total_rooms', 1000, 2001)
cleanRandom(file1, 'total_bedrooms', 1000, 2001)
cleanRandom(file1, 'population', 5000, 10001)
cleanRandom(file1, 'households', 500, 2501)
cleanRandom(file1, 'median_house_value', 100000, 250001)

print(f"{len(file1)} records imported into the database")

#################################################################
# FILE 2 - INCOME FILE DATA
print("Cleaning Income File data")

# median_income column - clean by replacing corrupt data with random numbers in specified range
cleanRandom(file2, 'median_income', 100000, 750001)

print(f"{len(file2)} records imported into the database")

###################################################################
# FILE 3 - ZIP FILE DATA
print("Cleaning ZIP File data")

# zip_code column - clean by replacing with 1st number of state zip + 0000
corruptData = re.compile("^[A-Z]{4}$")

# Create empty list for bad zips and a dictionary for good zips
badZips = []
goodZips = {}

# Identify bad ZIP codes and collect valid ZIP codes by city and state
for index, row in file3.iterrows():
    if corruptData.match(row['zip_code']):
        badZips.append(index)
    else:
        cityStateKey = f"{row['city']}{row['state']}"
        goodZips[cityStateKey] = row['zip_code']

# Fix the bad ZIP codes
for index in badZips:
    city = file3.iloc[index]['city']
    state = file3.iloc[index]['state']
    county = file3.iloc[index]['county']
    countyStateKey = f"{county}{state}"

    # If a valid ZIP code exists for the same county-state, use its first two digits followed by '000'
    if countyStateKey in goodZips:
        newZipCode = f"{goodZips[countyStateKey][:3]}00"
    else:
        # Otherwise, generate a new ZIP code using the state's first two digits
        stateZipSample = next((zipCode for key, zipCode in goodZips.items() if key.endswith(state)), None)
        if stateZipSample:
            newZipCode = f"{stateZipSample[:3]}00"


    # Update the bad ZIP code in the dataset
    file3.loc[index, 'zip_code'] = newZipCode

# Debug print for ZIP codes after cleaning
print("ZIP codes after cleaning:")
print(file3[['city', 'state', 'zip_code']].head(10))

# Ensure consistency across all datasets
file1['zip_code'] = file3['zip_code']
file2['zip_code'] = file3['zip_code']

# Debug print for ZIP code uniqueness
print("Unique ZIP codes:")
print(f"File1: {file1['zip_code'].nunique()}, File2: {file2['zip_code'].nunique()}, File3: {file3['zip_code'].nunique()}")


# Print updated ZIP codes
print("Updated ZIP codes:")
print(file3.loc[badZips, ['city', 'state', 'zip_code']])

# Now ensure consistency across all datasets
file1['zip_code'] = file3['zip_code'].values
file2['zip_code'] = file3['zip_code'].values

print(f"{len(file3)} records imported into the database")

# Check the length of each dataset before merging
print(f"File1 (housing) rows: {len(file1)}")
print(f"File2 (income) rows: {len(file2)}")
print(f"File3 (zip) rows: {len(file3)}")

# Ensure consistency of ZIP codes across datasets
print("File1 unique ZIP codes:", file1['zip_code'].nunique())
print("File2 unique ZIP codes:", file2['zip_code'].nunique())
print("File3 unique ZIP codes:", file3['zip_code'].nunique())

# Merge 3 files so we can import one database into SQL
mergeFile12 = pd.merge(file1, file2, how='inner', on='zip_code')
mergedAll = pd.merge(mergeFile12, file3, how='inner', on='zip_code')

######################################################################
# IMPORT FILE RECORDS INTO SQL DATABASE
try:
    myConnection = pymysql.connect(host=host,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
    exit()

try:
    with myConnection.cursor() as cursor:
        # our sql statement using placeholders for each column of data
        sqlInsert = """
                insert
                into
                housing(zip_code, city, state, county, median_age, total_rooms,
                        total_bedrooms, population, households, median_income, median_house_value)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        # insert values for each row into each column
        for index, row in mergedAll.iterrows():
            cursor.execute(sqlInsert, (f"{row.zip_code}",
                                       f"{row.city}", f"{row.state}", f"{row.county}",
                                       f"{row.housing_median_age}", f"{row.total_rooms}",
                                       f"{row.total_bedrooms}", f"{row.population}",
                                       f"{row.households}", f"{row.median_income}",
                                       f"{row.median_house_value}"))

        # commit the file to the database
        myConnection.commit()

# if there is an exception, show what that is
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()

print(f"Import Completed")
print(f"\nBeginning validation\n")

# Begin Validation - Allow user input for custom values
try:
    with myConnection.cursor() as cursor:
        # Ask for user input for totalRooms and zipMedianIncome
        totalRooms = int(input("Enter the minimum number of total rooms to filter by: "))
        zipMedianIncome = int(input("Enter the ZIP code for which to calculate the median income: "))

        # Validation part
        # our roomSql summation statement
        roomSql = """select
                    sum(total_bedrooms) as bedrooms 
                    from 
                    housing
                    where
                    total_rooms > %s 
                    """
        # user input is value for roomSql
        cursor.execute(roomSql, (totalRooms,))
        # get the resulting sum from sql
        sumResult = cursor.fetchall()
        print(f"For locations with more than {totalRooms} rooms, "
              f"there are a total of {sumResult[0]['bedrooms']} bedrooms.")

        # our incomeSql averaging statement
        incomeSql = """select
                    format(round(avg(median_income)),0) as zipCode
                    from 
                    housing
                    where
                    zip_code = %s 
                    """

        # user input is value for incomeSql
        cursor.execute(incomeSql, (zipMedianIncome,))
        # get the resulting avg of median income from sql
        incomeResult = cursor.fetchall()
        print(f"The median household income for ZIP code {zipMedianIncome} is {incomeResult[0]['zipCode']}.")

# if there is an exception, show what that is
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
finally:
    myConnection.close()

print(f"\nProgram exiting.")
