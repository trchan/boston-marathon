# CLEAN RAW .CSV FILES from marathonguide.com scraping
#
# Script takes a csv file consisting of raw data extracted from html tables
# and converts all the different formats to ONE FORMAT.  This includes
# cleaning the data, but not overprocessing it.
#
# Format matches that created by cleanboston.py
#
# |  fieldname    | type     | description |
# |---------------|----------|-------------|
# | marathon      | string   | code
# | year          | integer  |
# | bib           | string   |
# | url           | string   | link to biography or photos
# | name          | unicode  | full name
# | firstname     | string   | cleaned firstname for matching
# | lastname      | string   | cleaned lastname for matching
# | age           | integer  |
# | gender        | Boolean  | True if male, False if female
# | city          | string   |
# | state         | string   | 2 letter string
# | country       | string   |
# | citizenship   | string   |
# | subgroup      | string   |
# | gunstart      | float    | time of day 'HH:MM:SS'
# | starttime     | float    | time of day 'HH:MM:SS'
# | time5k        | float    | split time 'HH:MM:SS'
# | time10k       | float    | split time
# | time15k       | float    | split time
# | time20k       | float    | split time
# | timehalf      | float    | split time
# | time25k       | float    | split time
# | time30k       | float    | split time
# | time35k       | float    | split time
# | time40k       | float    | split time
# | pace          | float    | time/mile in minutes
# | projtime      | float    | ??
# | offltime      | float    | official finish time relative to gun
# | nettime       | float    | finish time relative to start
# | overall_rank  | integer  |
# | gender_rank   | integer  |
# | division_rank | integer  |
# | minage        | string   | minimum age (from runner category)
# | maxage        | string   | maximum age (from runner category)
# | other3        | string   | Field for future assignment
# | other4        | string   | Field for future assignment
#
# 36 columns

import pandas as pd
from string import punctuation, digits
from marathonlib import time_to_minutes
import os


def get_fullname(names):
    '''
    Example
    -------
    >>> names = pd.Series(['Jean-Marc Th (M)', 'Miguel Angel Cifuentes (M)'])
    >>> get_fullname(names)
    ['Th, Jean-Marc', 'Cifuentes, Miguel Angel']
    '''
    fullnames = []
    for name in names:
        fullname = name.split(' ')
        n = len(fullname)
        try:
            fullname = fullname[-2] + ', ' + " ".join(fullname[0:-2])
        except IndexError:
            fullname = ""
        fullnames.append(fullname)
    return fullnames


def clean_name(name):
    '''Takes a full name in the general format of "Lastname, Firstname I" and
    converts it to a format that increases the chance of matching names from a
    variety of sources.
        - Uppercase
        - Punctuation is removed
        - middle names removed
        - Suffixes removed
    Parameters
    ----------
    name : string
    Returns
    -------
    (firstname, lastname) : string, string
    Examples
    --------
    >>> clean_name('Jose F Gonzalez (M)')
    ('JOSE', 'GONZALEZ', True, -1)
    >>> clean_name('Ignacio Lopez-Mancisidor (M)')
    ('IGNACIO', 'LOPEZMANCISIDOR', True, -1)
    >>> clean_name('Karina Lizette Garcia Barrios (F28)')
    ('KARINA', 'BARRIOS', False, 28)
    '''
    names = name.split(' ')
    if len(names) > 2:
        lastname = names[-2]
        lastname = [c.upper() for c in lastname if c not in punctuation+' ']
        lastname = "".join(lastname)
        firstname = names[0]
        firstname = [c.upper() for c in firstname if c not in punctuation+' ']
        firstname = "".join(firstname)
    else:
        firstname = ""
        lastname = names[0]
    genderage = names[-1]
    if genderage[1] == 'M':
        gender = True
    else:
        gender = False
    try:
        age = int("".join([c for c in genderage if c in digits]))
    except ValueError:
        age = -1
    return firstname, lastname, gender, age


def get_age_range(div):
    '''Estimates age based on a giving division name
    Examples
    --------
    >>> get_age_range('M35-39')
    (35, 39)
    >>> get_age_range('Mopen')
    (18, 99)
    '''
    if div.find('-') > 0:
        ages = div.split('-')
        min_age = ages[0]
        max_age = ages[1]
        try:
            min_age = int(min_age[-2:])
        except ValueError:
            min_age = 0
        try:
            max_age = int(max_age[0:2])
        except ValueError:
            max_age = 99
    else:
        min_age = 18
        max_age = 99
    return min_age, max_age


def getcity_state_country(series):
    '''Takes a list of 'City State Country' in text form and returns a list of
    states and a list of countries.

    Example
    -------
    >>> series = pd.Series(['Dublin, Ireland', 'Miami, FL, USA', ''])
    >>> getcity_state_country(series)
    (['Dublin', 'Miami', ''], ['', 'FL', ''], ['Ireland', 'USA', ''])
    '''
    error = False
    cities = []
    states = []
    countries = []
    for item in series:
        if len(item) > 0:
            split_items = item.split(',')
        else:
            split_items = ['']
        if len(split_items) == 1:
            country = split_items[0].strip()
            state = ''
            city = ''
        elif len(split_items) == 2:
            country = split_items[1].strip()
            state = ''
            city = split_items[0].strip()
        elif len(split_items) >= 3:
            country = split_items[-1].strip()
            state = split_items[-2].strip()
            city = split_items[0].strip()
        else:
            print 'error in getcity_state_country'
            print item
            country = ''
            state = ''
            cty = ''
        cities.append(city)
        states.append(state)
        countries.append(country)
    return cities, states, countries


def getstate_country(series):
    '''Takes a list of 'State, Country' in text form and returns a list of
    states and a list of countries.

    Example
    -------
    >>> series = pd.Series(['TX, USA', 'Mexico', 'FL, USA'])
    >>> getstate_country(series)
    (['TX', '', 'FL'], ['USA', 'Mexico', 'USA'])
    '''
    error = False
    countries = []
    states = []
    for item in series:
        split_items = item.split(',')
        if len(split_items) == 1:
            country = split_items[0].strip()
            state = ''
        elif len(split_items) == 2:
            country = split_items[1].strip()
            state = split_items[0].strip()
        else:
            state = ''
            country = ''
            error = True
        countries.append(country)
        states.append(state)
    if error:
        print 'Too many values found in getstate_country'
    return states, countries


def clean_raw_marathon(raw_df, marathon_id, year):
    '''Cleans data from a DataFrame containing a raw extract from the HTML.
    Clean DataFrame is standardized across marathons.  This should handle
    raw scraped data from marathonguide.com.
    INPUT
        DataFrame
    OUTPUT
        DataFrame

    RAW CSV HEADER - can vary
    Sample 1:
    "Last Name, First Name(Sex/Age)",Time,OverAllPlace,Sex Place/Div
    Place,DIV,Net Time,"City, State, Country",AG Time*,BQ*,midd
    Sample 2:
    "Last Name, First Name(Sex/Age)",Time,OverAllPlace,Sex Place/Div
    Place,DIV,Net Time,"State, Country",AG Time*,BQ*,midd
    '''
    blank_str = '-'
    blank_val = 0
    clean_columns = [u'marathon', u'year', u'bib', u'url', u'name',
                     u'firstname', u'lastname', u'age', u'gender', u'city',
                     u'state', u'country', u'citizenship', u'subgroup',
                     u'gunstart', u'starttime', u'time5k', u'time10k',
                     u'time15k', u'time20k', u'timehalf', u'time25k',
                     u'time30k', u'time35k', u'time40k', u'pace', u'projtime',
                     u'offltime', u'nettime', u'overall_rank', u'gender_rank',
                     u'division_rank', u'minage', u'maxage', u'other3',
                     u'other4']
    clean_df = pd.DataFrame(columns=clean_columns)
    clean_df['name'] = get_fullname(raw_df['Last Name, First Name(Sex/Age)'])
    clean_df['bib'] = blank_val
    clean_df['marathon'] = marathon_id
    clean_df['year'] = year
    clean_df['url'] = blank_str
    firstnames, lastnames, genders, ages = [], [], [], []
    # Extract 'Last Name, Firstname(Sex/Age)' field
    for name in raw_df['Last Name, First Name(Sex/Age)']:
        firstname, lastname, gender, age = clean_name(name)
        firstnames.append(firstname)
        lastnames.append(lastname)
        genders.append(gender)
        ages.append(age)
    clean_df['firstname'] = firstnames
    clean_df['lastname'] = lastnames
    clean_df['gender'] = genders
    clean_df['age'] = ages
    # Find age category
    if 'DIV' in raw_df.columns:
        raw_df.loc[raw_df['DIV'].isnull(), 'DIV'] = ''
        minages, maxages = [], []
        for div in raw_df['DIV']:
            min_age, max_age = get_age_range(div)
            minages.append(min_age)
            maxages.append(max_age)
        clean_df['minage'] = minages
        clean_df['maxage'] = maxages
    if 'State, Country' in raw_df.columns:
        raw_df[raw_df['State, Country'].isnull()] = ''
        state, country = getstate_country(raw_df['State, Country'])
        city = blank_str
    elif 'City, State, Country' in raw_df.columns:
        raw_df[raw_df['City, State, Country'].isnull()] = ''
        city, state, country = getcity_state_country(
                raw_df['City, State, Country'])
    else:
        city = blank_str
        state = blank_str
        country = blank_str
    clean_df['city'] = city
    clean_df['state'] = state
    clean_df['country'] = country
    clean_df['citizenship'] = blank_str
    clean_df['subgroup'] = blank_str
    clean_df['gunstart'] = blank_val
    clean_df['starttime'] = blank_val
    clean_df['time5k'] = blank_val
    clean_df['time10k'] = blank_val
    clean_df['time15k'] = blank_val
    clean_df['time20k'] = blank_val
    clean_df['timehalf'] = blank_val
    clean_df['time25k'] = blank_val
    clean_df['time30k'] = blank_val
    clean_df['time35k'] = blank_val
    clean_df['time40k'] = blank_val
    clean_df['pace'] = blank_val
    clean_df['projtime'] = blank_val

    if 'Time' in raw_df.columns:
        clean_df['offltime'] = map(time_to_minutes, raw_df['Time'])
    else:
        clean_df['offltime'] = map(time_to_minutes, raw_df['Net Time'])
    if 'Net Time' in raw_df.columns:
        clean_df['nettime'] = map(time_to_minutes, raw_df['Net Time'])
    else:
        clean_df['nettime'] = raw_df['Time']
    clean_df['overall_rank'] = blank_val
    clean_df['gender_rank'] = blank_val
    clean_df['division_rank'] = blank_val
    clean_df['other3'] = blank_str
    clean_df['other4'] = blank_str
    return clean_df


def filter_runners(df):
    """Filters out undesired records for our analysis.

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    filtered_df : DataFrame
    """
    filtered_df = df
    return filtered_df


def batch_clean_files(file_list, folder, midd_file):
    # read index file
    midd_df = pd.read_csv(folder+midd_file)
    for file in file_list:
        # Import raw file
        print 'Importing', file
        raw_df = pd.read_csv(folder+file)
        midd = raw_df.iloc[0]['midd']
        name = midd_df.loc[midd_df['midd'] == midd]['marathon'].values[0]
        year = midd_df.loc[midd_df['midd'] == midd]['year'].values[0]
        # Converts raw data into "Standardized" Clean DataFrame
        clean_df = clean_raw_marathon(raw_df, name, year)
        # Filter out records
        clean_df = filter_runners(clean_df)
        # Save clean csv file
        filename = folder+name+str(year)+'_clean.csv'
        print '    -->', filename
        clean_df.to_csv(filename, index=False)


def getallfiles():
    '''Searches folder for '*raw.csv', and returns list of files.
    '''
    output = []
    file_list = os.listdir(FOLDER)
    for file in file_list:
        if file.find('raw.csv') > 0:
            output.append(file)
    return output


if __name__ == '__main__':
    FOLDER = 'data/marathonguide/scrape/'
    MIDD_FILE = '../midd_list.csv'
    file_list = getallfiles()
    batch_clean_files(file_list, FOLDER, MIDD_FILE)
