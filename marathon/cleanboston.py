# CLEAN RAW .CSV FILES
#
# Script takes a csv file consisting of raw data extracted from html tables
# and converts all the different formats to ONE FORMAT.  This includes
# cleaning the data, but not overprocessing it.
# Homogeneous format required.
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
from string import punctuation
from marathonlib import time_to_minutes


def clean_bib(bib_string):
    """Takes a raw bib string, and converts it to an integer.

    Parameters
    ----------
    bib_string : string
        Typically a raw extract from a scrape

    Returns
    -------
    bib_number : integer

    Example
    -------
    >>> clean_bib('43')
    43
    >>> clean_bib('F12')
    12
    >>> clean_bib('W12')
    12
    """
    known_prefixes = 'FWH'  # Female, Wheelchair, and Handcycles
    if bib_string[0] in known_prefixes:
        return int(bib_string[1:])
    else:
        return int(bib_string)


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
    >>> clean_name('Abraham Peregrina, Nahim')
    ('NAHIM', 'ABRAHAMPEREGRINA')
    >>> clean_name('Aase, Geir Harald')
    ('GEIR', 'AASE')
    >>> clean_name('Abou-Zamzam, Ahmed M. Jr.')
    ('AHMED', 'ABOUZAMZAM')
    >>> clean_name('Buckley, Ed')
    ('ED', 'BUCKLEY')

    Trouble cases are handled as shown below
    >>> clean_name('Sung, Kwong Hung, Patrick')
    ('PATRICK', 'SUNG')
    >>> clean_name('Mercado, M.D., Michael G.')
    ('MICHAEL', 'MERCADO')
    >>> clean_name('Brown, E G Ned')
    ('EG', 'BROWN')
    >>> clean_name('Andres, R. Jimmy')
    ('RJ', 'ANDRES')
    '''
    names = name.split(',')
    lastname = names[0]
    lastname = [c.upper() for c in lastname if c not in punctuation+' ']
    lastname = "".join(lastname)
    firstname = names[-1]
    firstname = firstname.split()[0]
    # catch scenario where only a first initial is present.  In that case,
    # grab first 2 alphanumeric
    if len(firstname) < 3:
        firstname = names[1]
        firstname = [c.upper() for c in firstname if c not in punctuation+' ']
        firstname = "".join(firstname)
        firstname = firstname[0:2]
    else:
        firstname = [c.upper() for c in firstname if c not in punctuation+' ']
        firstname = "".join(firstname)
    return firstname, lastname


def clean_bos2010url(raw_url, year):
    '''Raw data contains url as a javascript call.
    Parameters
    ----------
    raw_url : string
        This is typically a javascript call embedded in the html page.
    year : integer
    Returns
    -------
    string
        url format
    Example
    -------
    >>> clean_bos2010url("javascript:OpenDetailsWindow('30562')", 2015)
    'http://registration.baa.org/2015/cf/public/wnd_iAthleteDetailsWindow.cfm?RaceAppID=30562'
    '''
    if len(raw_url) > 0:
        dissect = raw_url.split("'")
        if len(dissect) == 3:
            java_id = dissect[1]
            output = 'http://registration.baa.org/' + str(year) + \
                '/cf/public/wnd_iAthleteDetailsWindow.cfm?RaceAppID='+java_id
            return output
    return '-'


def clean_bos2010(raw_df, marathon_id, year):
    '''Cleans data from a DataFrame containing a raw extract from the HTML.
    Clean DataFrame is standardized across marathons.  Works when raw_df is
    from 2010-2016 Boston marathon website.
    INPUT
        DataFrame
        marathon_id: string
        year: int
    OUTPUT
        DataFrame

    RAW CSV HEADER
    Index([u'bib', u'name', u'age', u'gender', u'city', u'state', u'country',
           u'citizenship', u'subgroup', u'url', u'd5k', u'd10k', u'd15k',
           u'd20k', u'half', u'd25k', u'd30k', u'd35k', u'd40k', u'pace',
           u'projtime', u'offltime', u'overall', u'genderrank', u'division'])
    '''
    n = len(raw_df)
    blank_str = '-'
    blank_val = 0
    clean_columns = [u'marathon', u'year', u'bib', u'url', u'name',
                     u'firstname', u'lastname', u'age', u'gender', u'city',
                     u'state', u'country', u'citizenship', u'subgroup',
                     u'gunstart', u'starttime', u'time5k', u'time10k',
                     u'time15k', u'time20k', u'timehalf', u'time25k',
                     u'time30k', u'time35k', u'time40k', u'pace',
                     u'projtime', u'offltime', u'nettime', u'overall_rank',
                     u'gender_rank', u'division_rank', u'minage', u'maxage',
                     u'other3', u'other4']
    clean_df = pd.DataFrame(columns=clean_columns)
    clean_df['bib'] = map(clean_bib, raw_df['bib'])
    clean_df['marathon'] = marathon_id
    clean_df['year'] = year
    clean_df['url'] = map(lambda url: clean_bos2010url(str(url), year),
                          raw_df['url'])
    clean_df['name'] = raw_df['name']
    firstnames, lastnames = [], []
    for name in raw_df['name']:
        firstname, lastname = clean_name(name)
        firstnames.append(firstname)
        lastnames.append(lastname)
    clean_df['firstname'] = firstnames
    clean_df['lastname'] = lastnames
    clean_df['age'] = raw_df['age']
    clean_df['gender'] = raw_df['gender'] == 'M'
    clean_df['city'] = raw_df['city']
    clean_df['state'] = raw_df['state']
    clean_df['country'] = raw_df['country']
    clean_df['citizenship'] = raw_df['citizenship']
    clean_df['subgroup'] = raw_df['subgroup']
    clean_df['gunstart'] = blank_val
    clean_df['starttime'] = blank_val
    clean_df['time5k'] = map(time_to_minutes, raw_df['d5k'])
    clean_df['time10k'] = map(time_to_minutes, raw_df['d10k'])
    clean_df['time15k'] = map(time_to_minutes, raw_df['d15k'])
    clean_df['time20k'] = map(time_to_minutes, raw_df['d20k'])
    clean_df['timehalf'] = map(time_to_minutes, raw_df['half'])
    clean_df['time25k'] = map(time_to_minutes, raw_df['d25k'])
    clean_df['time30k'] = map(time_to_minutes, raw_df['d30k'])
    clean_df['time35k'] = map(time_to_minutes, raw_df['d35k'])
    clean_df['time40k'] = map(time_to_minutes, raw_df['d40k'])
    clean_df['pace'] = map(time_to_minutes, raw_df['pace'])
    clean_df['projtime'] = map(time_to_minutes, raw_df['projtime'])
    clean_df['offltime'] = map(time_to_minutes, raw_df['offltime'])
    clean_df['nettime'] = map(time_to_minutes, raw_df['offltime'])
    clean_df['overall_rank'] = raw_df['overall']
    clean_df['gender_rank'] = raw_df['genderrank']
    clean_df['division_rank'] = raw_df['division']
    clean_df['minage'] = blank_str
    clean_df['maxage'] = blank_str
    clean_df['other3'] = blank_str
    clean_df['other4'] = blank_str
    return clean_df


def clean_bos2001(raw_df, marathon_id, year):
    '''Cleans data from a DataFrame containing a raw extract from the HTML.
    Clean DataFrame is standardized across marathons.  Works when raw_df is
    from 2001-2009 Boston marathon website.
    INPUT
        DataFrame
    OUTPUT
        DataFrame

    RAW CSV HEADER
    Index([u'year', u'bib', u'name', u'age', u'gender', u'city', u'state',
       u'country', u'subgroup', u'overallrank', u'genderrank', u'divisionrank',
       u'Officialtime', u'nettime'], dtype='object')
    '''
    n = len(raw_df)
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
    clean_df['bib'] = map(clean_bib, raw_df['bib'])
    clean_df['marathon'] = marathon_id
    clean_df['year'] = year
    clean_df['url'] = blank_str
    clean_df['name'] = raw_df['name']
    firstnames, lastnames = [], []
    for name in raw_df['name']:
        firstname, lastname = clean_name(name)
        firstnames.append(firstname)
        lastnames.append(lastname)
    clean_df['firstname'] = firstnames
    clean_df['lastname'] = lastnames
    clean_df['age'] = raw_df['age']
    clean_df['gender'] = raw_df['gender'] == 'M'
    clean_df['city'] = raw_df['city']
    clean_df['state'] = raw_df['state']
    clean_df['country'] = raw_df['country']
    clean_df['citizenship'] = blank_str
    clean_df['subgroup'] = raw_df['subgroup']
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
    clean_df['offltime'] = map(time_to_minutes, raw_df['Officialtime'])
    clean_df['nettime'] = map(time_to_minutes, raw_df['nettime'])
    clean_df['overall_rank'] = map(lambda s: int(s.split('/')[0]),
                                   raw_df['overallrank'])
    clean_df['gender_rank'] = map(lambda s: int(s.split('/')[0]),
                                  raw_df['genderrank'])
    clean_df['division_rank'] = map(lambda s: int(s.split('/')[0]),
                                    raw_df['divisionrank'])
    clean_df['minage'] = blank_str
    clean_df['maxage'] = blank_str
    clean_df['other3'] = blank_str
    clean_df['other4'] = blank_str
    return clean_df


def filter_runners(df):
    """Filters out undesired records for our analysis.

    Criteria : ['subgroup'] not in {'WHEELCHAIR', 'HANDCYCLE}

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    filtered_df : DataFrame
    """
    filtered_df = df[~df['subgroup'].isin(['WHEELCHAIR', 'HANDCYCLE'])]
    return filtered_df


def batch_clean_2010(file_list, years, folder='data', name='boston'):
    for file, year in zip(file_list, years):
        # Import raw file
        raw_df = pd.read_csv(folder+'/'+file)
        # Converts raw data into "Standardized" Clean DataFrame
        clean_df = clean_bos2010(raw_df, name, year)
        # Filter out records
        clean_df = filter_runners(clean_df)
        # Save clean csv file
        filename = folder+'/'+name+str(year)+'_clean.csv'
        print year, 'saved as', filename
        clean_df.to_csv(filename, index=False)


def batch_clean_2001(file_list, years, folder='data', name='boston'):
    for file, year in zip(file_list, years):
        # Import raw file
        raw_df = pd.read_csv(folder+'/'+file)
        # Converts raw data into "Standardized" Clean DataFrame
        clean_df = clean_bos2001(raw_df, name, year)
        # Filter out records
        clean_df = filter_runners(clean_df)
        # Save clean csv file
        filename = folder+'/'+name+str(year)+'_clean.csv'
        print year, 'saved as', filename
        clean_df.to_csv(filename, index=False)


if __name__ == '__main__':
    file_list = ['bos10_marathon.csv', 'bos11_marathon.csv',
                 'bos12_marathon.csv', 'bos13_marathon.csv',
                 'bos14_marathon.csv', 'bos15_marathon.csv',
                 'bos16_marathon.csv']
    years = [2010, 2011, 2012, 2013, 2014, 2015, 2016]
    batch_clean_2010(file_list, years, folder='data', name='boston')

    file_list = ['bos01_marathon.csv', 'bos02_marathon.csv',
                 'bos03_marathon.csv', 'bos04_marathon.csv',
                 'bos05_marathon.csv', 'bos06_marathon.csv',
                 'bos07_marathon.csv', 'bos08_marathon.csv',
                 'bos09_marathon.csv']
    years = [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009]
    batch_clean_2001(file_list, years, folder='data', name='boston')
