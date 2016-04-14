"""Package of methods for merging marathon running data from different
marathons (time, place).  Using a technique known as Matching Estimators.
"""

import pandas as pd
import numpy as np


def count_estimators(df):
    """Count number of records in each estimator category.  Tallies number
    runners by age and gender.

    Parameters
    ----------
    df : DataFrame
        cleaned DataFrame.

    Output
    ------
    list
        GENDERS x AGES list of record counts
    """
    count_df = pd.DataFrame(columns=AGES)

    count_lst = []
    for gender in GENDERS:
        gender_df = df[df['gender'] == gender]
        lst = [sum(gender_df['age'] == age) for age in AGES]
        count_lst.append(lst)
    return count_lst


def get_weather_array(wunderground_array, unit):
    """Converts a list of strings from scraped wunderground data to a list of
    float.  Also checks to make sure data is what is expected.

    Parameters
    ----------
    wunderground_array : list of string / panda Series
    unit : string

    Returns
    -------
    list of float

    Examples
    --------
    >>> get_weather_array(['92.1F','99.9F'], 'F')
    [92.1, 99.9]
    >>> get_weather_array(['98%', '95%'], '%')
    [98.0, 95.0]
    """
    weather_array = []
    error = False
    unit_length = len(unit)
    for s in wunderground_array:
        if s[-unit_length:] == unit:
            weather_array.append(float(s[0:-unit_length]))
        elif s == '-':
            weather_array.append(0)
        else:
            error = True
            weather_array.append(0)
    if error:
        print 'Error in get_weather_float(), units do not match specification'
        print wunderground_array['date'], unit
    return weather_array


def get_wind_vector(windspeeds, winddirections):
    """Calculates the average wind vector given a list of wind speeds and
    directions.

    Parameters
    ----------
    windspeeds : array-like
        array of speeds in format '18.1mph'
    winddirections : array-like
        array of directions in 16-point compass format (wunderground)
    Returns
    -------
    (avg_east_wind, avg_north_wind) : (float, float)

    Examples
    --------
    >>> get_wind_vector(['10.0mph', '20.0mph'], ['North', 'North'])
    (0.0, 15.0)
    >>> get_wind_vector(['10.0mph', '20.0mph'], ['NW', 'NW'])
    (-10.606601717798217, 10.606601717798211)
    """
    # direction given in pi units
    # radians = compass['South'] * np.pi
    compass = {'North': 0, 'South': 1, 'East': 0.5, 'West': 1.5,
               'NE': 0.25, 'SE': 0.75, 'SW': 1.25, 'NW': 1.75,
               'NWE': 0.125, 'ENE': 0.375, 'ESE': 0.625, 'SSE': 0.875,
               'SSW': 1.125, 'WSW': 1.375, 'WNW': 1.625, 'NNW': 1.875,
               'Variable': None, 'Calm': None}

    east_winds = []
    north_winds = []
    windspeeds = get_weather_array(windspeeds, 'mph')
    for speed, direction in zip(windspeeds, winddirections):
        if compass[direction] != None:
            east_winds.append(np.sin(compass[direction]*np.pi) * speed)
            north_winds.append(np.cos(compass[direction]*np.pi) * speed)
        else:
            east_winds.append(0)
            north_winds.append(0)
    return np.mean(east_winds), np.mean(north_winds)


def fetch_weather_features(marathon_name, year):
    """Returns weather features for a given marathon and date.
    Information is derived from weather_df.

    Parameters
    ----------
    marathon_name : string
        name of the marathon event
    year : integer
        year of the event

    Returns
    -------
    avgtemp  : float
        average temperature across time and across cities
    avghumid : float
        average humidity across time and across cities
    avgwindE : float
        average wind speed, easterly component (west is negative)
    avgwindN : float
        average wind speed, northerly component (south is negative)
    isgusty  : boolean
        True if more than half of relevant weather observations were gusty
    raining  : float
        [0..1], Proportion of relevant weather observations where rain was
        observed.

    Example
    -------
    >>> "{0:.2f}, {1:.2f}".format(*fetch_weather_features('boston', 2014))
    '59.41, 25.29'
    >>> "{2:.2f}, {3:.2f}".format(*fetch_weather_features('boston', 2014))
    '-1.81, -6.61'
    """
    subset_mask = ((weather_df['marathon'] == marathon_name) &
                   (weather_df['year'] == year))
    subset_df = weather_df[subset_mask]
    n = len(subset_df)
    # No weather data found
    if n == 0:
        return 0, 0, 0, 0, False, 0
    avgtemp = np.mean(get_weather_array(subset_df['Temp.'], 'F'))
    avghumid = np.mean(get_weather_array(subset_df['Humidity'], '%'))
    avgwindE, avgwindN = get_wind_vector(subset_df['Wind Speed'],
                                         subset_df['Wind Dir'])
    isgusty = sum(subset_df['Gust Speed'] != '-') > (n / 2)
    rainhours = sum(subset_df['Events'] == 'Rain') / n

    return avgtemp, avghumid, avgwindE, avgwindN, isgusty, rainhours


def sample_estimator(df, gender, age):
    """Randomly sample rows from a specific estimator.  Add in weather
    data.

    Parameters
    ----------
    df : DataFrame

    Output
    ------
    sample_df : DataFrame
    """
    estimator_df = df[(df['gender'] == gender) & (df['age'] == age)]
    sample_df = estimator_df.sample(n=SAMPLE_SIZE, replace=True,
                                    random_state=42)

    # Features to include from each marathon
    sample_df = sample_df[['marathon', 'year', 'firstname', 'bib', 'age',
                           'gender', 'state', 'country', 'timehalf',
                           'offltime']]
    # Add weather columns
    marathon_name = df[0, 'marathon']
    year = df[0, 'year']
    avgtemp, avghumid, avgwindE, avgwindN, isgusty, rainhours \
        = fetch_weather_features(marathon_name, year)
    df['avgtemp'] = avgtemp
    df['avghumid'] = avghumid
    df['avgwindE'] = avgwindE
    df['avgwindN'] = avgwindN
    df['isgusty'] = isgusty
    df['rainhours'] = rainhours

    return sample_df


def sample_all(df):
    """Fetches the correct number of matching estimators from DataFrame

    Parameters
    ----------
    df : DataFrame

    Output
    ------
    sample_df : DataFrame
    """
    sample_df = pd.DataFrame()
    for gender in GENDERS:
        for age in AGES:
            sample_df = sample_df.append(sample_estimator(df, gender, age),
                                         ignore_index=True)
    return sample_df


# Estimator Definition, results in 40 x 2 estimators
AGE_MIN = 21
AGE_MAX = 60
GENDERS = (True, False)
AGES = range(AGE_MIN, AGE_MAX+1, 1)
SAMPLE_SIZE = 50

weather_file = 'data/marathon_weather.csv'
weather_df = pd.read_csv(weather_file)

if __name__ == '__main__':
    folder = 'data/'
    marathon_files = ['boston2015_clean.csv', 'boston2014_clean.csv',
                      'boston2013_clean.csv', 'boston2012_clean.csv',
                      'boston2011_clean.csv', 'boston2010_clean.csv',
                      'boston2009_clean.csv', 'boston2008_clean.csv',
                      'boston2007_clean.csv', 'boston2006_clean.csv',
                      'boston2005_clean.csv', 'boston2004_clean.csv',
                      'boston2003_clean.csv', 'boston2002_clean.csv',
                      'boston2001_clean.csv']

    matching_estimators = pd.DataFrame()
    for filename in marathon_files:
        df = pd.read_csv(folder+filename)
        matching_estimators = matching_estimators.append(sample_all(df))
    save_file = folder+'boston_estimators.csv'
    print 'Saving', save_file
    matching_estimators.to_csv(folder+'boston_estimators.csv', index=False)
