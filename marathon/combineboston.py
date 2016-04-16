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
        elif s == 'Calm':
            weather_array.append(0)
        else:
            error = True
            weather_array.append(0)
    if error:
        print 'Error in get_weather_float(), units do not match specification'
        # print wunderground_array, unit
    return weather_array


def get_avg_windspeed(windspeeds):
    """Calculates the average wind speed, irregardless of direction

    Parameters
    ----------
    windspeeds : array-like
        array of speeds in format '18.1mph'

    Returns
    -------
    avgwindspeed : float

    Example
    -------
    >>> get_avg_windspeed(['15.1mph', '18.6mph', '12.9mph'])
    15.533333333333333
    """
    windspeeds = get_weather_array(windspeeds, 'mph')
    return np.mean(windspeeds)


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
               'NNE': 0.125, 'ENE': 0.375, 'ESE': 0.625, 'SSE': 0.875,
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
    avgwind  : float
        average net wind speed
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
    >>> "{3:.2f}, {4:.2f}".format(*fetch_weather_features('boston', 2014))
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
    avgwind = get_avg_windspeed(subset_df['Wind Speed'])
    avgwindE, avgwindN = get_wind_vector(subset_df['Wind Speed'],
                                         subset_df['Wind Dir'])
    isgusty = sum(subset_df['Gust Speed'] != '-') > (n / 2)
    rainhours = sum(subset_df['Events'] == 'Rain') / float(n)

    return avgtemp, avghumid, avgwind, avgwindE, avgwindN, isgusty, rainhours


def find_nonqualifier_start(df):
    """Finds the bib number of the first non-qualifier in a seeded marathon.

    This works when qualifying and non-qualifying runners are grouped into two
    categories, split by bib numbers.  This algorithm finds the dividing line
    by recursively by looking for a big change in variance of the finish times.

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    bib : integer
        bib number of the first non-qualifier (approximately)
    """
    def get_variance_differences(df, bibs, interval):
        """Calculates variances differences
        Parameters
        ----------
        df : DataFrame
        bibs : list of integers
            ordered bib numbers
        interval : integer
            number of runners to calculate variances on

        Returns
        -------
        variance_differences : list of float
        """
        variance_differences = []
        for bib in bibs:
            bibs_before = df['bib'].isin(range(bib-interval, bib))
            bibs_after = df['bib'].isin(range(bib, bib+interval))
            difference = np.var(df.loc[bibs_after, 'offltime']) - np.var(df.loc[bibs_before, 'offltime'])
            variance_differences.append(difference)
        return variance_differences

    # First iteration, do a broad search every 500 bibs, measuring the variance
    # difference of the 1000 bibs preceding a big number ,compared to 1000 bibs
    # after (window = 1000).  Find the point of greatest delta variance.
    bibs = range(df['bib'].min()+500, df['bib'].max()-500, 500)
    differences = get_variance_differences(df, bibs, 1000)
    max_bib = bibs[differences.index(max(differences))]

    # Second iteration, use the point found above to zoom in.  Search every 50
    # bibs, with a 100 bib variance window.
    bibs = range(max_bib-1000, max_bib+1000, 50)
    differences = get_variance_differences(df, bibs, 100)
    max_bib = bibs[differences.index(max(differences))]

    # Final search, every 5 bibs, with a variance window = 10.  I do not thing
    # we can get any smaller than this.
    bibs = range(max_bib-100, max_bib+100, 5)
    differences = get_variance_differences(df, bibs, 10)
    max_bib = bibs[differences.index(max(differences))]
    return max_bib


def add_features(df):
    """Add features to existing dataframe

    Parameters
    df : DataFrame

    Returns
    augmented_df : Data Frame
    """
    # Features to include from each marathon
    augmented_df = df[['marathon', 'year', 'firstname', 'bib', 'age',
                       'gender', 'state', 'country', 'timehalf',
                       'offltime']].copy()
    # Add runner categories
    augmented_df['elite'] = augmented_df['bib'] <= 100
    # Add weather columns
    marathon_name = augmented_df['marathon'].iloc[0]
    year = augmented_df['year'].iloc[0]
    avgtemp, avghumid, avgwind, avgwindE, avgwindN, isgusty, rainhours \
        = fetch_weather_features(marathon_name, year)
    augmented_df['avgtemp'] = avgtemp
    augmented_df['avghumid'] = avghumid
    augmented_df['avgwind'] = avgwind
    augmented_df['avgwindE'] = avgwindE
    augmented_df['avgwindN'] = avgwindN
    augmented_df['isgusty'] = isgusty
    augmented_df['rainhours'] = rainhours
    return augmented_df


def sample_estimator(df, gender, age):
    """Randomly sample rows from a specific estimator.  Add in more features.

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
    sample_df = add_features(sample_df)
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
SAVE_FILENAME = 'boston_combined.csv'

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

    output_df = pd.DataFrame()
    for filename in marathon_files:
        print 'Importing', folder+filename
        df = pd.read_csv(folder+filename)
        output_df = output_df.append(add_features(df))
        #output_df = output_df.append(sample_all(df))
    save_file = folder+SAVE_FILENAME
    print 'Saving', save_file
    output_df.to_csv(save_file, index=False)
