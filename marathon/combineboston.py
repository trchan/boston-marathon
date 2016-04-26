"""Package of methods for merging marathon running data from different
marathons (time, place), and add in new features.

Three main methods:
1. combine_marathons()
        - combine all dataframes and calculate new features
2. combine_marathons(sample=True)
        - Use Matching Estimators to sample similar individuals across datasets
        and merge.  Adds in new features.
3. collect_runners()
        - Use one dataset to provide a list of runners, build a dataset of
        those runner's previous race results.
"""

import pandas as pd
import numpy as np
from sys import stdout
import os


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
        if compass[direction] is not None:
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
        return 0, 0, 0, 0, 0, False, 0
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
    def get_variance_differences(df, runners, interval):
        """Calculates variances differences
        Parameters
        ----------
        df : DataFrame
        runners : list of integers
            ordered list of runners by  bib number
        interval : integer
            number of runners to calculate variances on

        Returns
        -------
        variance_differences : list of float
        """
        variance_differences = []
        for runner in runners:
            runners_before = df.index.isin(range(runner-interval, runner))
            runners_after = df.index.isin(range(runner, runner+interval))
            difference = np.var(df.loc[runners_after, 'offltime']) - \
                np.var(df.loc[runners_before, 'offltime'])
            variance_differences.append(difference)
        return variance_differences

    def find_max_variance(df, start_ix, end_ix, bin_size):
        """Searches a dataframe (at a given level of specificity) for the
        biggest change in variance.
        """
        runners = range(start_ix, end_ix, bin_size)
        var_window = 2 * bin_size
        # Restricts minimum variance window
        if var_window < 20:
            var_window = 20
        differences = get_variance_differences(df, runners, var_window)
        return runners[differences.index(max(differences))]

    # Eliminate gaps in the data, by sorting and reindexing.
    sorted_df = df.sort_values(by='bib')
    sorted_df.reset_index(inplace=True)
    # First iteration, do a broad search every 500 runners, measuring the
    # variance difference of the 1000 runners preceding against the 1000
    # runners after (window=1000).
    # Find the point of the biggest change in variance.
    max_var_runner = find_max_variance(sorted_df, sorted_df.index.min()+500,
                                       sorted_df.index.max()-500, 500)
    # Second iteration, use the point found above to zoom in.  Search every 50
    # bibs, with a 100 bib variance window.
    max_var_runner = find_max_variance(sorted_df, max_var_runner-100,
                                       max_var_runner+100, 50)
    # Final iteration, search individual runners
    max_var_runner = find_max_variance(sorted_df, max_var_runner-10,
                                       max_var_runner+10, 1)
    return sorted_df.loc[max_var_runner, 'bib']


def fill_in_missing_splits(df):
    """Fills in missing splits with an interpolated value, and creates dummy
    columns to track missed splits.
    Parameters
    ----------
    df : DataFrame
        Containing running records
    Returns
    -------
    df : DataFrame
        Containing running records with interpolated splits and missed split
        columns
    """
    splits = ['starttime', 'time5k', 'time10k', 'time15k', 'time20k',
              'timehalf', 'time25k', 'time30k', 'time35k', 'time40k',
              'offltime']
    missed_splits_cols = ['miss5k', 'miss10k', 'miss15k', 'miss20k',
                          'misshalf', 'miss25k', 'miss30k', 'miss35k',
                          'miss40k']
    split_dist = {'starttime': 0., 'time5k': 5., 'time10k': 10.,
                  'time15k': 15., 'time20k': 20., 'timehalf': 21.098,
                  'time25k': 25., 'time30k': 30, 'time35k': 35.,
                  'time40k': 40., 'offltime': 42.195}

    def get_interpolated_time(runner, split, last_known_split,
                              next_known_split):
        '''Returns interpolated time
        '''
        distance = split_dist[next_known_split] - split_dist[last_known_split]
        time_diff = runner[next_known_split] - runner[last_known_split]
        pace = time_diff / distance
        time_to_interpolate = split_dist[split] - split_dist[last_known_split]
        return runner[last_known_split] + pace * time_to_interpolate

    # Create new columns of boolean indicating missed splits
    for newcol in missed_splits_cols:
        df[newcol] = False
    # Iterate through runners
    for ix, runner in df.iterrows():
        # Iterate through splits, find and fill in empty value
        last_known_split = splits[0]
        for split_ix, split in enumerate(splits[1:-1]):
            if runner[split] > 0:
                last_known_split = split
            elif runner[split] == 0:
                # Find next known split
                for next_split in splits[split_ix+1:]:
                    if runner[next_split] != 0:
                        next_known_split = next_split
                        break
                # Interpolate missing value difference
                df.loc[ix, split] = \
                    get_interpolated_time(runner, split, last_known_split,
                                          next_known_split)
                # Change dummy to record missing split
                df.loc[ix, missed_splits_cols[split_ix]] = True
    return df


def add_features(df, splits=True):
    """Add features to existing dataframe.

    Parameters
    df : DataFrame
    splits : boolean
        Whether to fill in empty split times (computationally intensive)

    Returns
    augmented_df : Data Frame
    """
    # Features to include from each marathon
    augmented_df = df[['marathon', 'year', 'firstname', 'bib', 'age',
                       'gender', 'offltime', 'starttime', 'time5k', 'time10k',
                       'time15k', 'time20k', 'timehalf', 'time25k', 'time30k',
                       'time35k', 'time40k']].copy()
    # Add runner categories
    augmented_df['elite'] = augmented_df['bib'] <= 100
    augmented_df['qualifier'] = augmented_df['bib'] < \
        find_nonqualifier_start(augmented_df)
    augmented_df['home'] = [state if country == 'USA' else country for
                            country, state in df[['country', 'state']].values]
    # Manipulate running data
    if splits:
        augmented_df = fill_in_missing_splits(augmented_df)

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


def add_features_for_priors(df):
    """Add features to existing "prior" dataframe.  This is the dataframe that collect_runners() builds.

    Parameters
    df : DataFrame

    Returns
    augmented_df : Data Frame
    """
    # Features to include from each marathon
    augmented_df = df[['marathon', 'year', 'bib', 'name', 'firstname',
                       'lastname', 'age', 'gender', 'city', 'state',
                       'country', 'citizenship', 'offltime',
                       'prior_marathon', 'prior_year', 'prior_time']].copy()
    # Add runner categories
    augmented_df['elite'] = False
    augmented_df['qualifier'] = False
    augmented_df['home'] = [state if country == 'USA' else country for
                            country, state in df[['country', 'state']].values]
    # Add weather columns
    marathon_name = df['prior_marathon'].iloc[0]
    year = augmented_df['prior_year'].iloc[0]
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
    return sample_df


def year_of_birth(runner):
    """Returns an estimate for the birth year of a runner.
    Parameters
    ----------
    runner : DataFrame

    Output
    ------
    birth_year : int
        estimated year of birth
    """
    birth_year = runner['year'] - runner['age']
    return birth_year


def runner_tiebreaker_(runner, df):
    '''Determines which record in df is the best match for a given runner.
    - middle initial
    - city
    - state
    - country
    - exact birth year
    - exact name match

    Parameters
    ----------
    runner : pd.Series
        Runner to match
    df : pd.DataFrame
        Runners to consider

    Returns
    -------
    best_match : pd.Series
    '''
    score = dict()
    runner_yob = year_of_birth(runner)
    for ix, runner2 in df.iterrows():
        score[ix] = 0
        if runner['state'] == runner2['state']:
            score[ix] += 2
        if runner['country'] == runner2['country']:
            score[ix] += 2
        if runner['city'] == runner2['city']:
            score[ix] += 2
        if runner['name'] == runner2['name']:
            score[ix] += 1
        # score for first initial

        def get_initial(name):
            '''Returns the middle initial of a name.  Format of name is
            expected to be 'Lastname, Firstname Patty'
            >>> get_initial('Lastname, Firstname Patty')
            'P'
            '''
            return name.split(' ')[-1][0]

        if get_initial(runner['name']) == get_initial(runner2['name']):
            score[ix] += 3
        if runner_yob == year_of_birth(runner2):
            score[ix] += 1
    best_match_ix = max(score, key=score.get)
    return df.loc[best_match_ix]


def do_ages_match(runner_yob, df):
    '''Checks whether runner matches age
    Parameters
    ----------
    runner_yob: integer
        runner's year of birth (eg. 1969)
    df: DataFrame
        Contains list of runners that may match runner yob
    '''
    if (df['age'] >= 0).all():
        return abs(year_of_birth(df) - runner_yob) <= 1
    else:
        # use age range, since age is not provided
        return (runner_yob <= df['year'] - df['minage']) & \
            (runner_yob >= df['year'] - df['maxage'])


def find_same_runner(runner, df, lastnames):
    '''Searches df for runner.
    Criteria:
        - first name must match
        - last name must match
        - Gender must match
        - Birthyear must be +/- 1 year
        - Tiebreaker, see runner_tiebreaker_()

    Parameters
    ----------
    runner : pandas.core.series.Series
        One running record.
        Must have ['lastname', 'firstname', 'gender', 'age']
    df : pandas DataFrame
        Dataframe to find matching runner record

    Returns
    -------
    found_runner : pandas.core.series.Series
        Matching row from df
    '''
    # Iteratively match categories, for algorithmic speed
    if runner['lastname'] in lastnames:
        match = (df['lastname'] == runner['lastname'])
        match_df = df.loc[match]
        if len(match_df) > 0:
            # Check that firstname matches
            match = match_df['firstname'] == runner['firstname']
            match_df = match_df.loc[match]
            if len(match_df) > 0:
                # Check that gender matches
                match = match_df['gender'] == runner['gender']
                match_df = match_df.loc[match]
                if len(match_df) > 0:
                    # Check that birthyear matches
                    runner_yob = year_of_birth(runner)
                    match = do_ages_match(runner_yob, match_df)
                    match_df = match_df.loc[match]
                    if len(match_df) == 1:
                        return match_df.iloc[0]
                    elif len(match_df) > 1:
                        # Tiebreaker
                        # print len(match_df), 'matches for:', runner['name'],\
                        # runner['city']
                        match_df = runner_tiebreaker_(runner, match_df)
                        # print match_df['name'], match_df['city']
                        return match_df
    return pd.Series()


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


def combine_marathons(filelist, sample=False):
    output_df = pd.DataFrame()
    for filename in marathon_files:
        print 'Importing', FOLDER+filename
        df = pd.read_csv(FOLDER+filename)
        if sample:
            sample_df = sample_all(df)
            output_df = output_df.append(add_features(sample_df))
        else:
            output_df = output_df.append(add_features(df))
    return output_df


def collect_runners(runners_file, marathon_files):
    '''retrieves runners from runners_file, and collects/extracts their running
    histories.

    Parameters
    ----------
    runners_file : string
        filename containing a list of runners and their bio
    marathon_files : list of string
        set of marathon files to extract running records from

    Returns
    -------
    output_df : Pandas DataFrame
    '''
    # List of columns to keep from runners_file
    current_features = ['marathon', 'year', 'bib', 'name', 'firstname',
                        'lastname', 'age', 'gender', 'city', 'state',
                        'country', 'citizenship', 'offltime']
    # List of columns to grab from runner's history, for each race
    prior_features = ['marathon', 'year', 'offltime']
    # New name of columns from runner's history
    prior_feature_names = ['prior_marathon', 'prior_year', 'prior_time']

    output_df = pd.DataFrame()
    runners_df = pd.read_csv(FOLDER+runners_file)
    num_runners = len(runners_df)
    for filename in marathon_files:
        print 'Extracting runners from:', FOLDER+filename
        df = pd.read_csv(FOLDER+filename)
        lastnames = set(df['lastname'])
        extracted_runners = []
        for ix, runner in runners_df.iterrows():
            print '\r{0:.0f}%'.format(ix*100. / num_runners),
            stdout.flush()
            prior = find_same_runner(runner, df, lastnames)
            if len(prior) > 0:
                prior = prior[prior_features]
                prior.index = prior_feature_names
                extracted_runners.append(runner.append(prior))
        if len(extracted_runners) == 0:
            extracted_df = pd.DataFrame(columns=current_features + prior_feature_names + ['elite', 'qualifier', 'home', 'avgtemp', 'avghumid', 'avgwind', 'avgwindE', 'avgwindN', 'isgusty', 'rainhours'])
        else:
            extracted_df = pd.concat(extracted_runners, axis=1).T
            extracted_df = add_features_for_priors(extracted_df)
        print extracted_df.shape
        output_df = output_df.append(extracted_df)
    return output_df


def create_misc_home(df):
    '''Cleans up the 'home' category column by reducing the number of
    categorical values.  All categories <0.1% of all records are merged as
    'MISC'
    '''
    print "Binning home category as 'MISC' if size <0.1% of all records"
    n = len(df)
    count_df = df['home'].value_counts()
    misc_list = set(count_df[count_df < n / 1000].index)
    print len(misc_list), "categories converting to 'MISC'"
    df.loc[df['home'].isin(misc_list), 'home'] = 'MISC'
    print sum(df['home'] == 'MISC'), "records binned as 'MISC'"
    return df


def getmarathonguide(folder):
    '''Searches folder for '*_clean.csv', and returns list of files.
    '''
    output = []
    file_list = os.listdir(FOLDER+folder)
    for file in file_list:
        if file.find('_clean.csv') > 0:
            output.append(folder+file)
    return output


# Estimator Definition, results in 40 x 2 estimators
AGE_MIN = 21
AGE_MAX = 60
GENDERS = (True, False)
AGES = range(AGE_MIN, AGE_MAX+1, 1)
SAMPLE_SIZE = 50

FOLDER = 'data/'
weather_file = 'marathon_weather.csv'
weather_df = pd.read_csv(FOLDER+weather_file)
SAVE_FILENAME = 'boston2015_priors+.csv'

if __name__ == '__main__':
    marathon_files = [# 'boston2015_clean.csv',
                      'boston2014_clean.csv', 'boston2013_clean.csv',
                      'boston2012_clean.csv']
    marathon_files = getmarathonguide('marathonguide/') + marathon_files
    # output_df = combine_marathons(marathon_files)
    output_df = collect_runners('boston2015_clean.csv', marathon_files)
    # Final processing
    output_df = create_misc_home(output_df)
    save_file = FOLDER+SAVE_FILENAME
    print 'Saving', save_file
    output_df.to_csv(save_file, index=False)
    print len(output_df), 'records saved'
