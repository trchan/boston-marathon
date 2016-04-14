"""Package of methods for merging marathon running data from different
marathons (time, place).  Using a technique known as Matching Estimators.
"""

import pandas as pd


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
    print df['marathon'][0], df['year'][0]
    estimator_df = df[(df['gender'] == gender) & (df['age'] == age)]
    return estimator_df.sample(n=SAMPLE_SIZE, replace=True, random_state=42)


def sample_all(df):
    """Get all matching estimators from DataFrame
    Parameters
    ----------
    df : DataFrame

    Output
    ------
    DataFrame
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
WEATHER_DF = pd.read_csv(weather_file)

if __name__ == '__main__':
    folder = 'data/'
    marathon_files = ['boston2015_clean.csv', 'boston2014_clean.csv',
                      'boston2013_clean.csv']

    matching_estimators = pd.DataFrame()
    for filename in marathon_files:
        df = pd.read_csv(folder+filename)
        matching_estimators = matching_estimators.append(sample_all(df))
    print matching_estimators.shape
