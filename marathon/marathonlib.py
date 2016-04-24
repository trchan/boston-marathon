"""Assortment of helper functions for manipulating, searching, and plotting
marathon running data.
"""

import pandas as pd
from numpy import float64
import matplotlib.pyplot as plt
from string import punctuation


def time_to_minutes(time_string):
    """Converts a string representation of time to number of minutes.
    INPUT:
    time_string : string in 'hh:mm:ss' format
    OUTPUT:
    float
        in minutes

    Examples
    --------
    >>> print time_to_minutes('1:23:45')
    83.75
    >>> print time_to_minutes('1:23')
    1.38333333333
    >>> print time_to_minutes('hour')
    0.0
    >>> print time_to_minutes('10:00:00')
    600.0
    """
    if type(time_string) in (float, float64):
        return time_string
    try:
        units = map(int, time_string.split(':'))
    except ValueError:
        units = [0]
    minutes = 0
    for unit in units:
        minutes = unit/60. + minutes*60
    return minutes


def time_to_timestring(time_min):
    """Converts a string representation of time to number of minutes.
    INPUT:
        time_min: float, in minutes
    OUTPUT:
        string, time in 'hh:mm:ss' format
    Example:
    >>> print time_to_timestring(0)
    0:00:00
    >>> print time_to_timestring(83.75)
    1:23:45
    >>> print time_to_timestring(1.38333333333)
    0:01:23
    >>> print time_to_timestring(600)
    10:00:00
    """
    hh = int(time_min) / 60
    mm = int(time_min) % 60
    ss = int(round((time_min % 1)*60, 0))
    return '{0}:{1:02}:{2:02}'.format(hh, mm, ss)


def find_missing_records(df):
    """Returns a list missing runners, specified by 'rank' of finish
    INPUT:
    df: DataFrame
    OUTPUT:
    list (of missing ranks)
    """
    ranks = set(df['rank'])
    cheaters = []
    for ix in range(1, df['rank'].max()):
        if ix not in ranks:
            cheaters.append(ix)
    return cheaters


def plot_runners_by_gender(df):
    num_bins = df['age'].max() - df['age'].min()
    df[df['gender']]['age'].hist(bins=num_bins, color='b', alpha=0.5)
    df[~df['gender']]['age'].hist(bins=num_bins, color='r', alpha=0.5)
    plt.legend(['men', 'women'])
    plt.xlabel('Age')
    plt.ylabel('Number of Runners')
    plt.title('Number of Runners by Age')


def plot_distribution_of_times(df):
    finish_times = map(m.time_to_minutes, df['offltime'])
    print 'max  : ', m.time_to_timestring(max(finish_times))
    print 'min  : ', m.time_to_timestring(min(finish_times))
    print 'mean : ', m.time_to_timestring(np.mean(finish_times))
    bins = int(max(finish_times) - min(finish_times))+1
    print bins
    plt.hist(finish_times, bins=bins/5, alpha=0.3, normed=True)


def clean_name(name):
    '''
    INPUT:
        name: string
    OUTPUT:
        (string, string): firstname, lastname
    Sample conversions:
        Lastname, Firstname A          --> FIRSTNAME, LASTNAME
        Abraham Peregrina, Nahim       --> NAHIM, ABRAHAMPEREGRINA
        Aase, Geir Harald              --> GEIR, AASE
        Abdallah, Michael A            --> MICHAEL, ABDALLAH
        Abreu, Boris R.                --> BORIS, ABREU
        Abou-Zamzam, Ahmed M. Jr.      --> AHMED, ABOUZAMZAM
        Zuccardi Merli, Gianluigi      --> GIANLUIGI, ZUCCARDIMERLI
    Problem names have multiple commas:
        Sung, Kwong Hung, Patrick
        Mercado, M.D., Michael G.
    '''
    names = name.split(',')
    lastname = names[0]
    firstname = names[1]
    lastname = [c.upper() for c in lastname if c not in punctuation+' ']
    lastname = "".join(lastname)
    firstname = firstname.split()[0]
    firstname = [c.upper() for c in firstname if c not in punctuation+' ']
    firstname = "".join(firstname)
    if len(names) > 2:
        print '{0:30} --> {1}, {2}'.format(name, firstname, lastname)
    return firstname, lastname


if __name__ == "__main__":
    pass
