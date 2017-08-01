#-*- coding: utf-8 -*-
"""Scrap Tennis matches results on ATP.

- One shot scrapping to get historic results
- source : http://www.atpworldtour.com/en/scores/results-archive
"""

# Imports
import requests
from bs4 import BeautifulSoup, element
import csv
import time
from retrying import retry
import random
from datetime import datetime
import pandas as pd
import os, errno


# constants
URL_ATP = 'http://www.atpworldtour.com'
RESULTS = '/en/scores/results-archive'


def decorator_counter(func):
    """Decorate a function and count the number of times a function is used."""
    def wrapper(*args, **kargs):
        wrapper.count += 1
        res = func(*args, **kargs)
        return res
    wrapper.count = 0
    return wrapper


def decorator_exec_time(func):
    """Decorate a function and compute total exec time."""
    def wrapper(*args, **kargs):
        start_time = time.time()
        res = func(*args, **kargs)
        duration = time.time() - start_time
        print('It took {0} seconds'.format(duration))
        return res
    return wrapper


def sleeper(alpha, beta):
    """Stop execution for a random duration."""
    time.sleep(random.gammavariate(alpha,beta))


@decorator_counter
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_soup(page):
    """Get soup from an HTML page."""
    html_page = requests.get(page, timeout=1).content
    soup = BeautifulSoup(html_page, 'lxml')
    return soup


def get_years(page_results, limit_nb_years):
    """Extract urls containing rankings history (One url per week).

    - limit_nb_years controls max number of years we want to scrap
    """
    # Get page content
    soup = get_soup(page_results)

    # Locate the weeks in a scroll button
    years = soup.find(name='ul', attrs={'data-value': 'year'}).\
        find_all(name='li', attrs={'data-value': True, 'style': False})
    i = 0
    for w in years:
        if (i >= limit_nb_years and limit_nb_years is not None):
            print('break ', limit_nb_years)
            break
        else:
            i += 1
            year = {}
            year['year'] = w.get('data-value')
            year['results_url'] = page_results + "?year=" + \
                str(w.get('data-value'))
            yield year


def get_tourneys(page_tourneys):
    """Extract information about tourneys for a given year."""
    # Get page content
    soup = get_soup(page_tourneys)

    tourneys_soup = soup.find('table', attrs={'class': 'results-archive-table mega-table'}).\
        find_all('tr', attrs={'class': 'tourney-result'})

    for r in tourneys_soup:
        tourney = {}
        tourney_start_date = r.find('span', class_='tourney-dates').string.strip() #YYYY-mm-dd
        if datetime.strptime(tourney_start_date, "%Y.%m.%d").date() < datetime.now().date():
            tourney['start_date'] = r.find('span', class_='tourney-dates').string.strip()
            tourney['name'] = r.find('span', class_='tourney-title').string.strip()
            tourney['location'] = r.find('span', class_='tourney-location').string.strip()
            tourney['indoor_outdoor'] = r.find('td', class_='tourney-details').findNext('td').div.div.contents[0].strip()
            tourney['surface'] = r.find('td', class_='tourney-details').findNext('td').div.div.span.string.strip()
            tourney['results_url'] = r.find('a', class_='button-border').get('href')
        else:
            break
        yield tourney


def get_tourney_results(page_tourney, tourney):
    """Extract the results of a given tourney.

    - winner
    - loser
    - score (64 64 means 2 sets victory / 754 63 means 2 sets victory with a tie-break in the 1st set won 7 points to 4
    - url with detailed stats of the match for further development
    """
    soup = get_soup(page_tourney)
    for c in soup.find('table', class_='day-table').children:
        if isinstance(c, element.NavigableString):
            continue  # go to the next iteration if it is a blank line
        if isinstance(c, element.Tag):
            if c.name == 'thead':
                # print(c.tr.th.string)
                round_name = c.tr.th.string
            if c.name == 'tbody':
                matches_soup = c.find_all('tr')
                for m in matches_soup:
                    try:
                        match = {}
                        match['round'] = round_name
                        match['winner'] = ' '.join(m.find('td', class_='day-table-name').a.string.split())
                        match['loser'] = ' '.join(m.find('td', class_='day-table-name').findNext('td', class_='day-table-name').a.string.split())
                        match['score'] = m.find('td', class_='day-table-score').a.text.strip()
                        if m.find('td', class_='day-table-score').a.get('href') is not None:  # No URL for Walkover
                            match['stats_url'] = URL_ATP + m.find('td', class_='day-table-score').a.get('href')  # could be used to get detailed stats
                        else:
                            match['stats_url'] = ''
                        result= {}
                        result.update(match)
                        result.update(tourney)
                        result_encoded = {k:v.encode('utf8') for (k,v) in result.items()}
                        yield result_encoded
                    except AttributeError:
                        print('drop a match')



@decorator_exec_time
def build_matches_results_history(year_start=2010, year_stop=None, limit_nb_years=5):
    """For each year available on http://www.atpworldtour.com/en/scores/results-archive :
    build a .csv that contains all matches result (tourney description, winner, loser, score)

    - year_start and year_stop : control history start and stop. ATTENTION year_start < year_stop
    - nb_years : control hustory depth
    """
    # Number of Get Request on www.atpworldtour.com = nb_years * nb_yearly_touneys + 1 (to get years list)
    # Initialization
    header = ['name', 'winner', 'surface', 'round', 'loser', 'indoor_outdoor',
              'score', 'location', 'stats_url', 'results_url', 'start_date']  # CSV. columns names
    i = 1
    try:
        current_dir = './atp-matches-results-history/'
        os.makedirs(current_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    try:
        print('try1')
        for year in get_years(URL_ATP+RESULTS, limit_nb_years):
            if (int(year['year']) < year_start and year_start is not None) or (int(year['year']) > year_stop and year_stop is not None)  :
                print ('pass this year' + str(year['year']) + 'i is ' + str(i))
                continue  # Matches results are only scrapped for requested years
            #elif i > limit_nb_years and i is not None:
            #    print('leaving ' + str(i))
            #    break  # If enought years have been scrapped
            else:
                print('compute this year ' + str(year['year']) + 'i is ' + str(i))
                #rows_list = []
                current_file = current_dir + '/matches_results' + str(year['year'])+'.csv'
                with open(current_file, 'w') as f:
                    f_csv = csv.DictWriter(f, header)
                    f_csv.writeheader()
                    for tourney in get_tourneys(year['results_url']):  # For a given year, go through all tourneys
                        try :
                            gen_matches = get_tourney_results(URL_ATP + tourney['results_url'], tourney)
                            f_csv.writerows(gen_matches)
                            sleeper(3,1)
                        except TypeError:
                            print('drop ' + tourney['name'])
                            continue

            print('end i ' + str(i))
            i += 1
    finally :
        with open('./log-ATP-matches-results.log', 'w') as f:
            f.write('run at {0} with {1} get requests scrap {2} years'.format(datetime.now().date(), get_soup.count, i))

#%%

build_matches_results_history(year_start=2002, year_stop=2015, limit_nb_years=None)
