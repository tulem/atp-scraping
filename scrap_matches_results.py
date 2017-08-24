#-*- coding: utf-8 -*-
"""Scrap Tennis matches results on ATP.

- One shot scrapping to get historic results
- source : http://www.atpworldtour.com/en/scores/results-archive
"""

# Imports
from bs4 import element
import csv
from datetime import datetime
import os
import errno
from helper_functions import *
import config


# Constants
ATP_BSE_URL = 'http://www.atpworldtour.com'
ATP_MATCHES_RESULTS_URL = '/en/scores/results-archive'

PATH_TO_STORAGE_DIR = config.path_to_storage_dir


def get_years(page_results, limit_year_start, limit_year_stop, limit_nb_years):
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
        if (i >= limit_nb_years and limit_nb_years != 0):
            print('break ', limit_nb_years)
            break
        else:
            year = {}
            year['year'] = w.get('data-value')
            year['results_url'] = page_results + "?year=" + \
                str(w.get('data-value'))
            if (int(year['year']) < limit_year_start and limit_year_start !=0) or (int(year['year']) > limit_year_stop and limit_year_stop != 0)  :
                print ('pass this year' + str(year['year']))
                continue  # Matches results are only scrapped for requested years
            else:
                yield year
                i += 1

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
                            match['stats_url'] = ATP_BSE_URL + m.find('td', class_='day-table-score').a.get('href')  # could be used to get detailed stats
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
def build_matches_results_history(limit_year_start=2010, limit_year_stop=0, limit_nb_years=5):
    """For each year available on http://www.atpworldtour.com/en/scores/results-archive :
    build a .csv that contains all matches result (tourney description, winner, loser, score)

    - year_start and year_stop : control history start and stop. ATTENTION year_start < year_stop
    - nb_years : control history depth
    """
    # Number of Get Request on www.atpworldtour.com = nb_years * nb_yearly_touneys + 1 (to get years list)
    # Initialization
    header = ['name', 'winner', 'surface', 'round', 'loser', 'indoor_outdoor',
              'score', 'location', 'stats_url', 'results_url', 'start_date']  # CSV. columns names
    try:
        storage_dir = PATH_TO_STORAGE_DIR
        os.makedirs(storage_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    try:
        print('try1')
        for year in get_years(ATP_BSE_URL+ATP_MATCHES_RESULTS_URL, limit_year_start, limit_year_stop, limit_nb_years):
            print('compute this year ' + str(year['year']))
            #rows_list = []
            current_file = storage_dir + '/matches_results' + str(year['year'])+'.csv'
            with open(current_file, 'w') as f:
                f_csv = csv.DictWriter(f, header)
                f_csv.writeheader()
                for tourney in get_tourneys(year['results_url']):  # For a given year, go through all tourneys
                    try :
                        gen_matches = get_tourney_results(ATP_BSE_URL + tourney['results_url'], tourney)
                        f_csv.writerows(gen_matches)
                        sleeper(3,1)
                    except (TypeError, AttributeError):
                        print('drop ' + tourney['name'])
                        continue
    finally :
        with open(PATH_TO_STORAGE_DIR+ 'scrapping_matches_results.log', 'w') as f:
            f.write('run at {0} with {1} get requests'.format(datetime.now().date(), get_soup.count))

#%%

build_matches_results_history(limit_year_start=2017, limit_year_stop=2017, limit_nb_years=2)
