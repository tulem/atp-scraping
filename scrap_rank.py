#-*- coding: utf-8 -*-
"""Scrap Tennis players weekly ranks on ATP.

- One shot scrapping to get historic rankings
- Tool for weekly scraping when ATP plublish ranking update
- source : http://www.atpworldtour.com/en/rankings/singles
"""

# Imports
from helper_functions import *
import config


def get_page_weekly_rankings(page_rankings, depth=500):
    """Extract urls containing rankings history (One url per week)

    depth controls how many players are wanted"""

    # Get page content
    soup = get_soup(page_rankings)

    # Locate the weeks in a scroll button
    weeks = soup.find(name ='ul', attrs={'data-value':'rankDate'}).\
    find_all(name='li', attrs={'data-value': True})

    for w in weeks:
        week = {}
        week['week'] = w.get('data-value')
        week['week_url'] = page_rankings + "?rankDate=" + \
        str(w.get('data-value')) + "&rankRange=0-" + str(depth)
        yield week


def get_players_info(page):
    """Extract players main info (name, ranking, age, points)."""

    # Get page content
    soup = get_soup(page)

    # locate players information
    players_soup = soup.tbody.find_all('tr')
    for p in players_soup:
        try:
            player = {}
            player['name'] = p.find('td', class_='player-cell').a.string
            player['ranking'] = p.find('td', class_='rank-cell').string.strip()
            player['age'] = p.find('td', class_='age-cell').string.strip()
            player['points'] = p.find('td', class_='points-cell').a.string
            yield player
        except Exception as e:
            print e  # coding=utf-8
            continue


@decorator_exec_time
def build_rankings_history(week_start=None, week_stop=None, nb_weeks=12, ranking_depth=500):
    """For each ranking week available on atpworldtour.com :
    build a .csv with player (name, ranking, age, ATP points)

    - nb_weeks : controls number of weeks wanted in history (from most recent),
    default is 12 (about 3 months)
    - ranking_depth : controls max ranking of players, default is 500
    - Get all rankings between week_start and week_stop
    """

    header = ['name', 'ranking', 'age', 'points']
    ATP_RANKINGS = "http://www.atpworldtour.com/en/rankings/singles"
    i = 0
    try:
        dir_rankings_history = './atp-ranking-history/'
        os.makedirs(dir_rankings_history)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    try:
        for w in get_page_weekly_rankings(ATP_RANKINGS, ranking_depth):
            if nb_weeks is not None and i > nb_weeks:
                print('break', w['week'])
                break
            if (week_start is not None and w['week'] > week_start) or w['week'] < week_stop:
                print('continue', w['week'])
                continue
            else:
                try:
                    # Create 1 dir per year
                    dir_year = dir_rankings_history + w['week'][0:4] + '/'
                    os.makedirs(dir_year)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                print('Getting ATP rankings for week {0}'.format(w['week']))
                my_gen = get_players_info(w['week_url'])

                with open(dir_year + w['week']+'.csv', 'w') as f:
                    f_csv = csv.DictWriter(f, header)
                    f_csv.writeheader()
                    f_csv.writerows(my_gen)
                sleeper(3,1)
                i +=1
    #except Exception as e:
        #print e
    finally:
        with open('./log-ATP-rankings-scrap.txt', 'w') as f:
            f.write('OK')

# %%
build_rankings_history(week_stop='2001-31-12', week_start = '2007-02-12',ranking_depth=500, nb_weeks=None)
