#-*- coding: utf-8 -*-
"""Scrap Tennis players weekly ranks on ATP.
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
from retrying import retry
import random


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
