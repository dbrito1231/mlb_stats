"""
author: Damian Brito
date: 09/02/2021
name: MLB Scraper
filename: scraper.py
description: class goes to a designated mlb.com url to
             pull overall hitting and pitching stats from
             all teams.

modules:
    time - used to pause script to give enough time to pull
            all data
    pandas - used to clean and mung web data.
    selenium - web scraper used to pull data from MLB.com.
               webdriver: drivers for selected browser scraper
               Options: used to set Chrome scraper to headless mode.
"""
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# list contains team names and part of the urls used to access
# team stats from MLB.com (ie. https://www.mlb.com/yankees/stats/team/).
# Will be used for later versions.
from typing import Tuple

teams = [("Arizona Diamondbacks", "dbacks"),
         ("Atlanta Braves", "braves"),
         ("Baltimore Orioles", "orioles"),
         ("Boston Red Sox", "redsox"),
         ("Chicago Cubs", "cubs"),
         ("Chicago White Sox", "whitesox"),
         ("Cincinnati Reds", "reds"),
         ("Cleveland Indians", "indians"),
         ("Colorado Rockies", "rockies"),
         ("Detroit Tigers", "tigers"),
         ("Houston Astros", "astros"),
         ("Kansas City Royals", "royals"),
         ("Los Angeles Angels", "angels"),
         ("Los Angeles Dodgers", "dodgers"),
         ("Miami Marlins", "marlins"),
         ("Milwaukee Brewers", "brewers"),
         ("Minnesota Twins", "twins"),
         ("New York Mets", "mets"),
         ("New York Yankees", "yankees"),
         ("Oakland Athletics", "athletics"),
         ("Philadeliphia Phillies", "phillies"),
         ("Pittsburgh Pirates", "pirates"),
         ("San Diego Padres", "padres"),
         ("San Francisco Giants", "giants"),
         ("Seattle Mariners", "mariners"),
         ("St. Louis Cardenals", "cardenals"),
         ("Tampa Bay Rays", "rays"),
         ("Texas Rangers", "rangers"),
         ("Toronto Blue Jays", "bluejays"),
         ("Washington Nationals", "nationals")]


def clean_raw(web_data, head_ind_start: int) -> pd.DataFrame:
    """
    Takes in data pulled from selenium object and site and cleans the data into a pandas dataframe
    :param web_data: pulled uncleaned data from website
    :param head_ind_start: number to recognize and separate headers from data
    :return: dataframe with organized data before data munging
    """
    # scraped data is converted into list of strings
    raw_data = web_data.text.split('\n')
    # headers are pulled from slicing raw_data by the index number
    headers = raw_data[:head_ind_start]
    # headers are pulled from slicing data starting from the first data number
    data = raw_data[head_ind_start:]
    # empty list is created to hold formatted data
    df_lst = []
    # after slicing, the data will be ordered in the list by index number, team name, and stats.
    # for loop checks for the length of each item in list
    for row in range(len(data) - 2):
        # if the length of row passed is less or equal to 2 characters then it
        # is considered the index to the table pulled
        if len(data[row]) <= 2:
            team_name = data[row + 1]
            num_data = data[row + 2]
            # a new list is created with the team name included
            clean_data = [team_name]
            # num_data is converted into list and each is appended to clean_data
            for number in num_data.split(' '):
                clean_data.append(number)
            # clean data is then appended to df_lst
            df_lst.append(clean_data)
    # after each record is formatted and added to df_lst
    # df_lst is then added to dataframe and headers is also added as column names
    dataframe = pd.DataFrame(df_lst, columns=headers)
    return dataframe
    # end of clean_raw


def data_mung(stats_df: pd.DataFrame):
    """
    Used to convert numerical columns into appropriate data types.
    :param stats_df: dataframe with extracted web data. This would
                     edit the existing dataframe already stored in
                     memory instead of returning a new dataframe.
    """
    # list of column names found in stats_df.  These columns will be used
    # to assign columns as float numbers.
    float_cols = ['AVG', 'OBP', 'SLG', 'OPS', 'IP',
                  'ERA', 'WHIP', 'AVG']

    # for loop to navigate to each column in stats_df
    for col in stats_df.columns.tolist():
        try:
            # every column that is not found in float_cols is considered
            # either an integer or a string column.
            if col not in float_cols:
                # if the column is not in float_cols then the column dtype
                # is changed to int.
                stats_df[col] = stats_df[col].astype('int')
            # if the column name is found, then the column is reassigned with
            # the float32 dtype
            elif col in float_cols:
                stats_df[col] = stats_df[col].astype('float32')
        # if the ValueError is thrown, then the column only contains alphabetical
        # strings. Therefore it is left alone and the loop continues.
        except ValueError:
            continue
    # end of data_mung


class TeamStats:
    """
    Class is used to load web scraping object and extract
    data from url to be processed.
    """
    def __init__(self):
        """
        Class constructor. Loads options for selenium object and url used to scrape
        data from.

        self.options: contains selenium options object. The headless attribute is set to true
                        so that selenium object does not appear on screen.
        self.url: url used to scrape and retrieve data from.  Can also be changed to select
                    data hosted from another team's url.
                    (ie. https://www.mlb.com/yankees/stats/team/,
                    https://www.mlb.com/redsox/stats/team/,
                    https://www.mlb.com/mets/stats/team/)
        self.hit_stats: holds hitting stats dataframe. Currently set to None.
        self.pitch_stats: holds pitching stats dataframe. Currently set to None.
        """
        self.options = Options()
        self.options.headless = True
        self.url = "https://www.mlb.com/yankees/stats/team/"
        self.hit_stats = None
        self.pitch_stats = None

    def show_hit_stats(self):
        """
        Prints hitting stats dataframe as string
        """
        print(self.hit_stats)

    def show_pitch_stats(self):
        """
        Prints pitching stats dataframe as string
        """
        print(self.pitch_stats)

    def get_stats(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns a tuple both hitting/pitching dataframes
        return (hitting stats, pitching stats)
        """
        return self.hit_stats, self.pitch_stats

    def change_url(self, team_name: str):
        """

        :param team_name: string that contains team name
        :return: a new url pointing to the entered team's site.
        """
        # list comprehension that searches all tuples in
        # teams list if team name matches any team entries.
        # It returns the team url.
        new_team = [n[1] for n in teams if team_name.lower() in n[0].lower()]
        # if the partial url is found, then self.url is changed to
        # reflect the team's MLB.com link.
        self.url = f"https://www.mlb.com/{new_team[0]}/stats/team/"
        # end of change_url

    def pull_data(self):
        """
        Function pulls in data from url using content xpath locations.
        Does not take parameters but pulls in class attributes.
        """
        # xpath location of table to be pulled
        div_xpath = '/html/body/main/div[2]/section/section/div[3]'
        # selenium object is created by loading selenium chrome driver.
        # if the chrome driver is set to another locaiton, the executable_path should
        # be changed. Options object is loaded to enable headless
        # mode on selenium object.
        driver = webdriver.Chrome(options=self.options,
                                       executable_path=r"C:\chromedriver\chromedriver.exe")
        # Selenium object begins to pull data from class url attribute.
        driver.get(self.url)
        # Set timer to give selenium object to fully pull url data.
        time.sleep(5)
        # Selenium object uses div_xpath to pull in hitting stats table
        # from url and assigns to new variable.
        team_stats_hit = driver.find_element_by_xpath(div_xpath)
        # clean_raw function begins to clean team_stats_hit and the number
        # 18 is passed for slicing/splitting headers and data
        self.hit_stats = clean_raw(team_stats_hit, 18)
        # driver looks for Pitching option button in url to switch
        # from hitting to pitching stats
        python_button = driver.find_elements_by_xpath(
            "/html/body/main/div[2]/section/div[2]/div/div[2]/button")
        # button is clicked twice to ensure pitching data is selected
        python_button[0].click()
        python_button[0].click()
        # 5 second timer is added to ensure data is pulled
        time.sleep(5)
        # pitching dats is pulled from url and assigned to team_stats_pitch
        team_stats_pitch = driver.find_element_by_xpath(div_xpath)
        # clean_raw function begins to clean team_stats_pitch and the number
        # 21 is passed for slicing/splitting headers and data. The pitching stats
        # have a larger table than hitting stats
        self.pitch_stats = clean_raw(team_stats_pitch, 21)
        # close the selenium scraper object to free up memory
        driver.quit()
        # both hit and pitch stats as passed to data_mung function, where the columns
        # will be assigned appropriate dtypes
        data_mung(self.hit_stats)
        data_mung(self.pitch_stats)
    # end of pull_data


if __name__ == '__main__':
    testObj = TeamStats()
    testObj.change_url('red sox')
    testObj.pull_data()
