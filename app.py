import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
import os
import time
import re
import json


# driver = webdriver.Chrome(ChromeDriverManager().install())

CITY = "Mumbai"
DATA_DIR = "data"

driver = webdriver.Chrome(ChromeDriverManager().install())
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"
}
parent_url = "https://www.proptiger.com"


def scroll_down(driver):
    """A method for scrolling the page uptil bottom"""

    # Get scroll height.
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:

        # Scroll down to the bottom.
        driver.execute_script("window.scrollTo(0, 300*document.body.scrollHeight);")

        # Wait to load the page.
        time.sleep(100)

        # Calculate new scroll height and compare with last scroll height.
        new_height = driver.execute_script("return document.body.scrollHeight")

        if new_height == last_height:
            break

        last_height = new_height


def get_property_geocodes(geo_url):
    if geo_url == "" or geo_url == "/":
        lat = None
        lon = None
    else:
        property_url = parent_url + geo_url
        response = requests.get(property_url, headers=headers)
        bs = BeautifulSoup(response.text, "html.parser")
        geo_info = json.loads(
            bs.find("div", {"class": "js-short-list short-list"})
            .find("script", {"type": "text/x-config"})
            .string
        )
        lat = geo_info["latitude"]
        lon = geo_info["longitude"]

    return lat, lon


def scrape_estate(soup, parent_url):
    property_lst = list()
    property_urls = list()
    geo_coordinates = list()

    for property in soup.find_all("section", {"class": "project-card-main-wrapper"}):
        property_details = json.loads(
            property.find("script", {"type": "text/x-config"}).string
        )

        property_lst.append(property_details)

        map_details = json.loads(
            property.find("div", {"class": "js-short-list short-list"})
            .find("script", {"type": "text/x-config"})
            .string
        )

        geo_url = map_details["URL"]
        property_urls.append(geo_url)

    print("property count: {0}".format(len(property_lst)))
    print("property url count: {0}".format(len(property_urls)))

    pool = ThreadPoolExecutor(max_workers=min(32, os.cpu_count() + 4))
    start_time = time.time()
    for lat, lon in pool.map(get_property_geocodes, property_urls):
        geo_coordinates.append((lat, lon))

    print("--- %s seconds ---" % (time.time() - start_time))
    df = pd.DataFrame(
        {"property_details": property_lst, "geocoordinates": geo_coordinates}
    )
    return df


if __name__ == "__main__":
    driver.get(parent_url + "/all-projects?cities={0}".format(CITY))
    scroll_down(driver)
    print("Completed scrolling down...")
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    property_info = scrape_estate(soup, parent_url)

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    property_info.to_csv(DATA_DIR + "/" + "{0}_property_data.csv".format(CITY))

    driver.quit()
