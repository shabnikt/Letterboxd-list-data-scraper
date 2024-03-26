from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

from src.help_lib.logfile import log
from src.help_lib.progress_bar_decoration import progress_bar


class HdRezkaScrapper:
    def __init__(self, films):
        self.films = films
        self.links = list()

    def get_links(self):
        log.debug("Run webdriver")
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        self.driver = webdriver.Chrome(options=options)

        self.driver.get("https://hdrezka.co/")

        self.search_film(self.films, "Parse streaming link:")
        self.driver.quit()
        return self.links

    @progress_bar
    def search_film(self, film):
        search_field = self.driver.find_element(By.ID, "search-field")
        search_field.clear()
        search_field.send_keys(film)
        time.sleep(1)
        self.get_film_link()

    def get_film_link(self):
        wait = WebDriverWait(self.driver, 10)
        first_result = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#search-results .b-search__section_list li a")))
        link = first_result.get_attribute("href")
        self.links.append(link)
