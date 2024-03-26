from requests import get as get_request
from bs4 import BeautifulSoup

from src.help_lib.progress_bar_decoration import progress_bar


class ImageParser:
    def __init__(self):
        self.film_imgs = list()

    @progress_bar
    def parse_list(self, film_page):
        parsed_img_url = self.get_parsed_img_url(film_page)
        self.film_imgs.append(parsed_img_url)

    def get_film_imgs(self):
        return self.film_imgs

    def get_parsed_img_url(self, film_page):
        response = get_request(film_page)
        soup = BeautifulSoup(response.content, 'html.parser')
        parsed_data = []

        for script in soup.find_all('script', type="application/ld+json"):
            data = script.string
            parsed_data.append(data)

        parsed_img = self.parse_img(parsed_data)

        return parsed_img

    @staticmethod
    def parse_img(parsed_data):
        cropped_str = str(parsed_data[0]).lstrip("\n/* <![CDATA[ */ {").strip('"')
        parsed_img = cropped_str.split('"')[2]

        return parsed_img
