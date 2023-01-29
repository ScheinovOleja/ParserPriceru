import asyncio
import datetime
from pyexpat import ExpatError

import aiofiles as aiofiles
import pandas as pd
import undetected_chromedriver as uc
import xmltodict
from aiohttp import ClientSession
from lxml.etree import XMLSyntaxError
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait


class Parser:

    def __init__(self):
        self.cookie = {
            "history": "4741713",
            "otherSessionData": "{\"split\":\"a\"}",
            "promo": "true",
            "puid": "CuQV7mPUDo2eV+LVCPw7Ag==",
            "qrator_jsid": "1674989284.094.lyV7A78mRV8FEkNH-rsmq8t7h8mdr1b35tbr57n8nbdn7gb83",
            "qrator_jsr": "1674989284.094.lyV7A78mRV8FEkNH-rpog47k5pc8e2rh76hnef0asfdsqdclc-00",
            "qrator_ssid": "1674989284.483.bPVfc54XSWmg04GU-righcufku3j5tl30f4ekllnmd4su541g",
            "region": "{\"id\":1,\"name\":\"ÐÐ¾ÑÐºÐ²Ð°\",\"subdomain\":\"\"}",
            "ruid": "AAAPomPU+hsAATEQASsuSgA=",
            "session": "{\"id\":\"192e9701-787f-4a87-8cb6-6e9fb10b4ea2\",\"depth\":11,\"ts\":\"29.01.2023, 09:47:44\",\"rhost\":\"\",\"cpc\":0}",
            "sessionLastAction": "1674989286232",
            "uuts": "4vrJyHgzYXIYw3Z7dEpfXvljik5ESjNp"
        }

        self.headers_categories = {"Host": "price.ru",
                                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Safari/537.36 OPR/52.0.2871.99',
                                   "Accept": "application/json, text/plain, */*",
                                   "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                                   "Accept-Encoding": "gzip, deflate, br",
                                   "Authorization": "Basic undefined",
                                   "Content-Type": "application/json",
                                   "Content-Length": "14",
                                   "Origin": "https://price.ru",
                                   "Connection": "keep-alive",
                                   "Sec-Fetch-Dest": "empty",
                                   "Sec-Fetch-Mode": "cors",
                                   "Sec-Fetch-Site": "same-origin",
                                   "Pragma": "no-cache",
                                   "Cache-Control": "no-cache"}
        self.headers_redirect_to_shop = {"Host": "price.ru",
                                         "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
                                         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                                         "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                                         "Accept-Encoding": "gzip, deflate, br",
                                         "Connection": "keep-alive",
                                         "Upgrade-Insecure-Requests": "1",
                                         "Sec-Fetch-Dest": "document",
                                         "Sec-Fetch-Mode": "navigate",
                                         "Sec-Fetch-Site": "none",
                                         "Sec-Fetch-User": "?1",
                                         "Pragma": "no-cache",
                                         "Cache-Control": "no-cache"}
        self.region_id = 1
        self.all_categories = [
            5209,  # мониторы
            920000000002,  # процессоры все
            920000000003,  # оперативка все
            5903,  # видеокарты
            5915,  # тв тюнеры
            5913,  # кулеры
            5907,  # корпуса
            5917,  # блоки питания
            5911,  # конструкторы
            5715,  # SSD
            5701,  # жесткие диски
            5910,  # Материнки
            9500000000000004,  # аксессуары все
            920000000012,  # дисководы все
            9600000000000036,  # контроллеры все
        ]
        self.params_category = {
            "region_id": self.region_id,
            "page": 1,
            "per_page": 100,
        }
        self.params_product = {
            "region_id": self.region_id,
            "page": 1,
            "per_page": 100,
        }
        self.json = '{"filters":[]}'
        self.total_count = 0
        self.move_on = True
        self.url_categories = "https://price.ru/v3/categories/{}/list"
        self.url_product = "https://price.ru/v3/models/{}/offers"
        self.url_redirect_to_shop = "https://price.ru/{}"
        self.df_product = pd.DataFrame()

    async def added_to_xml(self, xml_dict: dict, product: dict, offer: dict, url: str):
        try:
            if not any([category_id['@id'] == product['category']['id'] for category_id in
                        xml_dict['yml_catalog']['shop']['categories'].values()]):
                xml_dict['yml_catalog']['shop']['categories'].append(
                    {
                        "category": {
                            f"@id": product['category']['id'],
                            "#text": product['category']['name']
                        }
                    }
                )
            if not any([product_name['name'] == product['name'] for product_name in
                        xml_dict['yml_catalog']['shop']['offers'].values()]):
                xml_dict['yml_catalog']['shop']['offers'].append(
                    {
                        "offer": {
                            "@available": 'false' if offer['availability'] != 'в наличии' else 'true',
                            "categoryId": product['category']['id'],
                            "currencyId": "RUB",
                            "name": product['name'],
                            "shop_name": offer['name'],
                            "price": offer['price'],
                            "shop_url": url,
                            "url": f"https://price.ru/{product['slug']}"
                        }
                    }
                )
        except BaseException as e:
            if not any([category_id['@id'] == product['category']['id'] for category_id in
                        xml_dict['yml_catalog']['shop']['categories'].values()]):
                old_categories = xml_dict['yml_catalog']['shop']['categories']['category']
                xml_dict['yml_catalog']['shop']['categories']['category'] = []
                xml_dict['yml_catalog']['shop']['categories']['category'].append(old_categories)
                xml_dict['yml_catalog']['shop']['categories']['category'].append(
                    {
                        f"@id": product['category']['id'],
                        "#text": product['category']['name']
                    }
                )
            if not any([product_name['name'] == product['name'] for product_name in
                        xml_dict['yml_catalog']['shop']['offers'].values()]):
                old_offers = xml_dict['yml_catalog']['shop']['offers']['offer']
                xml_dict['yml_catalog']['shop']['offers']['offer'] = []
                xml_dict['yml_catalog']['shop']['offers']['offer'].append(old_offers)
                xml_dict['yml_catalog']['shop']['offers']['offer'].append(
                    {
                        "@available": 'false' if offer['availability'] != 'в наличии' else 'true',
                        "categoryId": product['category']['id'],
                        "currencyId": "RUB",
                        "name": product['name'],
                        "shop_name": offer['name'],
                        "price": offer['price'],
                        "shop_url": url,
                        "url": f"https://price.ru/{product['slug']}"
                    }
                )
        return xml_dict

    async def get_data(self, product, session: ClientSession, browser: uc.Chrome):
        async with session.post(self.url_product.format(product['id']), params=self.params_product,
                                data=self.json, headers=self.headers_categories) as response:
            response_product = await response.json()
            i = 0
            original_window = browser.current_window_handle
            wait = WebDriverWait(browser, 10, 0.1)
            browser.get(f"https://price.ru/{product['slug']}")
            wait.until(ec.presence_of_element_located((By.XPATH, '//div[@class="r-text offers"]')))
            price = browser.find_element(By.XPATH, '//div[@class="r-text offers"]')
            ActionChains(browser).move_to_element(price).click().perform()
            for offer in response_product['list']:
                wait.until(ec.presence_of_element_located((By.XPATH, '//a[@class="cpc-link p-c-price__btn"]')))
                try:
                    all_shops = browser.find_elements(By.XPATH, '//a[@class="cpc-link p-c-price__btn"]')
                    ActionChains(browser).move_to_element(all_shops[i]).click().perform()
                    i += 1
                except BaseException as e:
                    next = browser.find_element(By.XPATH, '//li[@class="element-wrap control next"]')
                    ActionChains(browser).move_to_element(next).click().perform()
                    wait.until(ec.presence_of_element_located((By.XPATH, '//a[@class="cpc-link p-c-price__btn"]')))
                    i = 0
                    all_shops = browser.find_elements(By.XPATH, '//a[@class="cpc-link p-c-price__btn"]')
                    ActionChains(browser).move_to_element(all_shops[i]).click().perform()
                wait.until(ec.number_of_windows_to_be(2))
                for window_handle in browser.window_handles:
                    if window_handle != original_window:
                        browser.switch_to.window(window_handle)
                        url = browser.current_url
                browser.close()
                browser.switch_to.window(original_window)
                try:
                    async with aiofiles.open(f"xmls/{offer['shop_info']['name'].replace(' ', '_')}.xml",
                                             'r') as file:
                        my_xml_shop_old = xmltodict.parse(await file.read())
                        my_xml_shop = await self.added_to_xml(my_xml_shop_old, product, offer, url)
                except (XMLSyntaxError, FileNotFoundError, ExpatError) as e:
                    my_xml_shop = {
                        "yml_catalog": {
                            '@date': datetime.date.today(),
                            "shop": {
                                "name": offer['shop_info']['name'],
                                "url": offer['shop_info']['site'],
                                "currencies": {
                                    "currency": {
                                        "@id": "RUR", "@rate": 1
                                    }
                                },
                                "categories": {
                                    'category': [
                                        {
                                            f"@id": product['category']['id'],
                                            "#text": product['category']['name']
                                        },
                                    ]
                                },
                                "offers": {
                                    "offer": [
                                        {
                                            "@available": 'false' if offer[
                                                                         'availability'] != 'в наличии' else 'true',
                                            "categoryId": product['category']['id'],
                                            "currencyId": "RUB",
                                            "name": product['name'],
                                            "shop_name": offer['name'],
                                            "price": offer['price'],
                                            "shop_url": url,
                                            "url": f"https://price.ru/{product['slug']}"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                async with aiofiles.open(f"xmls/{offer['shop_info']['name'].replace(' ', '_')}.xml",
                                         'w') as file:
                    my_xml = xmltodict.unparse(my_xml_shop, pretty=True)
                    await file.write(my_xml)
                print(f"Добавлен - {product['name']} в магазине {offer['shop_info']['name']}")
        # browser.close()

    async def start_category(self):
        async with ClientSession(cookies=self.cookie) as session:
            options = uc.ChromeOptions()
            options.add_argument("--start-maximized")
            with uc.Chrome(options=options) as browser:
                for category in self.all_categories:
                    self.move_on = True
                    while self.move_on:
                        async with session.post(self.url_categories.format(category), params=self.params_category,
                                                data=self.json, headers=self.headers_categories) as response:
                            response = await response.json()
                            self.total_count = response['total']
                            for product in response['list']:
                                await self.get_data(product, session, browser)
                            if self.params_category['page'] * 100 < self.total_count:
                                self.params_category['page'] += 1
                            else:
                                self.move_on = False


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    parser = Parser()
    loop.run_until_complete(parser.start_category())
    loop.close()
