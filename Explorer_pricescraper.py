#!/usr/bin/env python

"""Hurtigruten API scraper.

This module has been created to get price data from Hurtigrutens Explorer cruises.

If run directly from UNIX: Explorer_pricescraper.py
If run from Windows: python3 Explorer_pricescraper.py

Todo:
    * Change write-to-csv to write directly to database

Notes:
    * If body says sold out it will write the sold out row, but it will also write a row with
      data if it contains data
    * Seems to work well regarding normal tours, but tours with calendar on web page
      needs more testing

"""

import csv
import requests
import pytz
import sqlite3
from parsel import Selector
from datetime import datetime

class HurtigrutenAPI(object):
    """Hurtigruten API scraper.
    Contains all the methods used for scraping the API

    No parameters.

    """
    def __init__(self):
        self.main = 'https://www.hurtigruten.no'
        self.markets = ['NO', 'FR', 'DE', 'UK', 'US']
        self.voyagetype = 'EXPLORER'

    def travelfilter_response(self):
        """Get response from main filter page such that we can loop through all tours"""
        self.travelfilter_url = 'https://www.hurtigruten.com/api/travelfilter?destinationId=&departureMonthYear=&shipId=&marketCode=NO&languageCode=no'
        self.travel_response = requests.get(self.travelfilter_url).json()
        return self.travel_response

    def initial_response(self, i):
        """Extract tour code(s) from tour in question
 
        Get's an url to each separate cruise from the function travelfilter_response(),
        which is concatenated with the main url link in the __init__,
        then gets the html response from that link from which we can extract tour code(s).

        """
        self.i = i
        self.intermediate_url = self.travel_response['voyages'][self.i]['voyageUrl']
        self.img_url = self.main + self.travel_response['voyages'][self.i]['image']
        self.map_url = self.main + self.travel_response['voyages'][self.i]['map']
        self.initial_url = self.main + self.intermediate_url
        self.init_response = requests.get(self.initial_url)
        self.sel = Selector(self.init_response.text)
        print(self.init_response)
        return self.sel

    def travel_codes(self):
        """Extracts travel codes from the body in the initial response.

        Returns: list

        """
        self.codes = self.sel.xpath('//script[contains(.,"products")]').re_first('id: \"([^\"]+)\"')
        self.codes = self.codes.split(',')
        print(self.codes)
        return self.codes

    def gateways_response(self, code):
        """U
        Gateways request: We send a payload with the travel code(s) extracted from the initial
        response to get the cruise dates.

        """
        self.code = code
        self.gateways_url = "https://shadowprodapi.hurtigruten.com/api//travelsuggestions/gateways"
        self.gateways_payload = '{{"travelSuggestionCodes":["{}"],"marketCode":"NO","languageCode":"no"}}'
        self.headers = {'content-type': "application/json"}
        self.gate_response = requests.post(self.gateways_url, data=self.gateways_payload.format(self.code), headers=self.headers).json()
        self.date = self.gate_response["gateways"][0]["firstAvailableDate"].split('T')[0]
        return self.gate_response, self.date

    def grouped_response(self, code, m):
        """Sends travel code and date to intermediate API.

        Use the date from the gateways_response() and code from travel_codes()
        in payload to recieve a response containing voyage ID and quote ID which
        is needed for the final API call to get available price.

        """
        self.code = code
        self.marketcode = m
        #self.grouped_payload = '{{"packageCode":{},"searchFromDateTime":"{}","cabins":[{{"passengers":[{{"ageCategory":"ADULT","guestType":"REGULAR"}},{{"ageCategory":"ADULT","guestType":"REGULAR"}}]}}],"currencyCode":"NOK","marketCode":"'self.marketcode'","languageCode":"en","quoteId":null,"bookingSourceCode":"TDL_B2C_NO"}}'
        self.grouped_payload = '{{"packageCode":"{}","searchFromDateTime":"{}","cabins":[{{"passengers":[{{"ageCategory":"ADULT","guestType":"REGULAR"}},{{"ageCategory":"ADULT","guestType":"REGULAR"}}]}}],"currencyCode":"NOK","marketCode":"' + str(self.marketcode) +'","languageCode":"no","quoteId":null,"bookingSourceCode":"TDL_B2C_NO"}}'
        self.grouped_url = 'https://shadowprodapi.hurtigruten.com/api/availability/travelsuggestions/grouped'
        self.group_response = requests.post(self.grouped_url, data=self.grouped_payload.format(self.code, self.date), headers=self.headers).json()
        self.quote_id = self.group_response["quoteId"]
        print(self.quote_id)
        return self.group_response, self.quote_id

    def get_quote(self, item):
        """Final response:

        """
        self.item = item
        self.voyage_date = self.item["date"].split('T')[0]
        self.voyage_id = self.item["voyages"][0]["voyageId"]
        self.url = 'https://shadowprodapi.hurtigruten.com/api/quotes/{}/packagePrices?date={}&voyageId={}'
        self.quote = requests.get(self.url.format(self.quote_id, self.voyage_date, self.voyage_id)).json()

    def sold_out_check(self, i):
        """Checks if text contains sold out.

        If text box in site body contains "sold out", it invokes
        sold_out_writer() and writes info to csv

        """
        self.i = i
        self.sold_out_in_body = self.sel.xpath("//div[@class='top-image-promotion']/text()").extract()
        self.sold_out_in_body = [x.lower() for x in self.sold_out_in_body]
        self.sold_out = False
        if any("sold out" in s for s in self.sold_out_in_body):
            self.sold_out = True
            #self.sold_out_writer(i)

    def initiate_writer(self, name):
        """Initialize csv writer with header"""
        self.name = name
        self.writer = csv.writer(self.name, delimiter=',', lineterminator='\n')
        self.writer.writerow(('Iteration', 'Timestamp', 'Ship', 'Tourcode',
                              'Tourname', 'Destination', 'Tourstart', 'PolarInside',
                              'PolarOutside', 'ArcticSuperior', 'ExpeditionSuite'))

    def sold_out_writer(self, i, item=None):
        """Writes sold out data"""
        self.i = i
        self.item = item
        if item:
            self.sold_out_date = self.item["date"].split('T')[0] # Can get date of tour which is sold out using voyage_date
        else:
            self.sold_out_date = 0 # If we can't use voyage_date, put 0 instead, because firstAvailableDate (self.date) is not necessarily the one sold out.

        self.row_soldout = (self.i,
                            datetime.datetime.now(pytz.timezone("Europe/Tallinn")).strftime("%Y-%m-%d"),
                            self.travel_response["voyages"][self.i]["ships"][0]["id"],
                            #self.codes, 
                            self.travel_response["voyages"][self.i]["name"],
                            self.travel_response["voyages"][self.i]["destination"]["name"],
                            self.sold_out_date, 0, 0, 0, 0)

        self.writer.writerow(self.row_soldout)

    def quote_writer(self, i, code, item):
        """Extracts price information from quote and writes it to csv"""
        self.i = i
        self.code = code
        self.item = item
        self.row = (self.i,
                    datetime.datetime.now(pytz.timezone("Europe/Tallinn")).strftime("%Y-%m-%d"),
                    self.item["voyages"][0]["ship"]["shipCode"],
                    #self.code,
                    self.travel_response["voyages"][self.i]["name"],
                    self.travel_response["voyages"][self.i]["destination"]["name"],
                    self.voyage_date,
                    self.quote["categoryPrices"][0]['price']['amount'],
                    self.quote["categoryPrices"][1]['price']['amount'],
                    self.quote["categoryPrices"][2]['price']['amount'],
                    self.quote["categoryPrices"][3]['price']['amount'])
        self.writer.writerow(self.row)
        print(self.row)

    def startdate(self, start=None):
        if start is None:
            self.start = datetime.now()
        else:
            self.start = datetime.strptime(start, '%Y-%m-%d')
        self.curDate = '{:%Y-%m-%d}'.format(datetime.date(self.start))
        self.curYear = int(self.curDate[:4])
        self.curMonth = int(self.curDate[5:7])
        self.reqDate = datetime(self.curYear,self.curMonth,1)
        return self.reqDate

    def parse_and_store(self, price):
        self.price = price

        self.cr.execute('INSERT OR IGNORE INTO dimReportDate(ReportDate) VALUES (?);', (self.curDate,))
        self.cr.execute('SELECT id FROM dimReportDate WHERE ReportDate = ?;', (self.curDate, ))
        self.rDate_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimShips(ShipCode) VALUES (?);', (self.item["voyages"][0]["ship"]["shipCode"], ))
        self.cr.execute('SELECT id FROM dimShips WHERE shipCode = ?;', (self.item["voyages"][0]["ship"]["shipCode"], ))
        self.ship_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimCabinCategory(Category) VALUES (?);', (self.price['code'], ))
        self.cr.execute('SELECT id FROM dimCabinCategory WHERE Category = ?;', (self.price['code'], ))
        self.cat_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimVoyage(VoyageType) VALUES (?);', (self.voyagetype, ))
        self.cr.execute('SELECT id FROM dimVoyage WHERE VoyageType = ?;', (self.voyagetype, ))
        self.type_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimDepartureDate(DepartureDate) VALUES (?);', (self.voyage_date, ))
        self.cr.execute('SELECT id FROM dimDepartureDate WHERE DepartureDate = ?;', (self.voyage_date, ))
        self.dep_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimTour(TourName, TourImg, TourMap) VALUES (?, ?, ?);', (self.travel_response["voyages"][self.i]["name"], self.img_url, self.map_url, ))
        self.cr.execute('SELECT id FROM dimTour WHERE TourName = ?;', (self.travel_response["voyages"][self.i]["name"], ))
        self.tour_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimDestination(Destination) VALUES (?);', (self.travel_response["voyages"][self.i]["destination"]["name"], ))
        self.cr.execute('SELECT id FROM dimDestination WHERE Destination = ?;', (self.travel_response["voyages"][self.i]["destination"]["name"], ))
        self.dest_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO dimSourceMarket(SourceMarket) VALUES (?);', (self.marketcode, ))
        self.cr.execute('SELECT id FROM dimSourceMarket WHERE SourceMarket = ?;', (self.marketcode, ))
        self.source_id = self.cr.fetchone()[0]

        self.cr.execute('INSERT OR IGNORE INTO Data_Explorer(rDate_id, ship_id, cat_id, type_id, dep_id, tour_id, dest_id, source_id, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);', (self.rDate_id, self.ship_id, self.cat_id, self.type_id, self.dep_id, self.tour_id, self.dest_id, self.source_id, self.price['price']['amount']))
        print(self.rDate_id, self.ship_id, self.cat_id, self.type_id, self.dep_id, self.tour_id, self.dest_id, self.source_id, self.price['price']['amount'])
        self.connection.commit()

    def sql3_storage(self, location='C:\\Users\\H520139\\.spyder-py3\\HRG\\DBs\\', dbname='Pricing.db'):
        self.location = location
        self.dbname = dbname
        self.connection = sqlite3.connect(location+dbname)
        self.cr = self.connection.cursor()
        try:
            self.cr.executescript('''
    CREATE TABLE IF NOT EXISTS dimReportDate (id integer PRIMARY KEY AUTOINCREMENT, ReportDate TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimShips (id integer PRIMARY KEY AUTOINCREMENT, ShipCode TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimCabinCategory (id integer PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimVoyage (id integer PRIMARY KEY AUTOINCREMENT, VoyageType TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimDepartureDate (id integer PRIMARY KEY AUTOINCREMENT, DepartureDate TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimTour (id integer PRIMARY KEY AUTOINCREMENT, TourName TEXT NOT NULL UNIQUE, TourImg TEXT, TourMap TEXT);
    CREATE TABLE IF NOT EXISTS dimDestination (id integer PRIMARY KEY AUTOINCREMENT, Destination TEXT NOT NULL UNIQUE);    
    CREATE TABLE IF NOT EXISTS dimSourceMarket (id integer PRIMARY KEY AUTOINCREMENT, SourceMarket TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS Data_Explorer (
                                    rDate_id integer NOT NULL,
                                    ship_id integer NOT NULL,
                                    cat_id integer NOT NULL,
                                    type_id integer NOT NULL,
                                    dep_id integer NOT NULL,
                                    tour_id integer NO NULL,
                                    dest_id integer NOT NULL,
                                    source_id integer NOT NULL,
                                    price real,
                                    FOREIGN KEY (rDate_id) references dimReportDate(id),
                                    FOREIGN KEY (ship_id) references dimShips(id),
                                    FOREIGN KEY (cat_id) references dimCabinCategory(id),
                                    FOREIGN KEY (type_id) references dimVoyage(id),
                                    FOREIGN KEY (dep_id) references dimDepartureDate(id),
                                    FOREIGN KEY (tour_id) references dimTour(id),
                                    FOREIGN KEY (dest_id) references dimDestination(id),
                                    FOREIGN KEY (source_id) references dimSourceMarket(id),
                                    PRIMARY KEY (rDate_id, ship_id, cat_id, type_id, dep_id, tour_id, dest_id, source_id))
                                    ''')
            self.connection.commit()
            return self.connection, self.cr
        except:
            pass


    def scraper(self):
        """Scrapes the data from the API"""
#        with open('OOP_Price-{}.csv'.format(datetime.datetime.today().strftime("%Y-%m-%d")), 'w') as f:
#            self.initiate_writer(f)
        self.sql3_storage()
        self.startdate()
        self.travelfilter_response()
        for i in range(len(self.travel_response['voyages'])): # number of tours from travelfilter_response()
                self.initial_response(i)
                try:
                    self.travel_codes()
                    self.sold_out_check(i)
                    for code in self.codes:  # from travel_codes()
                        for m in self.markets:
                            try:
                                self.gateways_response(code)
                                self.grouped_response(code,m)
                                for item in self.group_response["calendar"]: # loops through all dates from grouped_response() on each code
                                    if item["voyages"] is None:
                                        if self.sold_out:
                                            continue
                                    self.get_quote(item)
                                    for q in self.quote['categoryPrices']:
                                        try:
                                            self.parse_and_store(q)
                                        except Exception:
                                            continue
#                                    self.quote_writer(i, code, item)
                            except Exception:
                                continue
                except Exception:
                    continue
        self.connection.close()
if __name__ == '__main__':
    SCRAPER = HurtigrutenAPI()
    SCRAPER.scraper()