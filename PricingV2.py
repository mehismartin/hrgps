import requests
import json
import sqlite3
from datetime import datetime

class HRGCoastalPScraper():
    
    def __init__(self):
        self.main='https://www.hurtigruten.com'
        self.url='https://api.hurtigruten.com:443/api/Availability'
        self.headers = {'content-type': 'application/json'}
        #How far will data be gathered (y-m-d):
        self.endDate = datetime(2020, 6, 2)
        self.DPs = ['BGO', 'KKN']
        self.APs = ['BGO', 'KKN', 'TRD']
        self.markets = ['NO', 'FR', 'DE', 'UK', 'US']
        self.fails = []
    
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

    def month_increment(self, datevalue):
        self.datevalue = datevalue
        self.datedate = '{:%Y-%m-%d}'.format(datetime.date(self.datevalue))
        self.dateyear = int(self.datedate[:4])
        self.datemonth = int(self.datedate[5:7])
        if self.datemonth == 12:
            self.datemonth = 0
            self.dateyear = self.dateyear + 1
        self.datemonth = self.datemonth + 1
        self.reqDate = datetime(self.dateyear, self.datemonth, 1)
        return self.reqDate

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
    CREATE TABLE IF NOT EXISTS dimDeparturePorts (id integer PRIMARY KEY AUTOINCREMENT, PortName TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimArrivalPorts (id integer PRIMARY KEY AUTOINCREMENT, PortName TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS dimSourceMarket (id integer PRIMARY KEY AUTOINCREMENT, SourceMarket TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS Data (
                                    rDate_id integer NOT NULL,
                                    ship_id integer NOT NULL,
                                    cat_id integer NOT NULL,
                                    type_id integer NOT NULL,
                                    dep_id integer NOT NULL,
                                    dport_id integer NO NULL,
                                    aport_id integer NOT NULL,
                                    source_id integer NOT NULL,
                                    occupancy real,
                                    viaKKN real,
                                    price real,
                                    FOREIGN KEY (rDate_id) references dimReportDate(id),
                                    FOREIGN KEY (ship_id) references dimShips(id),
                                    FOREIGN KEY (cat_id) references dimCabinCategory(id),
                                    FOREIGN KEY (type_id) references dimVoyage(id),
                                    FOREIGN KEY (dep_id) references dimDepartureDate(id),
                                    FOREIGN KEY (dport_id) references dimDeparturePorts(id),
                                    FOREIGN KEY (aport_id) references dimArrivalPorts(id),
                                    FOREIGN KEY (source_id) references dimSourceMarket(id),
                                    PRIMARY KEY (rDate_id, ship_id, cat_id, type_id, dep_id, dport_id, aport_id, source_id))
                                    ''')
            self.connection.commit()
            return self.connection, self.cr
        except:
            pass

    def query(self, fromPort, toPort, market, bookingSource="TDL_B2C_NO", viaKKN=True):
        self.fromPort = fromPort
        self.toPort = toPort
        self.viaKKN = viaKKN
        self.bookingSource = bookingSource
        self.market = market
        self.payload = {"currencyCode": "NOK","quoteId": "","fromPort": str(self.fromPort),"toPort": str(self.toPort),"isViaKirkenes": self.viaKKN,"searchFromDateTime": str(self.reqDate),"cabins": [{"passengers": [{"ageCategory": "ADULT","guestType": "REGULAR"},{"ageCategory": "ADULT","guestType": "REGULAR"}]}],"bookingSourceCode": str(self.bookingSource),"marketCode": str(self.market),"languageCode": "en"}
        self.response_data = requests.post(self.url,data = json.dumps(self.payload), headers= self.headers)
        self.json_results = self.response_data.json()
        return self.json_results

    def parse_and_store(self):
        self.occupancy = len(self.payload['cabins'][0]['passengers'])
        for date in self.json_results['calendar']:
            if date['voyages'] == None:
                continue
            for sail in date['voyages']:
                if sail['categoryPrices'] == None:
                    continue
                for categ in sail['categoryPrices']:
                    if categ['available'] == True:
                        self.cr.execute('INSERT OR IGNORE INTO dimReportDate(ReportDate) VALUES (?);', (self.curDate,))
                        self.cr.execute('SELECT id FROM dimReportDate WHERE ReportDate = ?;', (self.curDate, ))
                        self.rDate_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimShips(ShipCode) VALUES (?);', (sail['ship']['shipCode'], ))
                        self.cr.execute('SELECT id FROM dimShips WHERE shipCode = ?;', (sail['ship']['shipCode'], ))
                        self.ship_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimCabinCategory(Category) VALUES (?);', (categ['code'], ))
                        self.cr.execute('SELECT id FROM dimCabinCategory WHERE Category = ?;', (categ['code'], ))
                        self.cat_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimVoyage(VoyageType) VALUES (?);', (sail['voyageType'], ))
                        self.cr.execute('SELECT id FROM dimVoyage WHERE VoyageType = ?;', (sail['voyageType'], ))
                        self.type_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimDepartureDate(DepartureDate) VALUES (?);', (date['date'], ))
                        self.cr.execute('SELECT id FROM dimDepartureDate WHERE DepartureDate = ?;', (date['date'], ))
                        self.dep_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimDeparturePorts(PortName) VALUES (?);', (self.fromPort, ))
                        self.cr.execute('SELECT id FROM dimDeparturePorts WHERE PortName = ?;', (self.fromPort, ))
                        self.dport_id = self.cr.fetchone()[0]
                        self.cr.execute('INSERT OR IGNORE INTO dimArrivalPorts(PortName) VALUES (?);', (self.toPort, ))
                        self.cr.execute('SELECT id FROM dimArrivalPorts WHERE PortName = ?;', (self.toPort, ))
                        self.aport_id = self.cr.fetchone()[0]                        
                        self.cr.execute('INSERT OR IGNORE INTO dimSourceMarket(SourceMarket) VALUES (?);', (self.market, ))
                        self.cr.execute('SELECT id FROM dimSourceMarket WHERE SourceMarket = ?;', (self.market, ))
                        self.source_id = self.cr.fetchone()[0]
                        
                        self.cr.execute('INSERT OR IGNORE INTO Data(rdate_id, ship_id, cat_id, type_id, dep_id, dport_id, aport_id, source_id, occupancy, viaKKN, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (self.rDate_id, self.ship_id, self.cat_id, self.type_id, self.dep_id, self.dport_id, self.aport_id, self.source_id, self.occupancy, self.viaKKN, categ['price']['amount']))
                        self.connection.commit()
#                        print ("Inserted:", date['date'], sail['ship']['shipCode'],self.fromPort, self.toPort, self.market, categ['code'], categ['price']['amount'])
                        self.json_results = None

    def scrape(self, start=None):
        self.startdate(start)
        self.sql3_storage()
        while self.reqDate < self.endDate:
            for m in self.markets:
                for dp in self.DPs:
                    for ap in self.APs:
                        if dp == 'KKN' and ap == 'KKN':
                            continue
                        if dp == 'KKN' and ap == 'TRD':
                            continue
                        try:
                            self.query(dp, ap, m)
                            self.parse_and_store()
                        except:
                            self.fails.append(str(self.reqDate) + str(dp) + str(ap) + str(m))
            self.month_increment(self.reqDate)
        self.connection.close()

if __name__ == '__main__':
    SCRAPER = HRGCoastalPScraper()
    SCRAPER.scrape()
#print(o.fails)