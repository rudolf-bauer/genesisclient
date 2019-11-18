class GenesisClient(object):

    def __init__(self, site, username=None, password=None):
        self.sites = {
            'DESTATIS': {
                'webservice_url': 'https://www-genesis.destatis.de/genesisWS'
            },
            'LDNRW': {
                'webservice_url': 'https://www.landesdatenbank.nrw.de/ldbnrwws'
            },
            'REGIONAL': {
                'webservice_url': 'https://www.regionalstatistik.de/genesisws'
            },
            'BAYERN': {
                'webservice_url': 'https://www.statistikdaten.bayern.de/genesisWS'
            },
            #'SACHSEN': {
            #    'webservice_url': 'http://www.statistik.sachsen.de/...'
            #},
            'BILDUNG': {
                'webservice_url': 'https://www.bildungsmonitoring.de/bildungws'
            }
        }
        self.endpoints = {
            'TestService': '/services/TestService?wsdl',
            #'RechercheService': '/services/RechercheService?wsdl',
            'RechercheService_2010': '/services/RechercheService_2010?wsdl',
            'DownloadService': '/services/DownloadService?wsdl',
            #'DownloadService_2010': '/services/DownloadService_2010?wsdl',
            #'ExportService': '/services/ExportService?wsdl',
            #'ExportService_2010': '/services/ExportService_2010?wsdl',
            #'GEOMISService': '/services/GEOMISService?wsdl',
            #'NutzerService': '/services/NutzerService?wsdl',
            #'Version': '/services/Version?wsdl',
        }
        if site is None:
            raise Exception('No site given')
        if site not in self.sites:
            sitekeys = ", ".join(sorted(self.sites.keys()))
            raise ValueError('Site not known. Use one of %s.' % sitekeys)
        self.site = site
        self.username = None
        self.password = None
        self.service_clients = {}
        if username is not None:
            self.username = username
        if password is not None:
            self.password = password

    def init_service_client(self, name):
        """
        Initializes a client for a certain endpoint, identified by name,
        returns it and stores it internally for later re-use.
        """
        if name not in self.service_clients:
            url = (self.sites[self.site]['webservice_url']
                  + self.endpoints[name])
            self.service_clients[name] = suds.client.Client(url, retxml=True)
        return self.service_clients[name]

    def test_service(self):
        """
        Calls functions for test purposes.
        whoami and Exception handling.
        """
        client = self.init_service_client('TestService')
        client.service.whoami()
        try:
            client.service.exception()
        except suds.WebFault:
            pass

    def search(self, searchterm='*:*', limit=500, category='alle'):
        """
        Allows search for a resource (property, table, statistic, ...)
        by keyword.
        searchterm: The keyword you are trying to find in meta data
        category (defaults to any category):
            "Tabelle" for data tables,
            "Zeitreihe" for time series,
            "Datenquader", "Merkmal", "Statistik"
        """
        client = self.init_service_client('RechercheService_2010')
        #print client
        params = dict(luceneString=searchterm,
                      kennung=self.username,
                      passwort=self.password,
                      listenLaenge=str(limit),
                      sprache='de',
                      kategorie=category
                      )
        result = client.service.Recherche(**params)
        root = etree.fromstring(result)
        out = {
            'meta': {},
            'results': []
        }
        for element in root.iter("trefferUebersicht"):
            otype = element.find("objektTyp")
            num = element.find("trefferAnzahl")
            if otype is not None and num is not None:
                out['meta'][otype.text] = int(num.text)
        for element in root.iter("trefferListe"):

            code = element.find('EVAS')
            description = element.find('kurztext')
            name = element.find('name')
            if name is not None:
                name = name.text
            otype = element.find("objektTyp")
            if otype is not None:
                otype = otype.text
            if code is not None:
                out['results'].append({
                    'id': code.text,
                    'type': otype,
                    'name': name,
                    'description': clean(description.text)
                })
        return out

    def terms(self, filter='*', limit=20):
        """
        Gives access to all terms which can be used for search. Example:
        filter='bev*' will return only terms starting with "bev". Can be used
        to implement search term auto-completion.
        """
        client = self.init_service_client('RechercheService_2010')
        #print client
        params = dict(kennung=self.username,
                      passwort=self.password,
                      filter=filter,
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.BegriffeKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("begriffeKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def properties(self, filter='*', criteria='Code', type="alle", limit=500):
        """
        Access to the properties catalogue (data attributes).
        filter:
            Selection string, supporting asterisk notation.
        criteria:
            Can be "Code" or "Inhalt", defines on what the filter parameter
            matches and what the result is sorted by.
        type:
            Type of the properties to be matched. Supported are:
            "klassifizierend"
            "insgesamt"
            "räumlich"
            "sachlich"
            "wert"
            "zeitlich"
            "zeitidentifizierend"
            "alle" (default)
        area
        """
        client = self.init_service_client('RechercheService_2010')
        #print client
        params = dict(kennung=self.username,
                      passwort=self.password,
                      filter=filter,
                      kriterium=criteria,
                      typ=type,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.MerkmalsKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("merkmalsKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def property_occurrences(self, property_code, selection='*',
                             criteria="Code", limit=500):
        """
        Retrieve occurences of properties. Use property_code to indicate the
        property. You can further narrow down the selection of occurences
        using the selection parameter which supports asterisk notation
        (e.g. selection='hs18*').
        """
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=property_code,
                      auswahl=selection,
                      kriterium=criteria,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.MerkmalAuspraegungenKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("merkmalAuspraegungenKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def property_data(self, property_code='*', selection='*',
                             criteria="Code", limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=property_code,
                      auswahl=selection,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.MerkmalDatenKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("merkmalDatenKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            beschriftungstext = element.find('beschriftungstext')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text),
                    'longdescription': clean(beschriftungstext.text)
                })
        return out

    def property_statistics(self, property_code='*', selection='*',
                             criteria="Code", limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=property_code,
                      auswahl=selection,
                      kriterium=criteria,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.MerkmalStatistikenKatalog(**params)
        print result

    def property_tables(self, property_code='*', selection='*', limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=property_code,
                      auswahl=selection,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.MerkmalTabellenKatalog(**params)
        print result

    def statistics(self, filter='*', criteria='Code', limit=500):
        """
        Load information on statistics
        """
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      filter=filter,
                      kriterium=criteria,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.StatistikKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("statistikKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def statistic_data(self, statistic_code='*', selection='*', limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=statistic_code,
                      auswahl=selection,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.StatistikDatenKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("statistikDatenKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            beschriftungstext = element.find('beschriftungstext')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text),
                    'longdescription': clean(beschriftungstext.text)
                })
        return out

    def statistic_properties(self, statistic_code='*', criteria='Code', selection='*', limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=statistic_code,
                      auswahl=selection,
                      kriterium=criteria,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.StatistikMerkmaleKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("statistikMerkmaleKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def statistic_tables(self, statistic_code='*', selection='*', limit=500):
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=statistic_code,
                      auswahl=selection,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.StatistikTabellenKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("statistikTabellenKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def tables(self, filter='*', limit=500):
        """
        Retrieve information on tables
        """
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      filter=filter,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de')
        result = client.service.TabellenKatalog(**params)
        root = etree.fromstring(result)
        out = []
        for element in root.iter("tabellenKatalogEintraege"):
            code = element.find('code')
            inhalt = element.find('inhalt')
            if code is not None:
                out.append({
                    'id': code.text,
                    'description': clean(inhalt.text)
                })
        return out

    def catalogue(self, filter='*', limit=500):
        """
        Retrieve metadata on data offerings. Can be filtered by code, e.g.
        filter='11111*' delivers all entries with codes starting with '11111'.
        """
        client = self.init_service_client('RechercheService_2010')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      filter=filter,
                      bereich='Alle',
                      listenLaenge=str(limit),
                      sprache='de'
                      )
        result = client.service.DatenKatalog(**params)
        return result

    def table_export(self, table_code,
            regionalschluessel='',
            format='csv'):
        """
        Return data for a given table
        """
        client = self.init_service_client('DownloadService')
        params = dict(kennung=self.username,
                      passwort=self.password,
                      name=table_code,
                      bereich='Alle',
                      format=format,
                      komprimierung=False,
                      startjahr='1900',
                      endjahr='2100',
                      zeitscheiben='',
                      regionalschluessel=regionalschluessel,
                      sachmerkmal='',
                      sachschluessel='',
                      sprache='de',
                      )
        result = None
        if format == 'xls':
            del params['format']
            result = client.service.ExcelDownload(**params)
            # Really nasty way to treat a multipart message...
            # (room for improvement)
            parts = result.split("\r\n")
            for i in range(0, 12):
                parts.pop(0)
            parts.pop()
            parts.pop()
            return "\r\n".join(parts)
        else:
            result = client.service.TabellenDownload(**params)
            parts = result.split(result.split("\r\n")[1])
            data = parts[2].split("\r\n\r\n", 1)[-1]
            #data = unicode(data.decode('latin-1'))
            #data = unicode(data.decode('utf-8'))
            return data



def clean(s):
    """Clean up a string"""
    if s is None:
        return None
    s = s.replace("\n", " ")
    s = s.replace("  ", " ")
    s = s.strip()
    return s