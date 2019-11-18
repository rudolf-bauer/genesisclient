from lxml import etree
from zeep import Client
import requests
import tempfile


class GenesisClient(object):

    sites = {
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
        #  'SACHSEN': {
        #    'webservice_url': 'http://www.statistik.sachsen.de/...'
        #  },
        'BILDUNG': {
            'webservice_url': 'https://www.bildungsmonitoring.de/bildungws'
        }
    }

    endpoints = {
        'TestService': '/services/TestService?wsdl',
        #  'RechercheService': '/services/RechercheService?wsdl',
        'RechercheService_2010': '/services/RechercheService_2010?wsdl',
        'DownloadService': '/services/DownloadService?wsdl',
        #  'DownloadService_2010': '/services/DownloadService_2010?wsdl',
        #  'ExportService': '/services/ExportService?wsdl',
        #  'ExportService_2010': '/services/ExportService_2010?wsdl',
        #  'GEOMISService': '/services/GEOMISService?wsdl',
        #  'NutzerService': '/services/NutzerService?wsdl',
        #  'Version': '/services/Version?wsdl',
    }

    def __init__(self, site, username=None, password=None, language='de'):
        """
        A genesis client allows to download metadata and table data (csv/xls) from a Genesis-website.
        :param site: str, website to download from, has to be one of: `GenesisClient.sites`.
        :param username: str, username of the page.
        :param password: str, password of the page.
        :param language: str, language identifier, currently only `de` was tested.
        """

        # Ensure valid site
        if site is None:
            raise Exception('No site given')
        if site not in self.sites:
            site_keys = ", ".join(sorted(self.sites.keys()))
            raise ValueError('Site not known. Use one of %s.' % site_keys)

        self.site = site
        self.service_clients = {}
        self.base_params = {
            'sprache': language,
            'kennung': '',
            'passwort': '',
        }
        if username is not None:
            self.base_params['kennung'] = username
        if password is not None:
            self.base_params['passwort'] = password

    def download_excel(self, table_code, regionalschluessel='', start_year=1900, end_year=2100, compress_result=False):
        """
        Download an Excel file as binary.
        :param table_code: str, the code of the table to download.
        :param regionalschluessel: str, regional key to subset.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :param compress_result: bool, if `True`, the data will be compressed.
        :return: Binary array, contains the Excel file.
        """
        client = self._init_service_client('DownloadService')
        params = self._build_download_params(table_code, regionalschluessel, start_year, end_year, compress_result)
        #  print(params)
        result = client.service.ExcelDownload(**params)
        return result.attachments[0].content

    def download_csv(self, table_code, regionalschluessel='', start_year=1900, end_year=2100, compress_result=False):
        """
        Download a CSV file as UTF-8 string.
        :param table_code: str, the code of the table to download.
        :param regionalschluessel: str, regional key to subset.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :param compress_result: bool, if `True`, the data will be compressed.
        :return: str, UTF-8, contains the csv string.
        """
        client = self._init_service_client('DownloadService')
        params = self._build_download_params(table_code, regionalschluessel, start_year, end_year, compress_result)
        params.update({'format': 'csv'})
        #  print(params)
        result = client.service.TabellenDownload(**params)
        return result.attachments[0].content.decode("utf-8")

    def search(self, search_term='*:*', category='alle', limit=500):
        """
        Allows search for a resource (property, table, statistic, ...)
        by keyword.
        searchterm: The keyword you are trying to find in meta data
        category (defaults to any category):
            "Tabelle" for data tables,
            "Zeitreihe" for time series,
            "Datenquader", "Merkmal", "Statistik"
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'luceneString': search_term,
            'kategorie': category,
            'listenLaenge': str(limit)
        })
        result = client.service.Recherche(**params)
        root = etree.fromstring(result)
        out = {
            'meta': {},
            'results': []
        }
        for element in root.iter("trefferUebersicht"):
            object_type = element.find("objektTyp")
            num = element.find("trefferAnzahl")
            if object_type is not None and num is not None:
                out['meta'][object_type.text] = int(num.text)

        for element in root.iter("trefferListe"):
            code = element.find('EVAS')
            description = element.find('kurztext')
            name = element.find('name')
            if name is not None:
                name = name.text
            object_type = element.find("objektTyp")
            if object_type is not None:
                object_type = object_type.text
            if code is not None:
                out['results'].append({
                    'id': code.text,
                    'type': object_type,
                    'name': name,
                    'description': self._clean(description.text)
                })
        return out

    def terms(self, filter='*', limit=20):
        """
        Gives access to all terms which can be used for search. Example:
        filter='bev*' will return only terms starting with "bev". Can be used
        to implement search term auto-completion.
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'filter': filter,
            'listenLaenge': str(limit)
        })

        result = client.service.BegriffeKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'begriffeKatalogEintraege')

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
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'filter': filter,
            'kriterium': criteria,
            'typ': type,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.MerkmalsKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'merkmalsKatalogEintraege')

    def property_occurrences(self, property_code, selection='*', criteria="Code", limit=500):
        """
        Retrieve occurrences of properties. Use property_code to indicate the
        property. You can further narrow down the selection of occurences
        using the selection parameter which supports asterisk notation
        (e.g. selection='hs18*').
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': property_code,
            'auswahl': selection,
            'kriterium': criteria,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.MerkmalAuspraegungenKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'merkmalAuspraegungenKatalogEintraege')

    def property_data(self, property_code='*', selection='*', criteria="Code", limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': property_code,
            'auswahl': selection,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.MerkmalDatenKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'merkmalDatenKatalogEintraege', parse_long_description=True)

    def property_statistics(self, property_code='*', selection='*', criteria="Code", limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': property_code,
            'auswahl': selection,
            'kriterium': criteria,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.MerkmalStatistikenKatalog(**params)
        print(result)

    def property_tables(self, property_code='*', selection='*', limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': property_code,
            'auswahl': selection,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.MerkmalTabellenKatalog(**params)
        print(result)

    def statistics(self, filter='*', criteria='Code', limit=500):
        """
        Load information on statistics
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'filter': filter,
            'kriterium': criteria,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.StatistikKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'statistikKatalogEintraege')

    def statistic_data(self, statistic_code='*', selection='*', limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': statistic_code,
            'auswahl': selection,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.StatistikDatenKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'statistikDatenKatalogEintraege', parse_long_description=True)

    def statistic_properties(self, statistic_code='*', criteria='Code', selection='*', limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': statistic_code,
            'auswahl': selection,
            'kriterium': criteria,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.StatistikMerkmaleKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'statistikMerkmaleKatalogEintraege')

    def statistic_tables(self, statistic_code='*', selection='*', limit=500):
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'name': statistic_code,
            'auswahl': selection,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.StatistikTabellenKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'statistikTabellenKatalogEintraege')

    def tables(self, filter='*', limit=500):
        """
        Retrieve information on tables
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'filter': filter,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.TabellenKatalog(**params)
        root = etree.fromstring(result)
        return self._parse_elements(root, 'tabellenKatalogEintraege')

    def catalogue(self, filter='*', limit=500):
        """
        Retrieve metadata on data offerings. Can be filtered by code, e.g.
        filter='11111*' delivers all entries with codes starting with '11111'.
        """
        client = self._init_service_client('RechercheService_2010')
        params = self._clone_and_update_base_params({
            'filter': filter,
            'bereich': 'Alle',
            'listenLaenge': str(limit)
        })
        result = client.service.DatenKatalog(**params)
        return result

    def test_service(self):
        """
        Calls functions for test purposes.
        Tests `whoami` and exception handling.
        """
        client = self._init_service_client('TestService')
        client.service.whoami()

        #  try:
        #    client.service.exception()
        #  except suds.WebFault:
        #    pass

    def _init_service_client(self, name):
        """
        Initializes a client for a certain endpoint, identified by name,
        returns it and stores it internally for later re-use.
        """
        if name not in self.service_clients:
            url = (self.sites[self.site]['webservice_url'] + self.endpoints[name])
            
            # Workaround: `zeep` does not support the `apachesoap:DataHandler` datatype
            # -> replace by `xsd:base64Binary` (in WSDL file)
            response = requests.get(url)
            wsdl_string = response.text.replace('apachesoap:DataHandler', 'xsd:base64Binary')
            with tempfile.NamedTemporaryFile() as tmp:
                tmp.write(wsdl_string.encode("utf-8"))
                self.service_clients[name] = Client(wsdl=tmp.name)
        return self.service_clients[name]

    def _clone_and_update_base_params(self, update_dict):
        params = self.base_params.copy()
        params.update(update_dict)
        return params

    def _build_download_params(self, table_code, regionalschluessel, start_year, end_year, compress_result):
        return self._clone_and_update_base_params({
            'name': table_code,
            'bereich': 'Alle',
            'komprimierung': compress_result,
            'startjahr': str(start_year),
            'endjahr': str(end_year),
            'zeitscheiben': '',
            'regionalschluessel': regionalschluessel,
            'sachmerkmal': '',
            'sachschluessel': ''
        })

    def _parse_elements(self, root, iteration_element_name, parse_long_description=False):
        out = []
        for element in root.iter(iteration_element_name):
            code = element.find('code')
            if code is not None:
                new_element = {
                    'id': code.text,
                    'description': self._clean(element.find('inhalt').text)
                }
                if parse_long_description:
                    new_element['longdescription'] = self._clean(element.find('beschriftungstext').text)
                out.append(new_element)
        return out

    @staticmethod
    def _clean(s):
        """Clean up a string"""
        if s is None:
            return None
        s = s.replace("\n", " ")
        s = s.replace("  ", " ")
        s = s.strip()
        return s
