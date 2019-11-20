from zeep import Client
import requests
import tempfile
from .parser import parse_csv
import logging
from .utils import filter_urllib3_logging


class PyGenesisClient(object):

    sites = {
        'DESTATIS': {
            'webservice_url': 'https://www-genesis.destatis.de/genesisWS'
        },
        'LDNRW': {
            'webservice_url': 'https://www.landesdatenbank.nrw.de/ldbnrwws'
        }
    }

    endpoints = {
        'DownloadService': '/services/DownloadService?wsdl',
        'DownloadService_2010': '/services/DownloadService_2010?wsdl'
    }

    zeep_client = None

    def __init__(self, site, username=None, password=None, language='de', drop_empty_rows_and_columns=False):
        """
        The `pygenesis` client allows to download metadata and table data (csv/xls) from a Genesis-website.
        :param site: str, website to download from, has to be one of: `GenesisClient.sites`.
        :param username: str, username of the page.
        :param password: str, password of the page.
        :param language: str, language identifier, currently only `de` was tested.
        :param drop_empty_rows_and_columns: bool, if True, a flag is send that promises to drop empty rows and columns.
        """

        # Ensure valid site
        if site is None:
            raise Exception('No site given')
        if site not in self.sites:
            site_keys = ", ".join(sorted(self.sites.keys()))
            raise ValueError('Site `%s` is unknown. Use one of %s.' % (site, site_keys))

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
        self.drop_empty_rows_and_columns = drop_empty_rows_and_columns

        # Workaround: disable ugly `urllib3` warning as log as there is no solution
        # https://github.com/urllib3/urllib3/issues/800
        # https://stackoverflow.com/questions/49338811/does-requests-properly-support-multipart-responses
        filter_urllib3_logging()

    def download_excel(self, table_code, output_filename, start_year=1900, end_year=2100):
        """
        Download an Excel file as binary and save to file.
        :param table_code: str, the code of the table to download.
        :param output_filename: str, name of the file to save the result to.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :return: None
        """
        xls_bytes = self._download_excel_bytes(table_code, start_year, end_year)
        with open(output_filename, 'wb') as f:
            f.write(xls_bytes)

    def _download_excel_bytes(self, table_code, start_year=1900, end_year=2100):
        """
        Download an Excel file as binary.
        :param table_code: str, the code of the table to download.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :return: Binary array, contains the Excel file.
        """
        #  client = self._init_service_client('DownloadService')
        client = self._init_service_client('DownloadService_2010')
        params = self._build_download_params_2010(table_code, start_year, end_year)
        result = client.service.ExcelDownload(**params)
        return result.attachments[0].content

    def download_csv(self, table_code, output_filename, start_year=1900, end_year=2100):
        """
        Download a CSV file as UTF-8 string and save to file.
        :param table_code: str, the code of the table to download.
        :param output_filename: str, name of the file to save the result to.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :return: None
        """
        csv_string = self._download_csv_string(table_code, start_year, end_year)
        with open(output_filename, 'w') as f:
            f.write(csv_string)

    def read(self, table_code, start_year=1900, end_year=2100, skip_header_rows=0,
             na_values=['-', '/', 'x', '.', '...']):
        """
        Read a data frame specified by a `table_code`. Uses the `format=datencsv` option in Genesis API.
        :param table_code: str, the code of the table to download.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :param skip_header_rows: int, number of rows from the header lines to drop. Results in shorter columns.
        :param na_values: list of str, values that should be regarded as `np.nan`. See `pd.read_csv`.
        :return: pd.DataFrame.
        """
        csv_string = self._download_csv_string(table_code, start_year, end_year)
        return parse_csv(csv_string, skip_header_rows=skip_header_rows, na_values=na_values)

    #  def download_timeseries(self, table_code, start_year=1900, end_year=2100):
    #    client = self._init_service_client('DownloadService_2010')
    #    params = self._build_download_params_timeseries_2010(table_code, start_year, end_year)
    #    result = client.service.ZeitreihenDownload(**params)
    #    return result

    def _download_csv_string(self, table_code, start_year=1900, end_year=2100):
        """
        Download a CSV file as UTF-8 string.
        :param table_code: str, the code of the table to download.
        :param start_year: int, the first year to download.
        :param end_year: int, the last year to download.
        :return: str, UTF-8, contains the csv string.
        """
        client = self._init_service_client('DownloadService_2010')
        params = self._build_download_params_2010(table_code, start_year, end_year)
        params.update({'format': 'datencsv'})
        result = client.service.TabellenDownload(**params)
        return result.attachments[0].content.decode("utf-8")

    def _init_service_client(self, name):
        """
        Initialize a client for a certain endpoint, identified by name,
        returns it and stores it internally for later re-use.
        """
        if name not in self.service_clients:

            url = (self.sites[self.site]['webservice_url'] + self.endpoints[name])
            logging.info("Downloading WSDL from url: `%s`" % url)
            
            # Workaround: `zeep` does not support the `apachesoap:DataHandler` datatype
            # -> replace by `xsd:base64Binary` (in WSDL file)
            response = requests.get(url)
            wsdl_string = response.text.replace('apachesoap:DataHandler', 'xsd:base64Binary')
            with tempfile.NamedTemporaryFile() as tmp:
                tmp.write(wsdl_string.encode("utf-8"))
                self.service_clients[name] = Client(wsdl=tmp.name)
            logging.info("Initialized site: `%s`" % name)
        return self.service_clients[name]

    def _clone_and_update_base_params(self, update_dict):
        params = self.base_params.copy()
        params.update(update_dict)
        return params

    def _build_download_params(self, table_code, start_year, end_year):
        return self._clone_and_update_base_params({
            'name': table_code,
            'bereich': 'Alle',
            'komprimierung': self.drop_empty_rows_and_columns,
            'startjahr': str(start_year),
            'endjahr': str(end_year),
            'zeitscheiben': '',
            'regionalschluessel': '',
            'sachmerkmal': '',
            'sachschluessel': ''
        })

    def _build_download_params_2010(self, table_code, start_year, end_year):
        return self._clone_and_update_base_params({
            'name': table_code,
            'bereich': 'Alle',
            'komprimieren': self.drop_empty_rows_and_columns,
            'transponieren': False,
            'startjahr': str(start_year),
            'endjahr': str(end_year),
            'zeitscheiben': '',
            'regionalmerkmal': '',
            'regionalschluessel': '',
            'sachmerkmal': '',
            'sachschluessel': '',
            'sachmerkmal2': '',
            'sachschluessel2': '',
            'sachmerkmal3': '',
            'sachschluessel3': '',
            'auftrag': False,
            'stand': ''
        })

    def _build_download_params_timeseries_2010(self, table_code, start_year, end_year):
        return self._clone_and_update_base_params({
            'name': table_code,
            'format': 'csv',
            'bereich': 'Alle',
            'komprimieren': self.drop_empty_rows_and_columns,
            'transponieren': False,
            'startjahr': str(start_year),
            'endjahr': str(end_year),
            'zeitscheiben': '',
            'inhalte': '',
            'regionalmerkmal': '',
            'regionalschluessel': '',
            'regionalschluesselcode': '',
            'sachmerkmal': '',
            'sachschluessel': '',
            'sachschluesselcode': '',
            'sachmerkmal2': '',
            'sachschluessel2': '',
            'sachschluesselcode2': '',
            'sachmerkmal3': '',
            'sachschluessel3': '',
            'sachschluesselcode3': '',
            'auftrag': False,
            'stand': ''
        })
