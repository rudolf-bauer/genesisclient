# encoding: utf8

import suds
import logging
from lxml import etree

gc = None


def download(client, args):
    """
    Issue a download from command line arguments
    """
    rs = '*'
    path = '%s.%s' % (args.download, args.format)
    if args.regionalschluessel is not None and args.regionalschluessel != '*':
        rs = args.regionalschluessel
        path = '%s_%s.%s' % (args.download, args.regionalschluessel, args.format)
    print "Downloading to file %s" % path
    result = client.table_export(args.download,
            regionalschluessel=rs,
            format=args.format)
    open(path, 'wb').write(result)


def search(client, args):
    """
    Search the catalog for a term
    using options given via the command line
    """
    term = args.searchterm
    if type(term) != unicode:
        term = term.decode('utf8')
    result = client.search(term)
    for cat in result['meta'].keys():
        if result['meta'][cat] > 0:
            print "Hits of type '%s': %d" % (cat.upper(), result['meta'][cat])
    for hit in result['results']:
        otype = hit['type'].upper()
        if otype == 'MERKMAL' or otype == 'STATISTIK':
            print "%s %s %s" % (otype, clean(hit['name']), clean(hit['description']))
        elif otype == 'TABELLE':
            print "%s %s %s" % (otype, clean(hit['name']), clean(hit['description']))
        elif otype == 'BEGRIFF':
            print "%s %s" % (otype, clean(hit['name']))
        else:
            print "%s %s" % (hit['type'].upper(), hit)


def lookup(client, args):
    """
    Lookup tables and print out info on found entries
    """
    term = args.lookup
    if type(term) != unicode:
        term = term.decode('utf8')
    for stat in gc.statistics(filter=term):
        print "STATISTIC: %s %s" % (stat['id'], stat['description'])
    for s in gc.statistic_data(statistic_code=term):
        print "STATISTIC DATA: %s %s" % (s['id'], s['description'])
    for s in gc.statistic_properties(statistic_code=term):
        print "STATISTIC PROPERTY: %s %s" % (s['id'], s['description'])
    for s in gc.statistic_tables(statistic_code=term):
        print "STATISTIC TABLE: %s %s" % (s['id'], s['description'])
    for prop in gc.properties(filter=term):
        print "PROPERTY: %s %s" % (prop['id'], prop['description'])
    if '*' not in term:
        for prop in gc.property_occurrences(property_code=term):
            print "PROPERTY OCCURRENCE: %s %s" % (prop['id'], prop['description'])
    for prop in gc.property_data(property_code=term):
        print "PROPERTY DATA: %s %s" % (prop['id'], prop['longdescription'])
    for table in gc.tables(filter=term):
        print "TABLE: %s %s" % (table['id'], table['description'])
    for term in gc.terms(filter=term):
        print "TERM: %s %s" % (term['id'], term['description'])


def main():
    #logging.basicConfig(level='DEBUG')
    logging.basicConfig(level='WARN')
    # Our command-line options
    import sys
    import argparse
    parser = argparse.ArgumentParser(description='These are todays options:')
    parser.add_argument('-s', dest='site', default=None,
                   help='Genesis site to connect to (DESTATIS or LDNRW)')
    parser.add_argument('-u', dest='username', default='',
                   help='username for Genesis login')
    parser.add_argument('-p', dest='password', default='',
                   help='username for Genesis login')
    parser.add_argument('-l', '--lookup', dest='lookup', default=None,
                   metavar="FILTER",
                   help='Get information on the table, property etc. with the key named FILTER. * works as wild card.')
    parser.add_argument('-g', '--search', dest='searchterm', default=None,
                   metavar="SEARCHTERM",
                   help='Find an item using an actual search engine. Should accept Lucene syntax.')
    parser.add_argument('-d', '--downlaod', dest='download', default=None,
                   metavar="TABLE_ID",
                   help='Download table with the ID TABLE_ID')
    parser.add_argument('--rs', dest='regionalschluessel', default=None,
                   metavar="RS", help='Only select data for region key RS')
    parser.add_argument('-f', '--format', dest='format', default='csv',
                   metavar="FORMAT", help='Download data in this format (csv, html, xls). Default ist csv.')

    args = parser.parse_args()

    # create the webservice client
    gc = GenesisClient(args.site, username=args.username,
                    password=args.password)
    # test if the service works
    #gc.test_service()

    if args.download is not None:
        download(gc, args)
    elif args.searchterm is not None:
        search(gc, args)
    elif args.lookup is not None:
        lookup(gc, args)

    # See? All I allow you to do is download stuff.
    sys.exit()

    # submit a search
    result = gc.search('schule', limit=10, category='Tabelle')
    counter = 0
    for item in result:
        #print counter, item.name, item.objektTyp, item.kurztext
        counter += 1

    # retrieve terms satrting with 'a'.
    terms = gc.terms(filter='a*')
    print "Terms list has", len(terms), "entries. Example:"
    print (terms[0].inhalt)

    # retrieve catalogue items starting with "11111"
    catalogue = gc.catalogue(filter='11111*')
    print "Catalogue result has", len(catalogue), "entries. Example:"
    print (catalogue[0].code,
           catalogue[0].beschriftungstext.replace("\n", " "),
           catalogue[0].inhalt)

    # retrieve properties
    properties = gc.properties(filter='B*', type='sachlich')
    print "Properties list has", len(properties), "entries. Example:"
    print (properties[0].code, properties[0].inhalt)

    # retrieve occurences for a property
    occurences = gc.property_occurrences(property_code=properties[0].code)
    print "Occurrences list has", len(occurences), "entries. Example:"
    print (occurences[0].code, occurences[0].inhalt)

    # retrieve data for a property
    data = gc.property_data(property_code=properties[0].code)
    print "Data list has", len(data), "entries. Example:"
    print (data[0].code,
           data[0].inhalt,
           data[0].beschriftungstext.replace("\n", " "))

    statistics = gc.property_statistics(property_code=properties[0].code)
    print "Statistics list has", len(statistics), "entries. Example:"
    print (statistics[0].code,
           statistics[0].inhalt.replace("\n", " "))

    tables = gc.property_tables(property_code=properties[0].code)
    print "Tables list has", len(statistics), "entries. Example:"
    print (tables[0].code,
           tables[0].inhalt.replace("\n", " "))

    table = gc.table_export(table_code=tables[0].code)
    print table

if __name__ == '__main__':
    main()
