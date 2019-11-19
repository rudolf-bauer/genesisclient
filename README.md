# Fork info
This is a fork of:
[GitHub - marians/genesisclient: A Genesis (DeStatis et. al.) client for Python](https://github.com/marians/genesisclient)

The underlying wsdl library was migrated to `zeep` and the code was refactored. The work is still in progress.
The downloading of data is already working. Install using:
```
pip install git+https://github.com/rudolf-bauer/genesisclient.git
```

You can download the result as a `.csv` or `.xls` file. Also, you can directly download and parse the data as a `pandas` dataframe.
```
from genesisclient.genesis_client import GenesisClient

gc = GenesisClient(site='DESTATIS', username='', password='')

gc.download_csv('61111-07iz', 'data.csv')
gc.download_excel('61111-07iz', 'data.xls')

df = gc.download_dataframe('61111-07iz')
```

Note that you need to register in order to use the `DESTATIS` API.

# Original Readme.md

genesisclient
=============

A Genesis (DeStatis et. al.) client for Python - in a very early stage.

The goal here is to create a tool that allows for automated lookup and download of resources from several official statistics offices in Germany.

Currently, downloading of tables in various formats works in many cases.

## Installation

If you want to use pip:

    pip install genesisclient

The manual way: download or clone the code, change to the directory containing `setup.py` and execute

    python setup.py install

Then make sure the `lxml` and `suds` Python modules are installed.

## Supported backends

The client needs configuration for the backend systems to work with. On the command line you reference the desired system using the `-s` parameter and the handle for that system (see below for examples).

* `DESTATIS`: [www-genesis.destatis.de](https://www-genesis.destatis.de/)
* `LDNRW`: [landesdatenbank.nrw.de](https://www.landesdatenbank.nrw.de/)
* `REGIONAL`: [regionalstatistik.de](https://www.regionalstatistik.de/)
* `BAYERN`: [statistikdaten.bayern.de](https://www.statistikdaten.bayern.de)

You might need a user account (user name and password) for the system.

## Usage

### Downloading a data table

Download the table with reference code `13211-03ir` from the "Landesdatenbank NRW":

    genesiscl -s LDNRW -d 13211-03ir

This writes a file `13211-03ir.csv` to your current working directory.

The DESTATIS and BAYERN backends require a login for all operations. Once you have created an account on the desired backend system, you can pass user name and password via the `-u` and `-p` parameters:

    genesiscl -s DESTATIS -u YOUR_USERNAME -p YOUR_PASSWORD -d 14111-0001

#### Specifying the output file format

The parameter `-f` allows you to chose between `csv` (default), `xls` and `html` output.

    genesiscl -s LDNRW -d 13211-03ir -f xls

#### Selecting data for a specific region (experimental)

Genesis systems use a location hierarchy depending on which system you work with. When requesting a data table, by default, the data is not restricted to a specific region. When working with the DESTATIS system, this usually means you get data for entire Germany. When requesting a specific location, different data is contained in the response, usually matching the requested region.

Regions/locations are indicated using the "Amtlicher Gemeindeschlüssel". See https://github.com/marians/agssearch for a handy tool to find keys for location names. Note that you can omit trailing zeros.

This example shows how to download data from table `13211-03ir` for the City of Cologne:

    genesiscl -s LDNRW -d 13211-03ir --rs 05315

This feaure is marked "experimental" here since it's for now it seems undeterministic when the --rs switch actually makes a difference.

### Finding resources (experimental)

In order to find resources, ultimately tables, for specific keywords, try the `-g` or `--search` option.

    genesiscl -s LDNRW -g kind

The above search will spit out items matching the search term "kind".

The Genesis search engine is a bit tricky. Watch how the search above does not return any terms ("BEGRIFF"). However, if we have an asterisk in our search term, it returns nothing but terms.

    genesiscl -s LDNRW -g "kind*"

Under the hood, the backends are using Lucene for search. So some Lucene search term syntax can be used to form more complex search queries. The default boolean operator for multi-token search terms seems to be "AND".

    >>> genesiscl -s LDNRW -g "kind OR steuer"
    Hits of type 'MERKMAL': 19
	Hits of type 'TABELLE': 30
	Hits of type 'STATISTIK': 14
	...

    >>> genesiscl -s LDNRW -g "kind AND steuer"
    Hits of type 'STATISTIK': 1
    ...

    >>> genesiscl -s LDNRW -g "kind steuer"
    Hits of type 'STATISTIK': 1
    ...

It seems as if the unique codes used for properties, for example, cannot be found using the search engine.

	>>> genesiscl -s LDNRW -g "Kontinente Staatsangehörigkeit"
	Hits of type 'MERKMAL': 1
	Hits of type 'STATISTIK': 2
	MERKMAL KONTI2 Kontinente der Staatsangehörigkeit
	...

Although the search above returns "KONTI2" as a property, the following search won't find anything:

    genesiscl -s LDNRW -g KONTI2

But Genesis has a different way to look up resources by their unique identifier, and the client has, too. It uses the `-l` or `--lookup` option.

Now you can look up your known property:

    genesiscl -s LDNRW -l KONTI2

As a result you get not only properties, but also all kinds of related items, including - if available - tables which make use of the property.

## Like genesisclient?

Feel free to [tip me](https://www.gittip.com/marians/)!
