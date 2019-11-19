# PyGenesis
This library is a fork of:
[GitHub - marians/genesisclient: A Genesis (DeStatis et. al.) client for Python](https://github.com/marians/genesisclient)

The aim of the project is to provide simplified data access to German statistical offices in Python using the so-called 
[Genesis API](https://www.destatis.de/EN/Service/OpenData/_node.html). 
The focus is on the Federal Statistical Office of Germany (Statistisches Bundesamt) with its online website 
[destatis.de](destatis.de). This code represents an early stage of the development process. 
Please report all bugs in the issues tabs.

# Installation
 
Currently only the github installation is supported:
```
pip install git+https://github.com/rudolf-bauer/genesisclient.git
```

# Usage

First, import the client class and create an object by specifying the site you want to access.
```
from pygenesis.py_genesis_client import PyGenesisClient

client = PyGenesisClient(site='DESTATIS', username='', password='')
```

Currently supported sites:

`DESTATIS`: [Federal Statistical Office of Germany](https://www-genesis.destatis.de)

`LDNRW`: [Statistical office of the German state North Rhine-Westphalia](https://www.landesdatenbank.nrw.de)

For `DESTATIS` you need to provide a user and a password. You can register 
[here](https://www-genesis.destatis.de/genesis/online?Menu=RegistrierungForm&REGKUNDENTYP=001). 

Then, you can download and save the result as a `.csv` or `.xls` file. 
Also, you can directly read the data as a parsed `pandas` dataframe.
```
client.download_csv('12411-0001', 'data.csv')
client.download_excel('12411-0001', 'data.xls')

df = client.read('12411-0001')
```

You can look up the table codes on the corresponding websites.
