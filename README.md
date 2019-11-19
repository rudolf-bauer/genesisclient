# PyGenesis
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
