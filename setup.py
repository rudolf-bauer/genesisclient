# encoding: utf-8

from setuptools import setup

setup(name='pygenesis',
      version='0.0.1',
      description='PyGenesis - get access to german statistical offices using the genesis API',
      author='Rudolf Bauer',
      author_email='github@rudi-bauer.com',
      url='https://github.com/rudolf-bauer/genesisclient.git',
      license="MIT",
      packages=['pygenesis'],
      install_requires=['logging',
                        'lxml',
                        'numpy',
                        'pandas',
                        'requests',
                        'urllib3',
                        'zeep'])
