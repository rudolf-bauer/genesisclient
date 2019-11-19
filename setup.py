# encoding: utf-8

from setuptools import setup

setup(name='genesisclient',
      version='0.0.8',
      description='Genesis (DeStatis et. al.) client for Python',
      author='Marian Steinbach',
      author_email='marian@sendung.de',
      url='https://github.com/rudolf-bauer/genesisclient.git',
      license="MIT",
      packages=['genesisclient'],
      install_requires=['lxml', 'zeep', 'requests', 'tempfile', 'logging', 'pandas', 'numpy'])
