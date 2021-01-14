from setuptools import setup, find_packages

setup(name='dagster-geomagical',
      packages=find_packages(),
      install_requires=[
            'dagster',
            'Celery',
            'redis',
            'google-cloud-storage',
      ])
