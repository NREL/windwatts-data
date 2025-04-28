from setuptools import setup, find_packages

setup(
    name='windwatts_data',
    version='1.0.1',
    author='Sameer Shaik',
    author_email='sameer.shaik@nrel.gov',
    description='A python package for retrieving and querying climate data using AWS',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/NREL/windwatts-data',
    packages=find_packages(),
    license="BSD-3-Clause",
    include_package_data=True,
    package_data={'windwatts_data': ['data/*.pkl.gz']},
    install_requires=[
        'boto3',
        'pandas',
        'botocore',
        'numpy',
        'scipy',
        'geopandas',
        'tqdm'
    ],
    python_requires='>=3.7',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
