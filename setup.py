from setuptools import setup, find_packages

setup(
    name='crif-monthly-contribution',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'Click==7.1.2',
        'psycopg2==2.7.7',
        'pandas==1.0.3',
        'FixedWidth==1.3',
        'sqlalchemy-redshift==0.7.7',
        'dbt==0.13.1',
        'Logbook==1.5.3'
    ],
    entry_points='''
        [console_scripts]
        cirf_monthly_contribution=target_crif.target_crif:create_file
    ''',
)