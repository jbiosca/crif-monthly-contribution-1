from fixedwidth.fixedwidth import FixedWidth
import pandas as pd
from sqlalchemy import create_engine
from dbt.config import read_profiles
from logbook import Logger, StreamHandler
from datetime import datetime
import sys
import click

StreamHandler(sys.stdout).push_application()
log = Logger('crif-monthly-contribution')

CONFIGS = {
    'header': {
        "record_type": {
            "required": False,
            "type": "string",
            "start_pos": 1,
            "end_pos": 1,
            "alignment": "left",
            "padding": " "
        },
        "file_reference_date": {
            "required": False,
            "type": "string",
            "start_pos": 2,
            "end_pos": 9,
            "alignment": "right",
            "padding": " "
        },
        "crif_fi_code": {
            "required": False,
            "type": "string",
            "start_pos": 10,
            "end_pos": 14,
            "alignment": "left",
            "padding": " "
        },
        "code_linked_to_credit_line_data_file": {
            "required": False,
            "type": "string",
            "start_pos": 15,
            "end_pos": 17,
            "alignment": "left",
            "padding": " "
        },
        "filler": {
            "required": False,
            "type": "string",
            "start_pos": 18,
            "end_pos": 737,
            "alignment": "left",
            "padding": " "
        }
    },
    'trailer': {
        'record_type': {
            "required": False,
            "type": "string",
            "start_pos": 1,
            "end_pos": 1,
            "alignment": "left",
            "padding": " "
        },
        'record_number_counter_small': {
            "required": False,
            "type": "integer",
            "start_pos": 2,
            "end_pos": 7,
            "alignment": "right",
            "padding": "0"
        },
        'filler_a': {
            "required": False,
            "type": "string",
            "start_pos": 8,
            "end_pos": 36,
            "alignment": "left",
            "padding": " "
        },
        'record_number_counter_big': {
            "required": False,
            "type": "integer",
            "default": None,
            "start_pos": 37,
            "end_pos": 46,
            "alignment": "right",
            "padding": " "
        },
        'filler_b': {
            "required": False,
            "type": "string",
            "start_pos": 47,
            "end_pos": 737,
            "alignment": "left",
            "padding": " "
        }
    },
    'subject_data_details': {
        'record_type': {
            "required": False,
            "type": "string",
            "start_pos": 1,
            "end_pos": 1,
            "alignment": "left",
            "padding": " "
        },
        'crif_fi_code': {
            "required": False,
            "type": "string",
            "start_pos": 2,
            "end_pos": 6,
            "alignment": "left",
            "padding": " "
        },
        'branch_code': {
            "required": False,
            "type": "string",
            "start_pos": 7,
            "end_pos": 11,
            "alignment": "left",
            "padding": " "
        },
        'fi_subject_data_code': {
            "required": False,
            "type": "string",
            "start_pos": 12,
            "end_pos": 27,
            "alignment": "left",
            "padding": " "
        },
        'fi_credit_line_data_code': {
            "required": False,
            "type": "string",
            "start_pos": 28,
            "end_pos": 45,
            "alignment": "left",
            "padding": " "
        },
        'customer_type': {
            "required": False,
            "type": "string",
            "start_pos": 46,
            "end_pos": 46,
            "alignment": "left",
            "padding": " "
        },
        'fi_company_subject_data_code': {
            "required": False,
            "type": "string",
            "start_pos": 47,
            "end_pos": 62,
            "alignment": "left",
            "padding": " "
        },
        'link_to_company_code': {
            "required": False,
            "type": "string",
            "start_pos": 63,
            "end_pos": 63,
            "alignment": "left",
            "padding": " "
        },
        'borrower_type': {
            "required": False,
            "type": "string",
            "start_pos": 64,
            "end_pos": 64,
            "alignment": "left",
            "padding": " "
        },
        'tax_code': {
            "required": False,
            "type": "string",
            "start_pos": 65,
            "end_pos": 80,
            "alignment": "left",
            "padding": " "
        },
        'calculated_tax_code_flag': {
            "required": False,
            "type": "string",
            "start_pos": 81,
            "end_pos": 81,
            "alignment": "left",
            "padding": " "
        },
        'vat_registration_number': {
            "required": False,
            "type": "integer",
            "start_pos": 82,
            "end_pos": 92,
            "alignment": "left",
            "padding": "0"
        },
        'surname_name': {
            "required": False,
            "type": "string",
            "start_pos": 93,
            "end_pos": 212,
            "alignment": "left",
            "padding": " "
        },
        'surname_name_owner_of_sole_proprietorship': {
            "required": False,
            "type": "string",
            "start_pos": 213,
            "end_pos": 272,
            "alignment": "left",
            "padding": " "
        },
        'married_name': {
            "required": False,
            "type": "string",
            "start_pos": 273,
            "end_pos": 297,
            "alignment": "left",
            "padding": " "
        },
        'filler_a': {
            "required": False,
            "type": "string",
            "start_pos": 298,
            "end_pos": 322,
            "alignment": "left",
            "padding": " "
        },
        'country_and_municipality_of_birth': {
            "required": False,
            "type": "string",
            "start_pos": 323,
            "end_pos": 354,
            "alignment": "left",
            "padding": " "
        },
        'date_of_birth': {
            "required": False,
            "type": "string",
            "start_pos": 355,
            "end_pos": 362,
            "alignment": "left",
            "padding": " "
        },
        'gender': {
            "required": False,
            "type": "string",
            "start_pos": 363,
            "end_pos": 363,
            "alignment": "left",
            "padding": " "
        },
        'company_type': {
            "required": False,
            "type": "string",
            "start_pos": 364,
            "end_pos": 367,
            "alignment": "left",
            "padding": " "
        },
        'province_of_chamber_of_commerce_registration': {
            "required": False,
            "type": "string",
            "start_pos": 368,
            "end_pos": 369,
            "alignment": "left",
            "padding": " "
        },
        'chamber_of_commerce_registration_number': {
            "required": False,
            "type": "integer",
            "start_pos": 370,
            "end_pos": 376,
            "alignment": "left",
            "padding": "0"
        },
        'ceased_activity_flag': {
            "required": False,
            "type": "string",
            "start_pos": 377,
            "end_pos": 377,
            "alignment": "left",
            "padding": " "
        },
        'register_of_companies_province': {
            "required": False,
            "type": "string",
            "start_pos": 378,
            "end_pos": 380,
            "alignment": "left",
            "padding": " "
        },
        'register_of_companies_number': {
            "required": False,
            "type": "integer",
            "start_pos": 381,
            "end_pos": 387,
            "alignment": "left",
            "padding": "0"
        },
        'register_of_small_business_number': {
            "required": False,
            "type": "integer",
            "start_pos": 388,
            "end_pos": 394,
            "alignment": "left",
            "padding": "0"
        },
        'activity_group_area': {
            "required": False,
            "type": "integer",
            "start_pos": 395,
            "end_pos": 397,
            "alignment": "left",
            "padding": "0"
        },
        'activity_sector_subgroup': {
            "required": False,
            "type": "integer",
            "start_pos": 398,
            "end_pos": 400,
            "alignment": "left",
            "padding": "0"
        },
        'activity_subclassification': {
            "required": False,
            "type": "integer",
            "start_pos": 401,
            "end_pos": 403,
            "alignment": "left",
            "padding": " "
        },
        'address': {
            "required": False,
            "type": "string",
            "start_pos": 404,
            "end_pos": 443,
            "alignment": "left",
            "padding": " "
        },
        'zip_code': {
            "required": False,
            "type": "string",
            "start_pos": 444,
            "end_pos": 448,
            "alignment": "left",
            "padding": "0"
        },
        'municipality': {
            "required": False,
            "type": "string",
            "start_pos": 449,
            "end_pos": 478,
            "alignment": "left",
            "padding": " "
        },
        'province': {
            "required": False,
            "type": "string",
            "start_pos": 479,
            "end_pos": 480,
            "alignment": "left",
            "padding": " "
        },
        'country': {
            "required": False,
            "type": "string",
            "start_pos": 481,
            "end_pos": 483,
            "alignment": "left",
            "padding": " "
        },
        'telephone_number': {
            "required": False,
            "type": "string",
            "start_pos": 484,
            "end_pos": 499,
            "alignment": "left",
            "padding": " "
        },
        'filler_b': {
            "required": False,
            "type": "string",
            "start_pos": 500,
            "end_pos": 531,
            "alignment": "left",
            "padding": " "
        },
        'domicile_address': {
            "required": False,
            "type": "string",
            "start_pos": 532,
            "end_pos": 571,
            "alignment": "left",
            "padding": " "
        },
        'domicile_zip_code': {
            "required": False,
            "type": "string",
            "start_pos": 572,
            "end_pos": 576,
            "alignment": "left",
            "padding": "0"
        },
        'domicile_municipality': {
            "required": False,
            "type": "string",
            "start_pos": 577,
            "end_pos": 606,
            "alignment": "left",
            "padding": " "
        },
        'domicile_province': {
            "required": False,
            "type": "string",
            "start_pos": 607,
            "end_pos": 608,
            "alignment": "left",
            "padding": " "
        },
        'domicile_country': {
            "required": False,
            "type": "string",
            "start_pos": 609,
            "end_pos": 611,
            "alignment": "left",
            "padding": " "
        },
        'domicile_telephone_number': {
            "required": False,
            "type": "string",
            "start_pos": 612,
            "end_pos": 627,
            "alignment": "left",
            "padding": " "
        },
        'filler_c':  {
            "required": False,
            "type": "string",
            "start_pos": 628,
            "end_pos": 659,
            "alignment": "left",
            "padding": " "
        },
        'new_updated_subject_data_flag': {
             "required": False,
            "type": "integer",
            "start_pos": 660,
            "end_pos": 660,
            "alignment": "left",
            "padding": "0"
        },
        'ateco_code': {
            "required": False,
            "type": "string",
            "start_pos": 661,
            "end_pos": 668,
            "alignment": "left",
            "padding": " "
        },
        'year_of_ateco_code': {
            "required": False,
            "type": "string",
            "start_pos": 669,
            "end_pos": 672,
            "alignment": "left",
            "padding": " "
        },
        'filler_d': {
            "required": False,
            "type": "string",
            "start_pos": 673,
            "end_pos": 737,
            "alignment": "left",
            "padding": " "
        }
    }

}

wrapping_columns = {
    'customers': {
        'headers': ['record_type', 'file_reference_date', 'crif_fi_code', 'code_linked_to_credit_line_data_file'],
        'trailers': ['record_type', 'record_number_counter_small', 'record_number_counter_big']
    }
}


def dbt_url_provider(profile, target):
    target_profile = read_profiles()[profile]['outputs'][target]
    return f"redshift+psycopg2://{target_profile['user']}:{target_profile['pass']}@{target_profile['host']}:{target_profile['port']}/{target_profile['dbname']}"


class CrifDataFile:
    def __init__(self, header_vars, trailer_vars, subject_data_details):
        self.configs = CONFIGS
        self.header = FixedWidth(CONFIGS['header'])
        self.trailer = FixedWidth(CONFIGS['trailer'])
        self.header.update(**header_vars)
        self.trailer.update(**trailer_vars)
        self.records = []
        for detail in subject_data_details:
            fx = FixedWidth(CONFIGS['subject_data_details'])
            fx.update(**detail)
            self.records.append(fx)


    def header_line(self):
        return self.header.line

    def trailer_line(self):
        return self.trailer.line

    def get_subject_data_details(self):
        return [detail.line for detail in self.records]


QUERY = """
select * from mart_compliance.{} 
where date_trunc('month', to_date(header__file_reference_date, 'DDMMYYYY')) 
    = date_trunc('month', '{}'::date)
"""

class CrifTable:

    def __init__(self, table_name, file_type, month, connection_url):
        self.table_name = table_name
        self.file_type = file_type
        self.engine = create_engine(connection_url)
        self.month = month
        self.headers = {}
        self.table = None

    def __enter__(self):
        self.conn = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def get_headers(self):
        df = pd.read_sql(
            QUERY.format(self.table_name, self.month.isoformat()),
            self.conn,
            chunksize=1
        )
        for chunk in df:
            first_row = chunk.to_dict(orient='record')[0]
            self.headers['headers'] = {k.split('__')[1]: v for (k, v) in first_row.items() if k.startswith('header__')}
            self.headers['trailers'] = {k.split('__')[1]: v for (k, v) in first_row.items() if k.startswith('trailer__')}

    def check_headers(self):
        header_keys = self.headers['headers'].keys()
        trailer_keys = self.headers['trailers'].keys()
        if (set(header_keys) == set(wrapping_columns[self.file_type]['headers']) and
                set(trailer_keys) == set(wrapping_columns[self.file_type]['trailers'])):
            return True
        else:
            log.info(f"Invalid source table headers: {header_keys}. Expected {wrapping_columns[self.file_type]}")
            return False

    def pull_table(self):
        df = pd.read_sql(
            QUERY.format(self.table_name, self.month.isoformat()),
            self.conn
        )
        self.table = df

    def pull_table_as_dicts(self):
        self.pull_table()
        return self.table.to_dict(orient='record')

@click.command()
@click.option('--table-name', required=True, help='Name of the table containing the contribution.')
@click.option('--file-type', required=True, help='Type of file to be contributed')
@click.option('--date', required=True, help='Date to be contributed in YYYY-MM-DD format. It will be truncated to the month.')
@click.option('--dbt-profile', required=True, help='DBT profile target.')
def create_file(table_name, file_type, date, dbt_profile):
    parsed_date = datetime.strptime(date, '%Y-%m-%d')
    with CrifTable(table_name, file_type, parsed_date, dbt_url_provider('airflow', dbt_profile)) as table:
        table.get_headers()
        if table.check_headers():
            data = table.pull_table_as_dicts()
            headers = table.headers
        else:
            raise Exception('Invalid CRIF table')

    file = CrifDataFile(headers['headers'], headers['trailers'], data)

    print(file.header_line())
    for line in file.get_subject_data_details():
        print(line)
    print(file.trailer_line())


if __name__ == '__main__':
    create_file()

