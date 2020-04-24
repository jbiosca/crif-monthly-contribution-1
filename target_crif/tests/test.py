import unittest
from target_crif.target_crif import CrifDataFile, CrifTable
from testcontainers.postgres import PostgresContainer
from sqlalchemy import create_engine
import pandas as pd
from target_crif.tests.sample_data import sample_dict, sample_fixed

DATE = '20200420'
CRIF_FI_CODE = 'F9541'
CODE_LINKED_TO_CREDIT_LINE_DATA_FILE = '1'

class TestSubjectDataFile(unittest.TestCase):
    """
    Test CRIF data file generation
    """
    def setUp(self) -> None:
        self.header_vars = dict(
            record_type='T',
            file_reference_date=DATE,
            crif_fi_code=CRIF_FI_CODE,
            code_linked_to_credit_line_data_file=CODE_LINKED_TO_CREDIT_LINE_DATA_FILE
        )
        self.trailer_vars = dict(
            record_type='E',
            record_number_counter_small=2,
            record_number_counter_big=1000002
        )

    def test_header(self):
        data_file = CrifDataFile(self.header_vars, self.trailer_vars, [])
        self.assertEqual(
            data_file.header_line(),
            'T20200420F95411                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  \r\n'
        )

    def test_trailer(self):
        data_file = CrifDataFile(
            self.header_vars,
            dict(record_type='E', record_number_counter_small=4),
            []
        )
        self.assertEqual(
            data_file.trailer_line(),
            sample_fixed[-1] + '\r\n'
        )

    def test_subject_data_details(self):
        data = {
            'record_type': 'A',
            'crif_fi_code': 'XXXXX',
            'branch_code': '00001',
            'fi_subject_data_code': '0001239',
            'fi_credit_line_data_code': 'C01M1_PC005PD',
            'customer_type': 'G',
            'fi_company_subject_data_code': None,
            'link_to_company_code': None,
            'borrower_type': '1',
            'tax_code': 'XXXXXXXXXXXXXXXX',
            'calculated_tax_code_flag': 0,
            'vat_registration_number': 0,
            'surname_name': 'DEMARCHI,RAFFAELE                                                                                                       ',
            'surname_name_owner_of_sole_proprietorship': None,
            'married_name': None,
            'country_and_municipality_of_birth': 'SPAGNA,EE',
            'date_of_birth': 'XXXXXXXX',
            'gender': 'M',
            'company_type': None,
            'province_of_chamber_of_commerce_registration': None,
            'chamber_of_commerce_number': '0000000',
            'ceased_activity_flag': ' ',
            'chamber_of_commerce_number': None,
            'register_of_companies_province': None,
            'register_of_small_business_number': 0,
            'activity_group_area': 6,
            'activity_sector_subgroup': 0,
            'activity_subclassification': None,
            'address': 'VIA GIUSEPPE RE DAVID',
            'zip_code': 'XXXXX',
            'municipality': 'MADRID',
            'province': 'EE',
            'country': 'I',
            'telephone_number': None,
            'domicile_address': None,
            'domicile_zip_code': None,
            'domicile_municipality': None,
            'domicile_province': None,
            'domicile_country': None,
            'domicile_telephone_number': None,
            'new_updated_subject_data_flag': 0,
            'ateco_code': None,
            'year_of_ateco_code': None
        }
        data_file = CrifDataFile(
            self.header_vars,
            self.trailer_vars,
            [data]
        )
        self.assertEqual(
            data_file.get_subject_data_details()[0],
            'AXXXXX000010001239         C01M1_PC005PD     G                 1XXXXXXXXXXXXXXXX000000000000DEMARCHI,RAFFAELE                                                                                                                                                                                                                     SPAGNA,EE                       XXXXXXXXM      0000000    00000000000000600000   VIA GIUSEPPE RE DAVID                   XXXXXMADRID                        EEI                                                                                          00000                                                                                   0                                                                             ' + '\r\n'
        )


    def test_squared(self):
        data_file = CrifDataFile(self.header_vars, self.trailer_vars, [])
        self.assertEqual(
            len(data_file.header_line()),
            len(data_file.trailer_line())
        )


class TestPullCrifTable(unittest.TestCase):
    def setUp(self) -> None:

        self.sample_table_name = 'crif_sample'
        self.file_type = 'customers'
        self.sample_df = pd.DataFrame(sample_dict)

        self.postgres_container = PostgresContainer("postgres:9.5").start()
        with create_engine(self.postgres_container.get_connection_url()).connect() as conn, conn.begin():
            conn.execute("create schema if not exists mart_compliance ")
            self.sample_df.to_sql(
                self.sample_table_name,
                conn,
                schema='mart_compliance'
            )

        self.crif_table = CrifTable(self.sample_table_name, 'customers', self.postgres_container.get_connection_url())

    def tearDown(self) -> None:
        self.postgres_container.stop()

    def test_header_checks(self):
        with self.crif_table as crif_table:
            crif_table.get_headers()
            assert crif_table.check_headers()

    def test_pull_table(self):
        with self.crif_table as crif_table:
            crif_table.pull_table()
            assert crif_table.table.shape == (7, 51)
