import unittest
from target_crif.target_crif import SubjectDataFile

DATE = 20200420
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
        data_file = SubjectDataFile(self.header_vars, self.trailer_vars)
        self.assertEqual(
            data_file.header_line(),
            'T20200420F95411                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  \r\n'
        )

    def test_trailer(self):
        # TODO: test with a real sample
        pass

    def test_subject_data_details(self):
        # TODO: define and test with real example in the appropriate task
        pass

    def test_squared(self):
        data_file = SubjectDataFile(self.header_vars, self.trailer_vars)
        self.assertEqual(
            len(data_file.header_line()),
            len(data_file.trailer_line())
        )