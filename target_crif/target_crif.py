from fixedwidth.fixedwidth import FixedWidth

CONFIGS = {
    'header': {
        "record_type": {
            "required": True,
            "type": "string",
            "start_pos": 1,
            "end_pos": 1,
            "alignment": "left",
            "padding": " "
        },
        "file_reference_date": {
            "required": True,
            "type": "integer",
            "start_pos": 2,
            "end_pos": 9,
            "alignment": "right",
            "padding": "0"
        },
        "crif_fi_code": {
            "required": True,
            "type": "string",
            "start_pos": 10,
            "end_pos": 14,
            "alignment": "left",
            "padding": " "
        },
        "code_linked_to_credit_line_data_file": {
            "required": True,
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
            "required": True,
            "type": "string",
            "start_pos": 1,
            "end_pos": 1,
            "alignment": "left",
            "padding": " "
        },
        'record_number_counter_small': {
            "required": True,
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
            "required": True,
            "type": "integer",
            "start_pos": 37,
            "end_pos": 46,
            "alignment": "right",
            "padding": "0"
        },
        'filler_b': {
            "required": False,
            "type": "string",
            "start_pos": 47,
            "end_pos": 737,
            "alignment": "left",
            "padding": " "
        }
    }
}

class CrifDataFile:
    def __init__(self, header_vars, trailer_vars):
        self.configs = CONFIGS
        self.header = FixedWidth(CONFIGS['header'])
        self.trailer = FixedWidth(CONFIGS['trailer'])
        self.header.update(**header_vars)
        self.trailer.update(**trailer_vars)
        # self.records = []

    def header_line(self):
        return self.header.line

    def trailer_line(self):
        return self.trailer.line