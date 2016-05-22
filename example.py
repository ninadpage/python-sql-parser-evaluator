from sqlparser import  evaluate_select_statement


__author__ = "ninadpage"


if __name__ == '__main__':
    _db = {
        'records': [
            {
                'id': 1,
                'name': 'Name1',
                'description': 'Some description',
                'value': 10,
            },
            {
                'id': 2,
                'name': 'Name2',
                'description': 'Some description',
                'value': 20,
            },
            {
                'id': 3,
                'name': 'Name3',
                'description': 'Some description',
                'value': 30,
            },
            {
                'id': 4,
                'name': 'Name4',
                'description': 'Some description',
                'value': 40,
            },
        ]
    }

    select = "SELECT * FROM records WHERE value > 30 OR name = 'Name2'"
    print evaluate_select_statement(_db, select)
