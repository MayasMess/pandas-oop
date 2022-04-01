import string
import random
from unittest import TestCase
import pandas as pd
from pandas import Timestamp

from src.pandas_oop.custom_exceptions import MissingDecorator
from tests.test_models_declaration import PeopleNoTable, PEOPLE_DATA_FILE, People, PeopleFromDatabase, UniqueCars, \
    CARS_DATA_FILE


class TestSqlOperations(TestCase):

    def test_missing_sql_decorator_error(self):
        people = PeopleNoTable(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        if people.is_valid():
            self.assertRaises(MissingDecorator, people.save, if_exists='replace', index=False)

    def test_table_attribute(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        if people.is_valid():
            people.save(if_exists='replace')
        people_from_db = PeopleFromDatabase(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_insert_or_update(self):
        random_string = [self.get_random_string() for _ in range(3)]
        cars = UniqueCars(from_csv=CARS_DATA_FILE, delimiter=";")
        cars.random_string = random_string
        cars.save(if_row_exists='update')
        expected_result = UniqueCars(from_sql_query='select random_string from cars').random_string.tolist()
        self.assertEqual(random_string, expected_result)

    def setUp(self):
        # Old school creation
        self.old_school_df = pd.DataFrame({'name': pd.Series(dtype='O'),
                                           'age': pd.Series(dtype='int'),
                                           'money': pd.Series(dtype='float'),
                                           'insertion_date': pd.Series(dtype='datetime64[ns]'),
                                           'is_staff': pd.Series(dtype='bool')})
        self.old_school_read_csv_df = pd.read_csv(PEOPLE_DATA_FILE, delimiter=';', parse_dates=['insertion_date'])
        self.old_school_read_csv_df['is_staff'] = self.old_school_read_csv_df['is_staff'].map({'yes': True,
                                                                                               'no': False})

        # Test variable for new creation
        self.name_list = ["John", "Snow"]
        self.age_list = [15, 40]
        self.money_list = [13.6, 6.7]
        self.insertion_date_list = [Timestamp('2005-02-25'), Timestamp('2005-02-25')]
        self.is_staff_list = [True, False]

        self.expected_result = {
            'name': {
                0: 'John',
                1: 'Snow'
            },
            'age': {
                0: 15,
                1: 40
            },
            'money': {
                0: 13.6,
                1: 6.7
            },
            'insertion_date': {
                0: Timestamp('2005-02-25'),
                1: Timestamp('2005-02-25')
            },
            'is_staff': {
                0: True,
                1: False
            },
        }

        self.string_insertion_date_list = ['25-02-2005', '25-02-2005']

    @staticmethod
    def get_random_string() -> str:
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
