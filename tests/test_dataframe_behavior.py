from unittest import TestCase
import pandas as pd
import numpy as np
from pandas import Timestamp

from src.pandas_oop.models import DataFrame
from tests.test_models_declaration import People, PeopleNoTable, PEOPLE_DATA_FILE, PeopleFromDatabase, \
    PeopleFromDatabaseWithoutBoolArgs, PEOPLE2_DATA_FILE, PeopleJobs, UniqueCars, MergedPeople, retrieve_people, \
    PeopleFromIterator, PeopleDeclaredWithDifferentFields, LOT_OF_PEOPLE_DATA_FILE


class TestDataframeBehavior(TestCase):

    def test_instance_is_dataframe(self):
        people = People()
        self.assertIsInstance(people, pd.DataFrame, "Not an instance of pandas dataframe")

    def test_object_is_not_singleton(self):
        people_1 = People()
        people_2 = People()
        self.assertIsNot(people_2, people_1)

    def test_instance_is_dataframe_no_table(self):
        people = PeopleNoTable()
        self.assertIsInstance(people, pd.DataFrame, "Not an instance of pandas dataframe")

    def test_append_list_to_one_column(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.insertion_date_list
        people.is_staff = self.is_staff_list

        result = people.to_dict()

        self.assertEqual(result, self.expected_result)

    def test_from_csv(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        result = people.to_dict()
        self.assertEqual(result, self.expected_result)
        self.assertEqual(people.insertion_date.dtype.type, np.datetime64, "Column is not a date")

    def test_from_sql_query(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.sql_engine.execute('delete from people')
        people.save()
        people_from_db = PeopleFromDatabase(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_from_sql_query_without_bool_args(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.sql_engine.execute('delete from people')
        people.save()
        people_from_db = PeopleFromDatabaseWithoutBoolArgs(from_sql_query='select * from people')
        self.assertEqual(people_from_db.to_dict(), people.to_dict())

    def test_dataframe_is_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.insertion_date_list
        people.is_staff = self.is_staff_list
        self.assertTrue(people.is_valid())

    def test_dataframe_validate(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        people.validate()
        people.save()
        self.assertTrue(people.is_valid())

    def test_dataframe_is_not_valid(self):
        people = People()
        people.name = self.name_list
        people.age = self.age_list
        people.money = self.money_list
        people.insertion_date = self.string_insertion_date_list
        people.is_staff = self.is_staff_list
        self.assertFalse(people.is_valid())

    def test_isnull_return_custom_df(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";").isnull()
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when isnull is called')

    def test_head_return_custom_df(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";").head(1)
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when head is called')

    def test_abs_return_custom_df(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.name = [3, -7]
        people.insertion_date = [3, -7]
        people.is_staff = [3, -7]
        people = people.abs()
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when abs is called')

    def test_merge_return_custom_df(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people2 = PeopleJobs(from_csv=PEOPLE2_DATA_FILE, delimiter=";")
        merged_result = people.merge(people2, on='name')
        self.assertIsInstance(merged_result, DataFrame, 'Not a custom dataframe when abs is called')
        self.assertEqual(merged_result.to_dict(), self.expected_merged_result)

    def test_validate_accept_argument(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people_jobs = PeopleJobs(from_csv=PEOPLE2_DATA_FILE, delimiter=";")
        merged_result = people.merge(people_jobs, on='name').validate(from_class=MergedPeople)
        self.assertEqual(str(merged_result), 'MergedPeople')

    def test_transform_df_to_custom_df_from_class_instantiation(self):
        data = pd.read_csv(filepath_or_buffer=PEOPLE_DATA_FILE, delimiter=";")
        people = People(from_df=data)
        self.assertEqual(str(people), 'People')

    def test_populate_from_iterator(self):
        people = PeopleFromIterator(from_iterator=retrieve_people)
        self.assertEqual(people.shape, (1000, 5))
        self.assertTrue(people.is_valid())

    def test_dataframe_has_column_name_declared(self):
        people = PeopleDeclaredWithDifferentFields(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        self.assertEqual(list(people.columns), ['name_test', 'age', 'money_test', 'insertion_date_test', 'is_staff'])

    def test_slicing_return_custom_df(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people = people[people.name == 'John']
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when slicing is performed')

    def test_when_loc_is_performed(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people = people.loc[people.name == 'John']
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when loc is performed')

    def test_when_loc_set_value_is_performed(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people.loc[people.name == 'John'] = ('Marie', 15, 15.0, Timestamp('2005-02-25'), True)
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when loc set value is performed')

    def test_when_loc_slice_indexing_is_performed(self):
        people = People(from_csv=PEOPLE_DATA_FILE, delimiter=";")
        people = people[:1]
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when loc set value is performed')

    def test_multi_loc_conditions(self):
        people = People(from_csv=LOT_OF_PEOPLE_DATA_FILE, delimiter=";")
        people = people.loc[(people.age < 18) & (people.name.str.startswith("M"))]
        self.assertEqual(people.shape, (3, 5))
        self.assertIsInstance(people, DataFrame, 'Not a custom dataframe when loc multiple conditions is performed')

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

        self.expected_merged_result = {
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
            'job': {
                0: 'Developer',
                1: 'RH'
            },
        }

        self.string_insertion_date_list = ['25-02-2005', '25-02-2005']
