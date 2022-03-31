![image](static/images/poop.png)
# Pandas-Oop
(Also known as Poop), is a package that uses Pandas dataframes with object oriented programming style

Installation:
- 

```shell script
  pip install pandas-oop
```
Some examples
-

```python
from pandas_oop import models
```

```python
@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class People(models.DataFrame):
    name = models.StringColumn()
    age = models.IntegerColumn()
    money = models.FloatColumn()
    insertion_date = models.DateColumn(format='%d-%m-%Y')
    is_staff = models.BoolColumn(true='yes', false='no')
```

Now when instantiating this class, it will return a custom dataframe with all the functionalities of a Pandas
dataframe and some others

```python
people = People()
or
people = People(from_csv=DATA_FILE, delimiter=";")
or
people = People(from_sql_query='select * from people')
```

![image](static/images/df.png)

You can also save it to the database with the save() method (if the dtypes of the columns change, this will raise a 
ValidationException):

```python
people.save(if_exists='append', index=False)
```

If you want to revalidate your dataframe (convert the columns dtypes to the type that was declared in the class), you can 
call the validate() method:

```python
people.validate()
```
Coming soon
-
functions that are supported on the major databases (sqlite, postgres, oracle, mysql)
- save or update on duplicate
- save or do nothing on duplicate