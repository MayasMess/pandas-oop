![image](static/images/poop_sticker.png)
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
DB_CONNECTION = models.Connection('sqlite:///pandas_oop.db')
```
```python
@models.sql(table='people', con=DB_CONNECTION)
@models.Data
class People(models.DataFrame):
    name = models.StringColumn(unique=True)
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
or
people = People(from_df=some_dataframe)
```

![image](static/images/df.png)

You can also save it to the database with the save() method (if the dtypes of the columns change, this will raise a 
ValidationError):

```python
people.save()
```

You can upsert to the database and this will automatically look at the unique fields that were declared in the class

```python
people.save(if_row_exists='update')
or
people.save(if_row_exists='ignore')
```

If you want to revalidate your dataframe (convert the columns dtypes to the type that was declared in the class), you can 
call the validate() method:

```python
people.validate()
```

You can also validate from another class. For example, you can do something like this:  

```python
people = People(from_csv=DATA_FILE)
jobs = Jobs(from_sql_query='select * from jobs')
people_with_jobs = people.merge(jobs, on='name').validate(from_class=PeopleWithJobs)
```

This is the list of the overriten methods that return a pandas_oop custom dataframe
- 'isnull'
- 'head'
- 'abs'
- 'merge'

I will add more and more methods on this list.