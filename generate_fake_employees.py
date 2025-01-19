from faker import Faker
import pandas as pd
import random

gender = ('m', 'f')
departments = ('sales', 'expenses')

fake = Faker()
df_raw = []
for _ in range(500):
    x = {"name":fake.name(), "address":fake.address().replace("\n", ", "), "dob":fake.date(), "gender":gender[random.randint(0,len(gender)-1)], "contact_number":fake.phone_number(), "department":departments[random.randint(0,len(departments)-1)]}
    df_raw.append(x)
df = pd.DataFrame(df_raw)
df.to_parquet('D:/Data Engineering/dump/employees.parquet', engine="fastparquet")