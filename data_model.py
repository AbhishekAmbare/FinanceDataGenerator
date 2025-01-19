from faker import Faker
import pandas as pd
import random

def generatedata(numofrecords: int):
    fake = Faker()
    df_raw = []
    for _ in range(numofrecords):
        x = {"name":fake.name(), "address":fake.address().replace("\n", ", "), "dob":fake.date()}
        df_raw.append(x)
    df = pd.DataFrame(df_raw)
    df.to_parquet('D:/Data Engineering/dump/fake_persons.parquet', engine="fastparquet")
    

   
def generateFinanceData():
    #read Financial accounts master file
    df_fin = pd.read_csv('D:/Data Engineering/MDM/fin_MD.csv')
    
    #read fake employees generated from generate_fake_employees.py
    df_emp = pd.read_parquet('D:/Data Engineering/dump/employees.parquet')
    
    #define key for joining
    df_emp['key'] = [1 if x == 'sales' else 2 for x in df_emp['department'] ]
    
    #drop duplicates with same name
    df_emp.drop_duplicates(subset=['name'], keep='first', inplace=True)
    
    #join Finance account and Employees who can perform the transaction
    df_cross_fin = df_fin.merge(df_emp, how='inner', left_on='CatKey',right_on='key')
    
    #drop unused columns
    df_cross_fin.drop(columns=['CatKey', 'address','dob','gender','contact_number','key'], inplace=True)
    
    #read customer ID for generating a sold to ID
    cust_DF = pd.read_csv('D:/Data Engineering/MDM/customers_MD.csv')['Customer Id']
    
    #initialize how many records to generate
    to_create = random.randint(df_fin['RecordLRange'].min(),df_fin['RecordURange'].sum())
    
    #get highest index for finance cross master for random selection
    tot_generator = len(df_cross_fin.index)
    
    #get highest index for customer master for random selection
    tot_customer = len(cust_DF.index)
    
    arr = []
    for _ in range(to_create):
        
        #select random Fin data for Employee responsible and account used for transaction
        acc = df_cross_fin.iloc[random.randint(0,tot_generator - 1)]
        
        #generate random dictonary based on random master data selection
        fin_data = {"AccountCode": acc["Code"], "Amount":random.randint(acc["LowerRange"], acc["UpperRange"]), "employee":acc["name"], "customerid":cust_DF.iloc[random.randint(0,tot_customer - 1)]}
        
        #append dict to array for further processing
        arr.append(fin_data)
        
    #convert array to dataframe
    df_toload = pd.DataFrame(arr)
    print(df_toload.count())
    
    #save dataframe to file as parquet
    df_toload.to_parquet('D:/Data Engineering/dump/fake_fin_data.parquet', engine="fastparquet")
    
    
    
def generateFinanceData_by_Path(path: str):
    #read Financial accounts master file
    df_fin = pd.read_csv('D:/Data Engineering/MDM/fin_MD.csv')
    
    #read fake employees generated from generate_fake_employees.py
    df_emp = pd.read_parquet('D:/Data Engineering/dump/employees.parquet')
    
    #define key for joining
    df_emp['key'] = [1 if x == 'sales' else 2 for x in df_emp['department'] ]
    
    #drop duplicates with same name
    df_emp.drop_duplicates(subset=['name'], keep='first', inplace=True)
    
    #join Finance account and Employees who can perform the transaction
    df_cross_fin = df_fin.merge(df_emp, how='inner', left_on='CatKey',right_on='key')
    
    #drop unused columns
    df_cross_fin.drop(columns=['CatKey', 'address','dob','gender','contact_number','key'], inplace=True)
    
    #read customer ID for generating a sold to ID
    cust_DF = pd.read_csv('D:/Data Engineering/MDM/customers_MD.csv')['Customer Id']
    
    #initialize how many records to generate
    to_create = random.randint(df_fin['RecordLRange'].min(),df_fin['RecordURange'].sum())
    
    #get highest index for finance cross master for random selection
    tot_generator = len(df_cross_fin.index)
    
    #get highest index for customer master for random selection
    tot_customer = len(cust_DF.index)
    
    arr = []
    for _ in range(to_create):
        
        #select random Fin data for Employee responsible and account used for transaction
        acc = df_cross_fin.iloc[random.randint(0,tot_generator - 1)]
        
        #generate random dictonary based on random master data selection
        fin_data = {"AccountCode": acc["Code"], "Amount":random.randint(acc["LowerRange"], acc["UpperRange"]), "employee":acc["name"], "customerid":cust_DF.iloc[random.randint(0,tot_customer - 1)]}
        
        #append dict to array for further processing
        arr.append(fin_data)
        
    #convert array to dataframe
    df_toload = pd.DataFrame(arr)
    
    
    #save dataframe to file as parquet
    df_toload.to_parquet(path, engine="fastparquet")
    arr.remove
    return df_toload['AccountCode'].count()