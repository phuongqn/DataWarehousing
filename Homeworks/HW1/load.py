import pandas as pd
import json
import requests
import numpy as np

db_names= [ 'city', 'country', 'countrylanguage']
def get_db (file_path):
    d = pd.read_csv(file_path, error_bad_lines=False, encoding='latin1')
    return d
def clean_d(data):
    data.columns=data.columns.str.strip()
#     data.columns=data.columns.str.replace("# ", "")
    for column in data.columns:
        data[column]=data[column].str.strip()
        data[column]=data[column].str.replace("'","")
        data[column]=data[column].str.replace("NULL","NaN")
    return data
d= clean_d(get_db('/Users/phuongqn/Desktop/INF551/countrylanguage.csv'))
d1=clean_d(get_db('/Users/phuongqn/Desktop/INF551/country.csv'))
d2=clean_d(get_db('/Users/phuongqn/Desktop/INF551/city.csv'))
d = d.to_json(orient='records')
d1 = d1.to_json(orient='records')
d2 = d2.to_json(orient='records')
response = requests.put('https://world-1c5af.firebaseio.com/countrylanguage.json' ,json=d)
response = requests.put('https://world-1c5af.firebaseio.com/country.json' ,json=d1)
response = requests.put('https://world-1c5af.firebaseio.com/city.json' ,json=d2) 

new_dict= {}
new_dict['countrylanguage']= json.loads(d)
new_dict['country'] = json.loads(d1)
new_dict['city']= json.loads(d2)

for name, table in new_dict.items():
    for rec in table: 
        for key, val in rec.items():
            if "." in val:
                try: 
                    rec[key] = float(val)
                except:
                    pass
            else:
                try:
                    rec[key] = int(val)
                except:
                    pass
            
for name, table in new_dict.items():
    s = set( val for dic in table for val in dic.values())
    unique=[]
    punctuation = '''`~!@#$%^&*(){}[];:'".,/\\?´©±¡³¼¤º°''' 
    for i in s:
        if type(i)==str and len(i.strip()) >0 :
            if "-" in i:
                i = i.replace("-", " ")
            for char in i:
                if char in punctuation: 
                    i = i.replace(char,"")
            
            sp = i.split()
            for w in sp:
                unique.append(w.lower())
        else:
            pass

inv_index = {}
for word in unique:
    word_idx=[]
    for name, table in new_dict.items():
        attribute = None
        if name =='countrylanguage':
            attribute ='# CountryCode'
        elif name =='country':
            attribute ='# Code'
        elif name =='city':
            attribute ='# ID'
     
        for rec in table:
            for key, val in rec.items():
                if type(val) == str:
                    if word in val.lower():
                        word_idx.append({"TABLE": name, "COLUMN": key, attribute: rec[attribute]})
                    else:
                        pass
                else:
                    pass
        inv_index[word] = word_idx

requests.put('https://world-1c5af.firebaseio.com/index.json',inv_index) 
resp = dict(requests.get('https://world-1c5af.firebaseio.com/index.json').json()) 
print( json.dumps(resp,indent=2))              
        
                       
