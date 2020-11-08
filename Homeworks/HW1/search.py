import requests
import json

url ='https://world-1c5af.firebaseio.com/.json' 
i_url='https://world-1c5af.firebaseio.com/index.json' 

def search(words):
    response_json = dict(requests.get(url).json()) 
    result = [] 
    output ={} 
    for values in words: 
        temp = requests.get(i_url+values+'/'+'.json').json() 
        if temp is None:
            print ('No Matching Results Found') 
            return
        for val in temp:
            result.append(val)
    
    for values in result:
        output[values] = response_json[values] 
    output = json.dumps(output,indent = 4) 
