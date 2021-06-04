## run API using python env
```
python3 -m  venv env
source env/bin/activate
pip install -r requirements.txt
python final_task.py
```
## build and run docker container
```
docker build -t task_api_image .
docker run -it -p 8080:8080 -t task_api_image
```
## endpoints
**URL** : `/stats/os`  
**Method** : `GET`  
**Auth required** : NO  
**ARGUEMENTS**: start_date, end_date (2014-10-12)
#### example request
```python

import requests

url = "http://0.0.0.0:8080/stats/os?start_date=2014-10-12&end_date=2014-10-12"

payload = ""
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```

**URL** : `/stats/browser`  
**Method** : `GET`  
**Auth required** : NO  
**ARGUEMENTS**: start_date, end_date (2014-10-12)
#### example request
```python

import requests

url = "http://0.0.0.0:8080/stats/browser?start_date=2014-10-12&end_date=2014-10-12"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```

**URL** : `/stats/device`  
**Method** : `GET`  
**Auth required** : NO  
**ARGUEMENTS**: start_date, end_date (2014-10-12)
#### example request
```python
import requests

url = "http://0.0.0.0:8080/stats/device?start_date=2014-10-12&end_date=2014-10-12"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```
## Success Response

**Code** : `200 OK`

**Content example**

```json
{
    "results": [
        {
            "browser": "iPad",
            "percent": 43.205275229357795
        },
        {
            "browser": "iPhone",
            "percent": 22.10656316160903
        },
        {
            "browser": "Mac",
            "percent": 14.875176429075513
        },
        {
            "browser": "Other",
            "percent": 14.544371912491178
        },
        {
            "browser": "Samsung GT-I9505",
            "percent": 0.573394495412844
        }
    ]
}
```
