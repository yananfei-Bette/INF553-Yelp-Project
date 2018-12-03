import requests
import json
import csv

categories = []
past = 34551
count = 5000
with open('vegas_business_id.txt') as f:
    for line in f:
        if past > 0:
            past -= 1
            continue
        if count <= 0:
            break
        count -= 1
        url = "https://api.yelp.com/v3/businesses/" + str(line).strip('\n')
        data = requests.get(url, headers={"Content-Type":"application/json", "Authorization":"Bearer S-JUPIpbZ8iMvlOmRit6sppDAahSBPplANR3JhjDfT4fTsgSgoCpmJXZHYyizzaWk0WbBkWju90s9MuIJEORj4aCeEPdCsyii76x5a7lBijnzZ_fEa2HgFXqy7kDXHYx"}).json()
        if 'categories' not in data:
            print(url)
            print(data)
            continue
        cat = [o['title'] for o in data['categories']]
        categories.append((data['id'], cat))

with open('biz_categories_8.csv', 'w') as csvfile:
    for o in categories:
        csvfile.write(','.join([o[0]] + o[1]) + '\n')
