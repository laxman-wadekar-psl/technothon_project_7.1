import json

def read_json():
    with open('metadata.json', 'r') as f:
        docs = json.load(f)
        for doc in docs:
            print(doc)


read_json()