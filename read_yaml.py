import yaml

def read_yaml():
    with open('dit.yaml','r') as f:
        docs = yaml.safe_load_all(f)
        for doc in docs:
            for k, v in doc.items():
                print(k, "->", v)

read_yaml()
