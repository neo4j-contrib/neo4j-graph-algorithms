import requests

print("testing...")

data = {'tag_name': '3.5.0.0'}
response = requests.post("https://api.github.com/repos/neo4j-contrib/neo4j-graph-algorithms/releases", data=data)
