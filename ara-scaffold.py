from flask import Flask, request
from flask_restful import Resource, Api
import requests
import json
app = Flask(__name__)
api = Api(app)

@app.route('/')
def hello_world():
    return 'Hello World!'
class Graph (object):
    def __init__(self,query):
        graph={}
        if query == None:
            return None
        self.__nodes = query['nodes']
        self.__edges = query['edges']
        for edge in self.__edges:
            source = tuple(self.getNodeFromQueryById(self.__nodes, edge['source_id']).items())
            print("source = "+str(type(source)))

            target = self.getNodeFromQueryById(self.__nodes, edge['target_id'])
            target['edge_id']=edge['id']
            if source not in graph.keys():
                print("added new node "+str(source))
                graph[source]=[target]
                print("graph is now: " +str(graph))
            elif source in graph.keys():
                print("appended"+target['id']+ "to "+str(source))
                graph[source]=graph[source].append(target)
                print("graph is now: " +str(graph))
            else:
                print("something has gone wrong "+str(source)+ " is neither in nor out of the graph")
        self.__graph = graph
    def getNodeFromQueryById(self, nodes, id):
        for node in nodes:
            if node['id']==id:
                return node
        #return {} #is returning blank when none are found best?
    def next(self,node):
        id = node['id']
        for key in self.__graph.keys():
            dictKey = dict(key)
            if dictKey['id'] == id:
                return self.__graph[key]
        return []

    def getNodeById(self,nodeId):
        for node in self.__nodes:
            if node['id'] == nodeId:
                return node
        return {}
    def getEdgeById(self,edgeId):
        for edge in self.__edges:
            if edge['id'] == edgeId:
                return edge
        return {}
    def getGraph(self):
        return self.__graph
    def distanceToNextSpecifiedNode(self,node,i=1):
        nextNodes = self.next(node)
        for nextNode in nextNodes:
            if 'curie' not in nextNode:
                i+=1
                return self.distanceToNextSpecifiedNode(nextNode,i)
            else:
                return i
        return i

class Query(Resource):
    def post(self):
        query = request.get_json(force=True)
        #query = json.loads(open('ngramQuery.json').read())
        graph = Graph(query)
        self.processQuery(graph)
    def processQuery(self,graph):
        graphMap = graph.getGraph()
        queryGraphs=[]
        for node in graphMap.keys():
            dictNode = dict(node)
            distance = graph.distanceToNextSpecifiedNode(dictNode)


        return graph


api.add_resource(Query,'/query')
if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port='7072',
        debug=True
    )
