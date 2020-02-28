from flask import Flask, request
from flask_restful import Resource, Api
import requests
from contextlib import closing
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
        for edge in self.getEdges():
            source = edge['source_id']
            target = self.getNodeFromQueryById(self.getNodes(), edge['target_id'])
            target['edge_id']=edge['id']
            if source not in graph.keys():
                #print("added new node "+str(source))
                graph[source]=[target]
                #print("graph is now: " +str(graph))
            elif source in graph.keys():
                #print("appended"+target['id']+ "to "+str(source))
                graph[source]=graph[source].append(target)
                #print("graph is now: " +str(graph))
            else:
                print("something has gone wrong "+str(source)+ " is neither in nor out of the graph")
        self.__graph = graph
    def getNodeFromQueryById(self, nodes, id):
        for node in nodes:
            if node['id']==id:
                return node
        return {} #is returning blank when none are found best?
    def getNext(self,node):
        #Is this the right way to overload in Python?
        if isinstance(node, str):
            id = node
        elif isinstance(node, dict):
            id = node['id']
        myGraph = self.getGraph()
        for key in myGraph.keys():
            if key == id:
                return myGraph[key]
        return []
    def getNextIds(self,node):
        ids = []
        for n in self.getNext(node):
            ids.append(n['id'])
        return ids
    def hasNext(self,node):
        if node['id'] in self.getGraph().keys():
            return True
        else:
            return False

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
    def getNodes(self):
        return self.__nodes
    def getEdges(self):
        return self.__edges

    def distanceToNextSpecifiedNode(self,node,i=1):
        nextNodes = self.getNext(node)
        for nextNode in nextNodes:
            if 'curie' not in nextNode:
                i+=1
                return self.distanceToNextSpecifiedNode(nextNode,i)
            else:
                return i
        return i

    def getLockedNodes(self):
        nodes = self.getNodes()
        lockedNodes = []
        for node in nodes:
            if 'curie' in node:
                lockedNodes.append(node)
        return lockedNodes
    def getPath(self,node1,node2,path=None):
        print("node 1: "+node1+" node2: "+node2+" path: "+str(path))
        if path is None:
            path = [node1]
        if node1==node2:
            return path
        for node in self.getNextIds(node1):
            if node not in path:
                path.append(node)
            return self.getPath(node, node2, path)

    def getIntermediaryNodes(self,node1,node2):
        path = self.getPath(node1,node2)
        path.remove(node1)
        path.remove(node2)
        nodes=[]
        for id in path:
            nodes.append(self.getNodeById(id))
        return nodes

class Query(Resource):
    def post(self):
        query = request.get_json(force=True)
        graph = Graph(query)
        self.processNgramQuery(graph)

    def processQuery(self,graph):
        lockedNodes = graph.getLockedNodes()
        if len(lockedNodes) >= 2:
            print("filling")
            self.ngramFill(lockedNodes, graph)
        else:
            print(str(len(lockedNodes)))
        graphMap = graph.getGraph()
        queryGraphs=[]
        for nodeId in graphMap.keys():
            node = graph.getNodeById(nodeId)
            distance = graph.distanceToNextSpecifiedNode(node)
        return graph

    def processNgramQuery(self,graph):
        lockedNodes = graph.getLockedNodes()
        lockedPairs =list(zip(lockedNodes, lockedNodes[1:] + lockedNodes[:1]))
        del lockedPairs[-1] #removing 'wrap-around' of pairs, find a better way to do this
        responses = []
        for pair in lockedPairs:
            responses.append(self.queryNgram(self.createNgramQuery(pair,graph)))
        return self.assembleResponses(responses,graph)

    def assembleResponses(self,responses,graph):
        edges = []
        nodes =[]
        qedges=[]
        qnodes=[]
        queryGraph={}
        for response in responses:
            edges.extend(response['knowledge_graph']['edges'])
            nodes.extend(response['knowledge_graph']['nodes'])
            qedges.extend(response['query_graph']['edges'])
            qnodes.extend(response['query_graph']['nodes'])

    def queryNgram(self,query):
        #url ='http://transltr.io:7072/query'
        url = 'http://localhost:7072/query'
        with closing(requests.post(url, json=query, stream=False)) as response:
            return json.loads(response.text)

    def createNgramQuery(self,pair,graph):
        intermediaryNodes=graph.getIntermediaryNodes(pair[0]['id'],pair[1]['id'])
        middleNode=intermediaryNodes[len(intermediaryNodes)//2]


        nodes=[pair[0],middleNode,pair[1]]
        edges = [
            {
                "id":"e00",
                "source_id":nodes[0]['id'],
                "target_id":nodes[1]['id']
            },
            {
                "id":"e01",
                "source_id":nodes[1]['id'],
                "target_id":nodes[2]['id']
            }
        ]
        query = {
            "nodes":nodes,
            "edges":edges}

        return query



api.add_resource(Query,'/query')
if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port='7073',
        debug=True
    )
