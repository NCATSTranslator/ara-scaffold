from flask import Flask, request
from flask_restful import Resource, Api
import requests
from contextlib import closing
import json
app = Flask(__name__)
api = Api(app)
import sys
import time
import cProfile
import pstats

@app.route('/')
def hello_world():
    return 'Hello World!'
class QueryGraph (object):
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
        myGraph = self.getGraphDict()
        for key in myGraph.keys():
            if key == id:
                return myGraph[key]
        return {}
    def getPrevious(self,node):
        if isinstance(node, str):
            id = node
        elif isinstance(node, dict):
            id = node['id']
        myGraph = self.getGraphDict()
        prevList = []
        for k,v in myGraph.items():
            for node in v:
                if id ==node['id']:
                    print("found:"+str(v))
                    prevList.append(node)
        print("previous: "+str(prevList))
        return prevList
        #return list(myGraph.keys())[list(myGraph.values()).index(id)]

    def getNextIds(self,node):
        ids = []
        for n in self.getNext(node):
            ids.append(n['id'])
        return ids
    def hasNext(self,node):
        if node['id'] in self.getGraphDict().keys():
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
    def getGraphDict(self):
        return self.__graph
    def getRawGraph(self):
        return {
            "nodes":self.getNodes(),
            "edges":self.getEdges()
        }
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
    def getConnectingEdges(self, node1, node2):
        connectingEdges=[]
        for edge in self.getEdges():
            if edge['source_id']==node1['id'] and edge['target_id']==node2['id']:
                connectingEdges.append(edge)
            if edge['target_id']==node1['id'] and edge['source_id']==node2['id']:
                connectingEdges.append(edge)
        return connectingEdges
class ResponseGraph(object):
    def __init__(self,response):
        self.__querygraph=QueryGraph(response['query_graph'])
        self.__results=response['results']
        self.__knowledgegraph=response['knowledge_graph']
    def getAllValuesForNode(self,queryNode):
        if isinstance(queryNode, str):
            id = queryNode
        elif isinstance(queryNode, dict):
            id = queryNode['id']
        results = self.getResults()
        matchingNodes =[]
        for result in results:
            nodes = result['node_bindings']
            for node in nodes:
                if(id==node['qg_id']):
                    if self.getKgNodeById(node['kg_id']) not in matchingNodes:
                        matchingNode =self.getKgNodeById(node['kg_id'])
                        if not matchingNode is None:
                            matchingNodes.append(matchingNode)
                        else:
                            print(str(node['kg_id'])+" not found in knowledge graph")
        return matchingNodes

    def getKgNodeById(self,id):
        kg = self.getKnowledgeGraph()
        for node in kg['nodes']:
            if node['id']==id:
                return node
        return None
    def getUnknownNodes(self):
        qnodes = self.getQueryGraph().getNodes()
        unknownNodes = []
        for qnode in qnodes:
            values = self.getAllValuesForNode(qnode['id'])
            if values == []:
                unknownNodes.append(qnode)
        return unknownNodes
    def removeResult(self, result):
        results = self.getResults()
        results.remove(result)
    def removeIncompleteResults(self):
        results = self.getResults()
        querySize = len(self.getQueryGraph().getNodes())
        for result in results:
            if len(result['node_bindings'])<querySize:
                self.removeResult(result)

    def removeOrphansFromKg(self):
        kgNodes = self.getKGNodes()
        results = self.getResults()
        newKgNodes=[]
        resultNodeIds=[]
        kgEdges = self.getKGEdges()
        resultEdgeIds=[]
        newKgEdges=[]
        for result in results:
            for node in result['node_bindings']:
                if node['kg_id'] not in resultNodeIds:
                    resultNodeIds.append(node['kg_id'])
            for edge in result['edge_bindings']:
                if edge['kg_id'] not in resultEdgeIds:
                    resultEdgeIds.append(edge['kg_id'])
        for node in kgNodes:
            if node['id'] in resultNodeIds:
                newKgNodes.append(node)
        for edge in kgEdges:
            if edge['id'] in resultEdgeIds:
                newKgEdges.append(edge)
        newKg = {
            "nodes":newKgNodes,
            "edges":newKgEdges
        }
        self.setKnowledgeGraph(newKg)

    #returns a map of kgNodeIds to a list of tuples with the edge id (0) and kgNodeId (1) for edges and nodes connected
    #to the kgNodeId key for a given query node id
    def getConnected(self,qNodeId):
        def updateMap(source,targetTuple):
            if source in qToKMap.keys():
                if targetTuple not in qToKMap[source]:
                    current = qToKMap[source]
                    current.append(targetTuple)
                    qToKMap.update({source:current})
            else:
                qToKMap.update({source:[targetTuple]})
        results = self.getResults()
        qToKMap = {}
        for result in results:
            for node in result['node_bindings']:
                if qNodeId==node['qg_id']:
                    for edge in result['edge_bindings']:
                        kgEdge = self.getKGEdgeById(edge['kg_id'])
                        if kgEdge['source_id']==node['kg_id']:
                            updateMap(kgEdge['source_id'],(kgEdge['id'],kgEdge['target_id']))
        return qToKMap
    def json(self):
        jsonResponse ={
            "query_graph":self.getQueryGraph().getRawGraph(),
            "results":self.getResults(),
            "knowledge_graph":self.getKnowledgeGraph()
        }
        return jsonResponse
    def __str__(self):
        return str(self.json())

    def __dict__(self):
        return {
            "query_graph":self.getQueryGraph().getRawGraph(),
            "results":self.getResults(),
            "knowledge_graph":self.getKnowledgeGraph()
        }


    def setResults(self,results):
        self.__results=results
    def setKnowledgeGraph(self,kg):
        self.__knowledgegraph=kg

    def getQueryGraph(self):
        return self.__querygraph
    def getResults(self):
        return self.__results
    def getKnowledgeGraph(self):
        return self.__knowledgegraph
    def getKGEdges(self):
        return self.getKnowledgeGraph()['edges']
    def getKGNodes(self):
        return self.getKnowledgeGraph()['nodes']
    def getKGEdgeById(self,edge_id):
        kgEdges = self.getKGEdges()
        for edge in kgEdges:
            if edge['id']==edge_id:
                return edge





class Query(Resource):
    def post(self):
        query = request.get_json(force=True)
        responseGraph = ResponseGraph(
            {
                "query_graph":query,
                "results":[],
                "knowledge_graph":{}
            }
        )
        #profile = cProfile.Profile()
        #profile.runcall(self.processQuery(responseGraph))
        #ps = pstats.Stats(profile)
        #ps.print_stats()
        #assembledResponse = self.processNgramQuery(responseGraph)
        assembledResponse = self.processQuery(responseGraph)
        return assembledResponse.json()



    def processQuery(self, responseGraph):
        responseGraph=self.processNgramQuery(responseGraph)
        #print(len(responseGraph.getKGNodes()))
        #self.processOneHopQuery(responseGraph)
        #do other stuff here
        responseGraph = self.processOneHopQueryRecursive(responseGraph)
        return responseGraph
    #todo make this recursive to keep progogating through the QG
    def processOneHopQuery(self,responseGraph):
        gq = responseGraph.getQueryGraph()
        queries=[]
        responses=[responseGraph.json()]
        for node in gq.getNodes():
            nextList = gq.getNext(node['id'])
            for nextNode in nextList:
                if responseGraph.getAllValuesForNode(nextNode)==[]:
                    if 'curie' in node:
                        queries.append(self.createOneHopQuery(node,nextNode,responseGraph))
                    else:
                        for nodeValue in responseGraph.getAllValuesForNode(node):
                            fixedNode = node.copy()
                            fixedNode['curie']=nodeValue['id']
                            fixedNode['name']=nodeValue['name']
                            query = self.createOneHopQuery(fixedNode,nextNode,responseGraph)
                            response = self.queryKnowledgeProviderScaffold(query)
                            print ("RES TYPE: "+str(type(response)))
                            queries.append(self.createOneHopQuery(fixedNode,nextNode,responseGraph))
        for query in queries:
            res = self.queryKnowledgeProviderScaffold(query)
            responses.append(res)
        return self.assembleResponses(responses)
        return responseGraph
    #TODO Change node traversal to be based on a stack.  Current implementation could break with non linear QGs
    def processOneHopQueryRecursive(self,responseGraph, qNode=None):

        #DEBUGGING SECTION
        kg = responseGraph.getKnowledgeGraph()
        results = responseGraph.getResults()
        qg=responseGraph.getQueryGraph()
        #####
        if qNode==None:
            qNode = qg.getNodes()[0]
        nextList = qg.getNext(qNode)

        #base case but might bug with branching graphs
        if len(nextList)==0:
            responseGraph.removeIncompleteResults()
            #removing nodes and edges from our ResponseGraph Knowledge Graph that no longer correspond to a result
            responseGraph.removeOrphansFromKg()
            return responseGraph
        #iterate over all the "next" nodes for our current query node
        for nextNode in nextList:
            if responseGraph.getAllValuesForNode(nextNode)==[]:
                #if we have a pre-defined value for this node, find one-hop results for it, and then eliminate results for
                #the previous node if they don't overlap with the results for the fixed node
                if 'curie' in nextNode:
                    prevList =qg.getPrevious(qNode)
                    #iterating through all the nodes 'previous' to our current node
                    for prevNode in prevList:
                        #query for connections nextNode-->prevNode (backwards)
                        #TODO reevaluate this if we support edge directionality in the future
                        query = self.createOneHopQuery(nextNode,prevNode,responseGraph)
                        prevResponse = ResponseGraph(self.queryKnowledgeProviderScaffold(query))
                        previousResults=prevResponse.getResults()
                        idsOfPrevResults=[] #a set of ids of nodes found in results and bound to our previous node
                        for result in previousResults:
                            nodes=result['node_bindings']
                            for node in nodes:
                                if node['qg_id']==qNode['id'] and node['kg_id'] not in idsOfPrevResults:
                                    idsOfPrevResults.append(node['kg_id'])
                        #go through our full set of (current) results, having been iteratively built and containing incomplete
                        #results (not featuring all query nodes), and eliminate the ones eminating from our previous
                        #node that don't match with our "next" node
                        remainingIds=[]
                        for result in results:
                            nodes=result['node_bindings']
                            for node in nodes:
                                if node['qg_id']==prevNode['id']:
                                    if node['kg_id'] not in idsOfPrevResults:
                                        responseGraph.removeResult(result)
                                    else:
                                        remainingIds.append(node['kg_id'])

                    #Making a new ResponseGraph that is a merge of the next->previous query and our now pruned full set
                    #TODO is this the right way to do this, or are we merging 'bad' results from prevResponse?
                    try:
                        newRG=self.assembleResponses([prevResponse.json(),responseGraph.json()],responseGraph.getQueryGraph().getRawGraph())
                    except:
                        print("bad")
                    #DEBUG
                    newKG = newRG.getKnowledgeGraph()
                    newResults = newRG.getResults()
                    ####
                    return self.processOneHopQueryRecursive(newRG,nextNode)


                #if we don't have a fixed node next, and also don't have any results for the next node, then we need some.
                else:
                        #if our qnode has a curie (is fixed) then we just query that to our next node
                        if 'curie' in qNode:
                            query=self.createOneHopQuery(qNode,nextNode,responseGraph)
                            response = self.queryKnowledgeProviderScaffold(query)
                            responseList = [response,responseGraph.json()]
                            try:
                                assembledResponse = self.assembleResponses(responseList,responseGraph.getQueryGraph().getRawGraph())
                            except:
                                print("bad")
                            newRG=assembledResponse
                            return self.processOneHopQueryRecursive(newRG,nextNode)
                        #otherwise, we need to get all the values for our qnode and query each of those to our next node
                        else:
                            newRG=responseGraph
                            for nodeValue in responseGraph.getAllValuesForNode(qNode):
                                fixedNode = qNode.copy()
                                fixedNode['curie']=nodeValue['id']
                                fixedNode['name']=nodeValue['name']
                                query = self.createOneHopQuery(fixedNode,nextNode,responseGraph)
                                response = self.queryKnowledgeProviderScaffold(query)
                                if len(response)>0: #might not get results for all; skip the ones we don't
                                    newRG=self.assembleResponses([response,responseGraph.json()],responseGraph.getQueryGraph().getRawGraph())
                            return self.processOneHopQueryRecursive(newRG,nextNode)
            else:
                #TODO I'm unsure this will hold with non-linear graphs
                return self.processOneHopQueryRecursive(responseGraph,nextNode)

    def createOneHopQuery(self,fixedNode,unknownNode,responseGraph):
        nodes = [fixedNode,unknownNode]
        edges=responseGraph.getQueryGraph().getConnectingEdges(fixedNode, unknownNode)
        query = {
            "nodes":nodes,
            "edges":edges
        }
        return query

    def processNgramQuery(self, responseGraph):
        queryGraph = responseGraph.getQueryGraph()
        lockedNodes = queryGraph.getLockedNodes()
        lockedPairs =list(zip(lockedNodes, lockedNodes[1:] + lockedNodes[:1]))
        del lockedPairs[-1] #removing 'wrap-around' of pairs, find a better way to do this
        responses = []
        for pair in lockedPairs:
            if len(queryGraph.getPath(pair[0]['id'],pair[1]['id']))==3:
                responses.append(self.queryNgram(self.createNgramQuery(pair, queryGraph)))
        if responses == []:
            return responseGraph
        else:
            return self.assembleResponses(responses,queryGraph.getRawGraph())





    def getTestResponses(self):
        with open("/Users/williamsmard/Software/ara-scaffold/response0.json") as json_file:
            response0= json.load(json_file)
        with open("/Users/williamsmard/Software/ara-scaffold/response1.json") as json_file:
            response1= json.load(json_file)
        responses=[response0,response1]
        return responses
    def assembleResponses(self,responses,query):
        if len(responses)==1:
            return ResponseGraph(responses[0])
        edges = []
        nodes =[]
        #qedges=[]
        #qnodes=[]
        results=[]
        for i in range(len(responses)):
            #for qedge in responses[i]['query_graph']['edges']:
            #    if qedge not in qedges:
            #        qedges.append(qedge)
            #for qnode in responses[i]['query_graph']['nodes']:
            #    if qnode not in qnodes:
            #        qnodes.append(qnode)

            #print("subresults: "+str(len(responses[i]['results'])))

            for firstResult in responses[i]['results']:
                if i==0:
                    j=1
                elif j<len(responses)-1:
                    j+=1
                else:
                    break
                firstNodes = firstResult['node_bindings']
                firstEdges = firstResult['edge_bindings']
                if len(responses[j]['results'])==0:
                    results=responses[i]['results']
                for secondResult in responses[j]['results']:
                    secondNodes=secondResult['node_bindings']
                    secondEdges=secondResult['edge_bindings']
                    newNodeBindings =[]
                    newEdgeBindings=[]
                    overlap = False
                    for firstNode in firstNodes:
                        for secondNode in secondNodes:
                            if firstNode==secondNode:
                                overlap=True
                                #print("Overlap between " +str(firstNode) +" and "+str(secondNode) )
                            if firstNode not in newNodeBindings:
                                #previously appended names for readability, removed for now
                                #name = list(filter(lambda n: n['id'] == secondNode['kg_id'], responses[j]['knowledge_graph']['nodes']))[0]['name']
                                #secondNode.update({"name":name})
                                newNodeBindings.append(firstNode)
                            if secondNode not in newNodeBindings:
                                newNodeBindings.append(secondNode)
                    if overlap:
                        for fedge in firstEdges:
                            for sedge in secondEdges:
                                if fedge not in newEdgeBindings:
                                    newEdgeBindings.append(fedge)
                                if sedge not in newEdgeBindings:
                                    newEdgeBindings.append(sedge)
                        if len(newNodeBindings)>len(query['nodes']):
                            print('stop')
                        results.append(
                            {
                                "node_bindings":newNodeBindings,
                                "edge_bindings":newEdgeBindings
                            }
                        )

            if responses[i]['knowledge_graph'].keys() <{'edges','nodes'}:
                continue #skip this if the knowledge graph is blank
            for edge in responses[i]['knowledge_graph']['edges']:
                if edge not in edges:
                    edges.append(edge)
            for node in responses[i]['knowledge_graph']['nodes']:
                if node not in nodes:
                    nodes.append(node)
        #queryGraph={
        #    "nodes":qnodes,
        #    "edges":qedges
        #}
        knowledgeGraph={
            "nodes":nodes,
            "edges":edges
        }
        assembledResponse ={
            "query_graph":query,
            "results":results,
            "knowledge_graph":knowledgeGraph
        }
        finalAnswer = ResponseGraph(assembledResponse)
        return finalAnswer

    def queryNgram(self,query):
        #url ='http://transltr.io:7072/query'
        url = 'http://localhost:7072/query'
        with closing(requests.post(url, json=query, stream=False)) as response:
            try:
                return json.loads(response.text)
            except Exception as ex:
                print("status " +str(response.status_code))
                print("text "+response.text)

    def queryKnowledgeProviderScaffold(self,query):
        #url ='http://transltr.io:7072/query'
        url = 'http://localhost:7072/query'
        try:
            with closing(requests.post(url, json=query, stream=False)) as response:
                if response.status_code==200:
                    return response.json()
                else:
                    print(str(response.status_code))
                    return {}
        except Exception as ex:
            print("error "+str(sys.exc_info()[0]))
            print("type "+str(type(ex)))
            print("query "+str(query))

    def createNgramQuery(self,pair,graph):
        intermediaryNodes=graph.getIntermediaryNodes(pair[0]['id'],pair[1]['id'])
        middleNode=intermediaryNodes[len(intermediaryNodes)//2]


        nodes=[pair[0],middleNode,pair[1]]
        edges=[]
        graphEdges =graph.getEdges()

        for edge in graphEdges:
            if(pair[0]['id'] in edge.values() or pair[1]['id'] in edge.values()) and middleNode['id'] in edge.values():
                edges.append(edge)

        query = {
            "nodes":nodes,
            "edges":edges
        }

        return query



api.add_resource(Query,'/query')
if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port='7073',
        debug=True
    )
