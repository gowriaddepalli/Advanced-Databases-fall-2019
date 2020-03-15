from collections import defaultdict

"""
Authors : Sree Gowri Addepalli (sga297)
      
"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# This is a class for the graph data structure for holding all transactions and detecting deadlocks.
# The structure looks like {'1'-> [2,3], '4' -> [5,6]}


class DeadlockDetector:

    def __init__(self):
        self.graph = defaultdict(list)
        # self.v = v
        self.result = set()
        self.isSelfLock = False

    # Add an edge to the adjacency list with vertexes u,v.
    def addTransactionEdge(self, u, v):
        u = int(u)
        v = int(v)
        if u in self.graph:
            if v not in self.graph[u]:
                # for self lock.
                if v == u:
                    self.isSelfLock = True
                    self.graph[u].insert(0,v)
                else:
                    self.graph[u].append(v)
                print("Graph after adding edge is: " + str(self.graph))
                print("\n")

    # Get the neighbours of a Transaction of vertex u.
    def getNeighboursOfATrans(self, u):
        #print("Neighbours: ")
        #print(self.graph)
        u = int(u)
        return self.graph[u]

    # Remove the neighbours of a Transaction with edge u,v.
    def removeNeighbourOfATrans(self, u, v):
        u = int(u)
        v = int(v)
        flag = False
        for i in self.graph[u]:
            if i == v:
                flag = True
                break

        if flag:
            self.graph[u].remove(v)

    # Add a vertex to the graph.
    def addTransactionVertex(self, u):
        u = int(u)
        #print("Vertex Added: " + str(u) +"\n")
        self.graph[u] = []

    # Remove the nodes of a vertex(Transaction) and its corresponding neighbours.
    def removeTransactionVertex(self, u):
        u = int(u)
        flag = False
        for k, v in self.graph.items():
            if u == int(k):
                flag = True
                break
        #print(flag)
        if flag:
            del self.graph[u]
            for vertex in self.graph:
                self.removeNeighbourOfATrans(vertex, u)

    # Detect a cycle.
    def detectCycleUtil(self):
        print("Detecting a cycle \n")
        #visited = [0] * len(self.graph)
        visited = {}
        for l in self.graph.keys():
            visited[l] = 0
        #print("Visited: ")
        #print(visited)
        self.result = set()
        for i in self.graph.keys():
            if visited[i] == 0:
                self.dfs(i, visited)
        #print("iiii")
        #print(list(self.result))
        return list(self.result)
    
    # depth first search for graph traversal to detect deadlock.
    def dfs(self, i, visited):
        i = int(i)
        if visited[i] == 1:
            self.result.add(i)
            return True

        visited[i] = 1
        cyclePresent = False
        for t in self.getNeighboursOfATrans(i):
            cyclePresent = self.dfs(t, visited)
            if cyclePresent:
                if i in self.result:
                    return False
                else:
                    self.result.add(i)
                    return True

        visited[i] = -1
        #print("is Cycle Present ->" + str(cyclePresent))
        return cyclePresent
    
    # represnt
    def __repr__(self):
        return str(self.__dict__)
