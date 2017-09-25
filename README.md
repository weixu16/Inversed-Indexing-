# Inversed-Indexing-MapReduce

###### Inversed Indexing Definition**

Inversed indexing is a batch processing to sort documents for each popular keyword, so that it makes easier and faster for document-retrieval requests to find documents with many hits for a set of user-given keywords. 

###### Hadoop and MapReduce Installation

I installed the Hadoop File System on the cluster of 4 machines for testing on an actual cluster.

###### Implementation Details

Map function: read from one line, and find the corresponding words from the args. Each words in the args be a list of keys, and if one of the keys been found, the corresponding filename and 1 be the value. 
Reduce function: Create a hash map. For particular keys, gather all the values together according to the filename. Put the values in the hash map.
Combine function: Combine the values in one node before reduce, which can reduce the memory usage.

###### Performance Evaluation

Adding a combine function, the memory usage is reduced by 2%; When the 1 node change to 4 nodes, the performance is not improved. Because the work load is small. According to https://wiki.apache.org/hadoop/HowManyMapsAndReduces, the map and reduce task will not be divided inside. It will just run in one node. So I forced the map and reduce divided into 4 tasks; If the tasks are forced to divided by 4, the time is increased. The heap usage is increased, too.  I think there are lot of overheads in network communication between each node.

