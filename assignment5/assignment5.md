###
1.What is the scalability issue with this particular HBase table design?

###

####
HBase doesn’t limit the number of column qualifiers so for a word which has extremely high document frequency, like “the”, number of column qualifiers will be a lot. Because they have the same family so they should stores together which is costly in terms of storage. If data grows too big, it exceeds capacity of memStore. 

####

###
2.How would you fix it? You don't need to actually implement the solution; just outline the design of both the indexer and retrieval engine that would overcome the scalability issues in the previous question.

###

####
One way to fix it is to hash documents to different tables, which on one hand avoids overflow of memStore, on the other hand distributes data uniformly across cluster nodes. For example if we spit the original table to four different tables, when inserting document id and term frequency for one word, mod document id first and decide which table to insert according to mod results. After TableReducer stages we will get four index tables. In retrieval stage, document ids are fetched from four tables in a loop according to queries and then aggregated for fetching text from document collections. But it may has the problem of in consistency. 

####
