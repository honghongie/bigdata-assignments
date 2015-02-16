####
Question 0. Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length.
####
#####
At first, I thought the formula could be transformed like PMI(x,y)=-(log(p(x)p(y)/p(x,y)))=-log(p(x)/p(x,y))-log(p(y)/p(x,y)), so I use two consecutive map reduce jobs to accomplish that. 

In pairs implementation, in the first job I get the unique characters, and then got value of pairs like (x,y)=-log(p(x)/p(x,y)), (y,x)=-log(p(y)/p(y,x)), result of the first job will be transferred to the second job, and in the second job, in map stage, pairs are sorted so (x,y) and (y,x) will have the same key (x,y), in the reduce stage pairs with the same key will be summed.

In stripes implementation, in the first job I will get concurrence words set for each word, * to represent itself. Still values of the first job will be transferred to the second job. In the map stage of second job, I will get values for each pair for example (x,y)=-log(p(x)/p(x,y)), (y,x)=-log(p(y)/p(y,x)), in reducer stage two pairs with the same element will be summed. 

Then I realize the formula is wrong. For the correct version, for both pairs and stripes implementation I use two independent jobs. 

In pairs implementation, in the first job I count number of lines each word appears and save the result in a temp file. In the second job, first in mapper stage I count the pairs, then in reduce stage I use cache to load results of the first job and  save them in a map, then for each pairs I get sum of pair frequency and get frequency of single element from the map, use the formula to calculate PMI.

In stripes implementation, use first job to calculate frequency of single words and save to a temp file. In the second job, use HMapStIW to count concurrence words set for each word. In the reduce part, I use map.toString to decompose map and get value for pairs. But there is a problem, when running on bible dataset the result will be fine but have some error in the big wiki dataset. So I change to use keyset to solve this problem.
#####
####
Question 1. What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? (Did you run this in your VM or on the UMIACS cluster? Either is fine, but tell me which one.)
####
```
Run on my VM.
Pairs: 208.546 seconds
Stripes: 104.801 seconds
```
####
Question 2. Now disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation? (Did you run this in your VM or on the UMIACS cluster? Either is fine, but tell me which one.)
####
```
Run on my VM.
Pairs:272.265 seconds
Stripes: 148.057 seconds
```
####
Question 3. How many distinct PMI pairs did you extract?
####
```
In the correct version there will be duplicates because PMI(x,y)=PMI(y,x), 
so if (x,y) and (y,x) are two distinct pairs the pairs is 233518, 
if they are treated as one pair then distinct pairs is 116759.
```
####
Question 4. What's the pair (x, y) with the highest PMI? Write a sentence or two to explain what it is and why it has such a high PMI.
####
```
Highest pair is (meshach, shadrach) : -1.146128. High PMI means they usually appear 
together in a line. As wikipedia says Shadrach, Meshach, and Abednego are biblical 
characters in the book of Daniel chapters 1–3. They are depicted as being saved by
divine intervention from the Babylonian execution of being burned alive in a fiery furnace.
```
####
Question 5. What are the three words that have the highest PMI with "cloud" and "love"? And what are the PMI values?
####
```
Except duplicate pairs,
the max words relates to love is: 
--1: hate,love : -4.075182 
--2: love,hermia : -4.312543 
--3:loved,love : -4.4553933
the max words relates to cloud is: 
--1: cloud,tabernacle : -3.390087 
--2: night,cloud : -4.0125 
--3:came,cloud : -4.246474
```

####
Question 6. What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? (Did you run this in your VM or on the UMIACS cluster? Either is fine, but tell me which one.)
####
Run on the UMIACS cluster.
Pairs Implementation:
Stripes Implementation: 2743.638 seconds

####
Question 7. Now disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation? (Did you run this in your VM or on the UMIACS cluster? Either is fine, but tell me which one.)
####
####
Question 8. How many distinct PMI pairs did you extract?
####
```
38210424

```
####
Question 9. What's the pair (x, y) with the highest PMI? Write a sentence or two to explain what it is and why it has such a high PMI.
####
```
((120 hp),88 kW) : -1.0
```
####
Question 10. What are the three words that have the highest PMI with "cloud" and "love"? And what are the PMI values?
####
```
the max words relates to love is: 
--1: love,madly : -3.048442 
--2: Love,Dangerously : -3.0819545 
--3:Oliver!,Love : -3.0865023
the max words relates to cloud is: 
--1: Cloud,Magellanic : -2.3422582 
--2: cloud,nebula. : -2.5360992 
--3:thunderstorm,cloud : -2.788875
```
