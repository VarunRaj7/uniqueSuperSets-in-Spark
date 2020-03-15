# uniqueSuperSets-in-Spark
This code gives out the uniqueSuperSets in a given list of transaction of Items.

# Table of Contents
1. [Algorithm Flow](#Algorithm-Flow)
2. [Classes and Functions used in Code](#Classes-and-Functions-used-in-Code)
3. [How to use the code](#How-to-use-the-code)
4. [Verification](#Verification)
5. [Time Comparision of SparkAlgo Vs NaiveAlgo](#Time-Comparision-of-SparkAlgo-Vs-NaiveAlgo)

## Algorithm Flow


#### Constraints:

1. An order to the items can be defined, that is, either by alphabetically or by support. This algorithm deals when items are numbers and natural ordering of numbers are treated.
2. Each transaction is unique. In other words there are no duplicates.
3. No Item is present in every transaction. If it does it is highly likely that data will get highly skewed towards it and needs more computation.

#### Algorithm Main Theme:

The main theme of algorithm is to find the unique supersets by item wise, which eventually gives you the global unique supersets. This is explained with the following examples.

**Algorithm with Example-1:**

NOTE: Though this example may violate the constraint 3, that the Item number 5 appears in every transaction.  

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
| 1 2 3 5  |     #1     |     4       |
| 1 4 5    |     #2     |     3       |
| 2 5      |     #3     |     2       |
| 2 3 5    |     #4     |     3       |
| 4 5      |     #5     |     2       |

**How UniqueId is generated given space separated list of items**

.hashCode() function is used to generate UniqueId for a given list of items.

For example: 

      val s = "1 2 3 5"
      val UniqueId = s.hashCode()
      println("UniqueId for 1 2 3 5:",UniqueId)
      // UniqueId for 1 2 3 5: 1501319659

.hashCode() working:

    public int hashCode() {
        int h = hash;
        if (h == 0 && value.length > 0) {
            char val[] = value;

            for (int i = 0; i < value.length; i++) {
                h = 31 * h + val[i];
            }
            hash = h;
        }
        return h;

From the above code, it is evident that given a list of items as space separated will generated uniqueId irrespective of distributed execution. 

**Note: You may try to use any other way to generate uniqueId. When you use different approach to identify each row make sure it does not vary when executed multiple times on different machines. hashCode() returns same value for a given string on multiple machines. However, it is also possible to get same hashCode() values for two different strings (common example of "FB" and "Ea") but it is less likely to happen in this case. This is can be considered as a minor issue in implementation, which will be resolved in new version that will be out soon while keeping the core idea same.**

**Expected Result for uniqueSuperSets:**

|  Items   | 
|----------|
| 1 2 3 5  |
| 1 4 5    |

#### Algo

Collecting all the suffixes with Item "1" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  2 3 5   |     #1     |     4       |
|  4 5     |     #2     |     3       |

From the above we see that both are unique hence return uniqueId #1 and #2.

1 => [#1, #2]

Collecting all the suffixes with Item "2" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  3 5     |     #1     |     4       |
|  5       |     #3     |     2       |
|  3 5     |     #4     |     3       |

Clearly, from the above we see that #3 is subset of other two transactions as the Items in #3 are "2 5".

What about #1 and #4?

#1 is superset of #4 because we can say that the #4 consists of Items "2 3 5" where as #1 has items "_ 2 3 5" using the information from the ItemsSize.

2 => [#1]

Collecting all the suffixes with Item "3" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  5       |     #1     |     4       |
|  5       |     #4     |     3       |


Using the ItemsSize information it is equally likely two possible situations arise, that is, the #4 is not a subset of #1 and #4 is a subset of #1. However, it will make a locally optimal decision that it returns #1 by assuming #4 is subset of #1. If #4 is not subset of #1 it will be dealt by the items in before positions.

3 => [#1]

Collecting all the suffixes with Item "4" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  5       |     #2     |     3       |
|  5       |     #5     |     2       |

From, the above it is clear that the #5 is subset of #2 because:

we know that there are only two items in #5 that are "4 5" but #2 has three items "_ 4 5". 

Collecting all the suffixes with Item "5" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  ()      |     #1     |     4       |
|  ()      |     #2     |     3       |
|  ()      |     #3     |     2       |
|  ()      |     #4     |     3       |
|  ()      |     #5     |     2       |

From, the above it is clear that the Item "5" is the last item in all the transactions. We are not sure of which is subset of which but locally with the given information of ItemsSize we can say that #1 is more likely to be a SuperSet.

5 => [#1]

Now collect all the uniqueIds returned by each keys. That is [#1, #2]

Hence, the uniqueSuperSets are the transactions with uniqueId are [#1, #2]

Algorithm with Example-2:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|  1 3 4   |     #1     |     3       |
|  2 3 4   |     #2     |     3       |


Collecting all the suffixes with Item "1" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|   3 4    |     #1     |     3       |

1 => [#1]

Collecting all the suffixes with Item "2" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|   3 4    |     #2     |     3       |

2 => [#2]

Collecting all the suffixes with Item "3" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|    4     |     #1     |     3       |
|    4     |     #2     |     3       |

Both the #1 and #2 have same size of 3 and we know two items for sure are "3 4". Hence, using the constraint 2, that is, no duplicate transactions are allowed we return both #1 and #2.

3 => [#1, #2]

Collecting all the suffixes with Item "4" as prefix:

|  Items   |  UniqueId  |  ItemsSize  |
|----------|------------|-------------|
|    ()    |     #1     |     3       |
|    ()    |     #2     |     3       |

Both the #1 and #2 have same size of 3 and we know one item for sure is "4". Hence, using the constraint 2, that is, no duplicate transactions are allowed we conclude that even though these two of same size(n) and we expect locally that at most n-1 are same but not n items are same. So, we return #1 and #2.

4 => [#1, #2]

UniqueIds returned by every Item as key we get #1 and #2. 

## Classes and Functions used in Code

ItemHash Class:

Creating a ItemHash Class to hold the information of Items, UniqueId and Items Size.  

      Items: is      => Stored as immutable.TreeSet[Int]
      UniqueId: hv  => Stored as Int
      Items Size:iss => Stroed as Int

      This class has two more methods:

      --(i:immutable.TreeSet[Int]):ItemHash
        
       This function returns a new ItemHash with 
       the "i" items discarded while preserving the
       UniqueId and size of the original.
       
      and

      compare(that:ItemHash):Int
      
       This function is required to 
       compare two ItemHash class objects and
       order them first by decreasing iss (size)
       and then by increasing hv (Hash Value).	
       
seqOp function       
      
      seqOp(elemSet:mutable.TreeSet[ItemHash], elemx:ItemHash):mutable.TreeSet[ItemHash]
    
      This function is used with the aggregateByKey as a function that applies on the values
      with same key within the partition. It will take a elemSet:mutable.TreeSet[ItemHash] 
      and a elemx:ItemHash. And, returns a modified elemSet:mutable.TreeSet[ItemHash].
    
     
     
      Firstly, create a flag variable to add the elemx to elemSet as true.
     
      Then, this function checks for an "elem"(element in elemSet):
       if elemx's items are equal to elem's items{
       	=> if yes check:
                  if elemx's size > elem's size {
                  => if yes do:
                        {
                        remove the elemx from the elemSet 
                        and break here to add elemx to elemSet
                        }
                  if elemx's size < elem's size{
                  => if yes do:
                        {
                        change flag to false as need not to be added to elemSet
                        and break here 
                        }
        => if no check:
              if elemx's items subset of elem's items {
               => if yes do:
                  {
                  change flag to false as need not to be added to elemSet
                  and break here 
                  }
                  if elem's items subset of elemx's items {
                  => if yes do:
                        {
                        remove the elemx from the elemSet
                        }	
       }
      
       Finally, add elemx to elemSet if flag is true.

combOp function

      combOp(elemSet:mutable.TreeSet[ItemHash], elemx:ItemHash):mutable.TreeSet[ItemHash]
    
      This function is used with the aggregateByKey as a function that applies on the
      values with same key that are resultant of seqOp of two partitions. It will take a 
      elemSet1:mutable.TreeSet[ItemHash] and a elemSet2:mutable.TreeSet[ItemHash]. 
      And, returns a modified elemSet1:mutable.TreeSet[ItemHash].
    
     
     
       This function checks for an "elem1"(element in elemSet1) with "elem2" (element in elemSet1):
       if elem2's items are equal to elem1's items{
      	=> if yes check:
                  if elem2's size > elem1's size {
                  => if yes do:
                        {
                        remove the elem1 from the elemSet1 
                        and break here 
                        }
                  if elem2's size < elem1's size{
                  => if yes do:
                        {
                        remove the elem2 from the elemSet2 
                        and break here 
                        }
        => if no check:
       		if elem2's items subset of elem1's items {
       		=> if yes do:
       			{
                        remove the elem2 from the elemSet2 
       			}
       		if elem1's items subset of elem2's items {
       		=> if yes do:
       			{
                        remove the elem1 from the elemSet1 
                        and break here 
       			}	
      }
      
       Finally, add elemSet2 elements to elemSet1.
      
       return elemSet1
 
 ## How to use the code:
 
 This code is written as a Scala Maven Project and one can use the **uSS.jar** available in the **target** folder and launch the application on any hadoop cluster. Or you can download the entire repository and run in standalone mode for test.
 
 Scala Code can be found at:
 
 [Scala Code](https://github.com/VarunRaj7/uniqueSuperSets-in-Spark/blob/master/src/main/scala/com/VarunRaj7/uniqueSuperSets.scala)
 
 When using Jar command:
 
 
 spark-submit < job configurations > com.VarunRaj7.uniqueSuperSets uSS.jar < num of map partitions > < num of final result partitions > < Input file path > < Output file path >
 
 Sample Input file:
 
 1 2 3 5
 
 1 4 5 
 
 2 5
 
 2 3 5
 
 4 5
 
 Sample Output file:
 
 1 2 3 5
 

1 4 5
 
## Verification

The working of Spark algorithm is verified with a equivalent sequential naive approach given in the following python notebook on the testInp dataset (all of these are available in the Verification folder):

[Verification Code](https://github.com/VarunRaj7/uniqueSuperSets-in-Spark/blob/master/Verification/Verifying%20the%20results%20of%20Unique%20SuperSet.ipynb)


 
## Time Comparision of SparkAlgo Vs NaiveAlgo

#### Dataset: FIFA

source: https://www.philippe-fournier-viger.com/spmf/index.php?link=datasets.php

num of rows: 20,450
num of columns: 2,990
Avg num of columns: ~35

FIFA dataset is cleaned to contain distinct rows, that is there are only 13,316 unique rows. This dataset is given to this **spark application** which yielded 5033 unique Super Set rows in **less than a minute**. However, the same is done on a **single machine sequentially** in python yielded same number of unique super sets but took almost **45 minutes**.

NOTE: Spark application configurations:

num of executors: 65
Driver-memory: 5g
Executor-memory: 4g
executor-cores: 4




      
      
