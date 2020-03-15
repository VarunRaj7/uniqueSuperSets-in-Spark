package com.VarunRaj7

/*
 * Creator: Varun Raj Rayabarapu
 * git: https://github.com/VarunRaj7
 * 
 * 
 * NOTE: Uncomment the commented sections described 
 *       below to understand the code flow
 */

/*
 * Importing Necessary Modules
 * 
 */

import scala.collection._
import util.control.Breaks._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object uniqueSuperSets3 {
  
      /*
     * Creating a Trans Class to hold the information of Items, HashValue and Items Size.  
     * 
     * Items: is 		  => Stored as immutable.TreeSet[Int]
     * Items Size:iss => Stroed as Int
     * 
     * This class has one method to define ordering:
     * 
     * compare(that:ItemHash):Int
     * 
     */
    class Trans(val is:immutable.TreeSet[Int], val iss:Int)
    extends Ordered[Trans] with Serializable           // Ordered trait is required to compare Items 
    {
      
      /* 
       * compare(that:ItemHash):Int
       * 
       * 
       * This function is required to 
       * compare two ItemHash class objects and
       * order them first by decreasing iss (size)
       * and then by increasing hv (Hash Value).	
       * 
       * 
       */
      
      def compare(that:Trans):Int = {
        //
        if (this.iss>that.iss) {
          -1
        }
        else if (this.iss<that.iss){
          1
        }
        else{
          this.is.toString.compareTo(that.is.toString)
        }
      }
            
    }
    
   /*
    * 
    * seqOp(elemSet:mutable.TreeSet[Trans], elemx:Trans):mutable.TreeSet[Trans]
    * 
    * This function is used with the aggregateByKey as a function that applies on the values
    * with same key within the partition. It will take a elemSet:mutable.TreeSet[Trans] 
    * and a elemx:Trans. And, returns a modified elemSet:mutable.TreeSet[Trans].
    * 
    * 
    * 
    * Firstly, create a flag variable to add the elemx to elemSet as true.
    * 
    * Then, this function checks for an "elem"(element in elemSet):
    *  		if elemx's items subset of elem's items {
    *   		=> if yes do:
    *   			{
    *   				change flag to false as need not to be added to elemSet
    *   				and break here 
    *   			}
    *     }
    *   	if elem's items subset of elemx's items {
    *   		=> if yes do:
    *   			{
    *   				remove the elemx from the elemSet
    *   				and break herehere to add elemx to elemSet
    *   			}	
    * 	}
    * 
    *  
    *  Finally, add elemx to elemSet if flag is true.
    * 
    */
   
   def seqOp(elemSet:mutable.TreeSet[Trans], elemx:Trans):mutable.TreeSet[Trans]={
     
     var flag = true  // add flag
     
     //println("In Start seqOp: ")
     
     //println("elemx: "+elemx.is.mkString(" ")++": "+elemx.hv)
     
     breakable{
     elemSet.foreach(elem => {  if(elemx.is subsetOf elem.is){
                                   flag = false
                                   break
                                 }
                                 if(elem.is subsetOf elemx.is){
                                   elemSet-=elem
                                 }
                             })
     }
     if(flag){
       //println("executed flag: ")
       elemSet+=elemx
       //println(elemSet.size)
       //println(elemSet.map(elem=>println(elem.is.mkString(" ")+": "+elem.hv)))
     }
     //println("In end seqOp: ")
     //println(elemSet.foreach(elem=>println(elem.is.mkString(" ")+": "+elem.hv)))
     
     return elemSet
   }
   
      /*
    * 
    * combOp(elemSet:mutable.TreeSet[Trans], elemx:Trans):mutable.TreeSet[Trans]
    * 
    * This function is used with the aggregateByKey as a function that applies on the values
    * with same key that are resultant of seqOp of two partitions. It will take a elemSet1:mutable.TreeSet[Trans] 
    * and a elemSet2:mutable.TreeSet[Trans]. And, returns a modified elemSet1:mutable.TreeSet[Trans].
    * 
    * 
    * 
    * This function checks for an "elem1"(element in elemSet1) with "elem2" (element in elemSet1):
    *  		if elem2's items subset of elem1's items {
    *   		=> if yes do:
    *   			{
    *  					remove the elem2 from the elemSet2 
    *   			}
    *   		if elem1's items subset of elem2's items {
    *   		=> if yes do:
    *   			{
    *  					remove the elem1 from the elemSet1 
    *  					and break here 
    *   			}	
    *  }
    *  
    *  Finally, add elemSet2 elements to elemSet1.
    *  
    *  return elemSet1
    * 
    */
   
   def combOp(elemSet1:mutable.TreeSet[Trans], elemSet2:mutable.TreeSet[Trans]):mutable.TreeSet[Trans]={
     //println("In combOp: ")
     //println("elemSet1: ")
     //elemSet1.foreach(elem =>println(elem.is.mkString(" ")+": "+elem.hv))
     //println("elemSet2: ")
     //elemSet2.foreach(elem =>println(elem.is.mkString(" ")+": "+elem.hv))
     elemSet1.foreach(elem1 => {
                                 breakable{
                                 elemSet2.foreach(elem2 => { if(elem2.is subsetOf elem1.is){
                                                               elemSet2-=elem2
                                                               //break
                                                             }
                                                             if(elem1.is subsetOf elem2.is){
                                                               elemSet1-=elem1
                                                               break
                                                             }
                                                            })
                                          }
                                 }
                       )
     
     elemSet1++=elemSet2
     //println("final elemSet: ")
     //elemSet1.foreach(elem =>println(elem.is.mkString(" ")+": "+elem.hv))
     return elemSet1
   }
   
   def mapperFunc(elem:String)={//:Set[Tuple2[Int, ItemHash]]={  
      
      // Create a immutable.TreeSet[Int] with items given in String
      val elem_set = new immutable.TreeSet[Int]()++(elem.split(" ").map(_.toInt))
      
      val trans = new Trans(elem_set, elem_set.size)
            
      for(i <- elem_set)
      yield (i, trans)//(i, tempIH.is.mkString(" "))//
 
   }
    
   def main(args: Array[String]) {
     
     
    // Creating a SparkSession
     val spark = SparkSession
                .builder
                //.master("local") //Uncomment this to run in standalone mode
                .appName("uniqueSuperSetsv3")
                .getOrCreate()
      
     // SparkContext
     val sc = spark.sparkContext
    
     // Map Partitions should be given as argument at position at "0"
     val map_partitions = args(0).toInt
     
     // Final result Partitions should be given as argument at position at "1"
     val final_res_partitions = args(1).toInt
     
     // Shuffle Partitions
     val shuffle_partitions = args(2)
     
     // Uncomment these examples to understand the code flow
     // val s = Array("1 2 3 5", "1 4 5", "2 5", "2 3 5", "4 5")
     
     //val s = Array("1 3 4", "2 3 4", "4")
     
     
     //val s = Array("1 3 6 7 8 9", "2 3 7 8 9")
     
     //val rdd_s = sc.parallelize(s,2)
     
     //val rdd_s = sc.textFile("data/sign_n.txt", 1).map(elem => elem.stripSuffix("\n").split("\t")(1).replace(",", " "))//.distinct()
     
     
     // Input file/files should be given as argument at position at "2"
     // Also, if distinct are not given the following code will check for distinct
     val rdd_s = sc.textFile(args(3), map_partitions).map(elem => elem.stripSuffix("\n"))//.replace(",", " ").distinct()     
    
     val rdd_s1 = rdd_s.map(mapperFunc) // map function on each element in rdd_s
                       .flatMap(elem => elem) // flatten out the map results and comment
                                              // when you uncomment the commented code for rdd_s2
     
     val rdd_s3 = rdd_s1.aggregateByKey(mutable.TreeSet[Trans]())(seqOp, combOp) // aggregateByKey Operation with seqOp and combOp
                        .map(elem => for(x<-elem._2) yield x.is.mkString(" "))  // mapper function to seek out all the hash values given by each key
                        .flatMap(elem => elem) // flatten out the map results

     val sqlc = spark.sqlContext
     import sqlc.implicits._

     /* Optional Configurations:
      * 
      * The dropDuplicates() function requires shuffling.
      * Hence, the default shuffle paritions must be changed or
      * a repartition operation must be performed to get the desired 
      * number of partitions.
      * 
      * Default:
      * 
      sqlContext.setConf("spark.sql.shuffle.partitions", "200")
      *
      */
                        
     sqlc.setConf("spark.sql.shuffle.partitions", shuffle_partitions) // you can set this to any desired value
     
     //rdd_s3.collect().map(println)
     
     // Creating unique_df_hv with unique HashValues
     val unique_df_hv = rdd_s3.toDF("UniqueSS").dropDuplicates()
       
     
     println("Unique Super Sets count: ",unique_df_hv.count)
     
     unique_df_hv.write.format("csv").save(args(4))
     //unique_df_hv.explain()
     //unique_df_hv.show()
   } 
}