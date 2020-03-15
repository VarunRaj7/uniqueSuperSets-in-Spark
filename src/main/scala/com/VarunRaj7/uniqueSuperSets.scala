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


object uniqueSuperSets {
    
    /*
     * Creating a ItemHash Class to hold the information of Items, HashValue and Items Size.  
     * 
     * Items: is 		  => Stored as immutable.TreeSet[Int]
     * HashValue: hv  => Stored as Int
     * Items Size:iss => Stroed as Int
     * 
     * This class has two more methods:
     * 
     * --(i:immutable.TreeSet[Int]):ItemHash
     * 
     * and
     * 
     * compare(that:ItemHash):Int
     * 
     */
    class ItemHash(val is:immutable.TreeSet[Int], val hv:Int, val iss:Int)
    extends Ordered[ItemHash] with Serializable           // Ordered trait is required to compare Items 
    {
      
      /*
       * --(i:immutable.TreeSet[Int]):ItemHash
       * 
       * 
       * This function returns a new ItemHash with 
       * the "i" items discarded while preserving the
       * HashValue and size of the original.
       * 
       */
      
      def --(i:immutable.TreeSet[Int]):ItemHash={
        val temp_is = this.is--i
        return new ItemHash(temp_is, hv, iss)
      }
      
      
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
      
      def compare(that:ItemHash):Int = {
        //
        if (this.iss>that.iss) {
          -1
        }
        else if (this.iss<that.iss){
          1
        }
        else{
          this.hv.toString.compareTo(that.hv.toString)
        }
      }
            
    }
    
   /*
    * 
    * seqOp(elemSet:mutable.TreeSet[ItemHash], elemx:ItemHash):mutable.TreeSet[ItemHash]
    * 
    * This function is used with the aggregateByKey as a function that applies on the values
    * with same key within the partition. It will take a elemSet:mutable.TreeSet[ItemHash] 
    * and a elemx:ItemHash. And, returns a modified elemSet:mutable.TreeSet[ItemHash].
    * 
    * 
    * 
    * Firstly, create a flag variable to add the elemx to elemSet as true.
    * 
    * Then, this function checks for an "elem"(element in elemSet):
    *  if elemx's items are equal to elem's items{
    *  	=> if yes check:
    *  				if elemx's size > elem's size {
    *  				=> if yes do:
    *  					{
    *  						remove the elemx from the elemSet 
    *  						and break here to add elemx to elemSet
    *   				}
    *   			if elemx's size < elem's size{
    *   			=> if yes do:
    *   				{
    *   					change flag to false as need not to be added to elemSet
    *   					and break here 
    *   				}
    *   => if no check:
    *   		if elemx's items subset of elem's items {
    *   		=> if yes do:
    *   			{
    *   				change flag to false as need not to be added to elemSet
    *   				and break here 
    *   			}
    *   		if elem's items subset of elemx's items {
    *   		=> if yes do:
    *   			{
    *   				remove the elemx from the elemSet
    *   			}	
    *  }
    *  
    *  Finally, add elemx to elemSet if flag is true.
    * 
    */
   
   def seqOp(elemSet:mutable.TreeSet[ItemHash], elemx:ItemHash):mutable.TreeSet[ItemHash]={
     
     var flag = true  // add flag
     
     //println("In Start seqOp: ")
     
     //println("elemx: "+elemx.is.mkString(" ")++": "+elemx.hv)
     
     breakable{
     elemSet.foreach(elem => {  //println("Inside foreach of seqOp: "+elem.is.mkString(" ")+": "+elem.hv)
                                if(elemx.is == elem.is){
                                 if(elemx.iss > elem.iss){
                                   elemSet-=elem
                                   break
                                 }
                                 if(elemx.iss < elem.iss){
                                   flag = false
                                   break
                                 }
                               }
                               else{
                                 if(elemx.is subsetOf elem.is){
                                   flag = false
                                   break
                                 }
                                 if(elem.is subsetOf elemx.is){
                                   elemSet-=elem
                                   //break
                                 }
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
    * combOp(elemSet:mutable.TreeSet[ItemHash], elemx:ItemHash):mutable.TreeSet[ItemHash]
    * 
    * This function is used with the aggregateByKey as a function that applies on the values
    * with same key that are resultant of seqOp of two partitions. It will take a elemSet1:mutable.TreeSet[ItemHash] 
    * and a elemSet2:mutable.TreeSet[ItemHash]. And, returns a modified elemSet1:mutable.TreeSet[ItemHash].
    * 
    * 
    * 
    * This function checks for an "elem1"(element in elemSet1) with "elem2" (element in elemSet1):
    *  if elem2's items are equal to elem1's items{
    *  	=> if yes check:
    *  				if elem2's size > elem1's size {
    *  				=> if yes do:
    *  					{
    *  						remove the elem1 from the elemSet1 
    *  						and break here 
    *   				}
    *   			if elem2's size < elem1's size{
    *   			=> if yes do:
    *   				{
    *  						remove the elem2 from the elemSet2 
    *  						and break here 
    *   				}
    *   => if no check:
    *   		if elem2's items subset of elem1's items {
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
   
   def combOp(elemSet1:mutable.TreeSet[ItemHash], elemSet2:mutable.TreeSet[ItemHash]):mutable.TreeSet[ItemHash]={
     //println("In combOp: ")
     //println("elemSet1: ")
     //elemSet1.foreach(elem =>println(elem.is.mkString(" ")+": "+elem.hv))
     //println("elemSet2: ")
     //elemSet2.foreach(elem =>println(elem.is.mkString(" ")+": "+elem.hv))
     elemSet1.foreach(elem1 => {
                                 breakable{
                                 elemSet2.foreach(elem2 => { if(elem2.is == elem1.is){
                                                               if(elem2.iss > elem1.iss){
                                                                 elemSet1-=elem1
                                                                 break
                                                               }
                                                               if(elem2.iss < elem1.iss){
                                                                 elemSet2-=elem2
                                                                 break
                                                               }
                                                             }
                                                             else{
                                                               if(elem2.is subsetOf elem1.is){
                                                                 elemSet2-=elem2
                                                                 //break
                                                               }
                                                               if(elem1.is subsetOf elem2.is){
                                                                 elemSet1-=elem1
                                                                 break
                                                               }
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
   
   def mapperFunc(elem:String):Set[Tuple2[Int, ItemHash]]={  
      
      // Create a immutable.TreeSet[Int] with items given in String
      val elem_set = new immutable.TreeSet[Int]()++(elem.split(" ").map(_.toInt))
      
      // Create a HashCode for the items given in String
      val elemIH = new ItemHash(elem_set, elem.hashCode(), elem_set.size)
      //println(elem+": "+elem.hashCode())
      
      // Create a mutable.ListBuffer[Int] to store the all items removed from 
      // the elem_set to yield the new ItemHash having the same Hashcode and size.
      val rem_set = mutable.ListBuffer[Int]()
      
      for{i <- elem_set;
          val tempIH = elemIH--(immutable.TreeSet[Int]()++(rem_set+=i))}
      yield (i, tempIH)//(i, tempIH.is.mkString(" "))//
 
   }
    
   def main(args: Array[String]) {
     
     
    // Creating a SparkSession
     val spark = SparkSession
                .builder
                //.master("local") //Uncomment this to run in standalone mode
                .appName("uniqueSuperSets")
                .getOrCreate()
      
     // SparkContext
     val sc = spark.sparkContext
    
     // Map Partitions should be given as argument at position at "0"
     val map_partitions = args(0).toInt
     
     // Final result Partitions should be given as argument at position at "1"
     val final_res_partitions = args(1).toInt
     
     // Uncomment these examples to understand the code flow
     //val s = Array("1 2 3 5", "1 4 5", "2 5", "2 3 5", "4 5")
     
     //val s = Array("1 3 4", "2 3 4", "4")
     
     
     //val s = Array("1 3 6 7 8 9", "2 3 7 8 9")
     
     //val rdd_s = sc.parallelize(s,2)
     
     //val rdd_s = sc.textFile("data/testInp.txt", 1).map(elem => elem.stripSuffix("\n").split("\t")(1).replace(",", " ")).distinct()
     
     
     // Input file/files should be given as argument at position at "2"
     // Also, if distinct are not given the following code will check for distinct
     val rdd_s = sc.textFile(args(2), map_partitions).map(elem => elem.stripSuffix("\n").replace(",", " ")).distinct()
     
     // Create a dataframe rdd_s_df with items and corresponding HashValues 
     val sqlc = spark.sqlContext
     import sqlc.implicits._
     var rdd_s_df = rdd_s.map(elem => (elem, elem.hashCode()))
                         .toDF(Seq("Items", "HashValues"):_*)
                         .cache()
     
     //println("rdd_s_df partitions: ", rdd_s_df.rdd.getNumPartitions)
     //rdd_s_df = rdd_s_df.withColumn("HashValue", hash(rdd_s_df("Items")))
     
     //rdd_s_df.show()
     //rdd_s_df.printSchema()
     
    
     val rdd_s1 = rdd_s.map(mapperFunc) // map function on each element in rdd_s
                       .flatMap(elem => elem) // flatten out the map results and comment
                                              // when you uncomment the commented code for rdd_s2
     
     
     /*
      * Uncomment this to understand the flow of the algorithm
      *
     val rdd_s2 = rdd_s1.flatMap(elem => elem) 
     rdd_s2.mapPartitionsWithIndex((x, iter) => {  val temp_list =  mutable.ListBuffer[Tuple4[Int, Int, String, Int]]()
                                                   while(iter.hasNext){
                                                     val temp = iter.next()
                                                     temp_list+=Tuple4(x, temp._1, temp._2.is.mkString(" "), temp._2.hv)
                                                   }
                                                   temp_list.iterator
                                                 }, true).collect().map(println)
     
     *                                               
     */
     
     // Comment this when you uncomment the code for rdd_s3
     val rdd_s3 = rdd_s1.aggregateByKey(mutable.TreeSet[ItemHash]())(seqOp, combOp) // aggregateByKey Operation with seqOp and combOp
                        .map(elem => for(x<-elem._2) yield x.hv)  // mapper function to seek out all the hash values given by each key
                        .flatMap(elem => elem) // flatten out the map results
     /*
      * Replace the above line for rdd_s3 with the below code while 
      * running to understand the flow of the algorithm 
      * 
     val rdd_s3 = rdd_s2.aggregateByKey(mutable.TreeSet[ItemHash]())(seqOp, combOp)
     rdd_s3.collect().map(elem => { println("For elem:"+elem._1)
                                    elem._2.map(elemx => println(elemx.hv))})
                                    
     val rdd_s4 = rdd_s3.map(elem => for(x<-elem._2) yield x.hv)
                        .flatMap(elem => elem)//.distinct()
     s_set.foreach(elem => println(elem.is.mkString(" ")))
     rdd_s4.collect().map(println)
     *
     */
     
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
                        
     sqlc.setConf("spark.sql.shuffle.partitions", "10") // you can set this to any desired value
     
     // Creating unique_df_hv with unique HashValues
     val unique_df_hv = rdd_s3.toDF("HashValues").dropDuplicates()
       
     
     // Suppressing the usual autoBroadcastJoin to use SortMergeJoin
     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1);
     
     //println("unique_df_hv partitions: ",unique_df_hv.rdd.getNumPartitions)
     //spark.conf.set("spark.sql.crossJoin.enabled",true);
     
     // Inner join the rdd_s_df with unique_df_hv with column "HashValues" 
     val res_df = rdd_s_df.join(unique_df_hv, Seq("HashValues"),"inner").repartition(final_res_partitions)
     
     println(res_df.count)
     
     val res_df_items = res_df.drop("HashValues") 
     
     res_df_items.write.format("csv").save(args(3))
     //res_rdd.explain()
     //res_rdd.show()
   }  
}
