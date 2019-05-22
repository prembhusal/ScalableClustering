package sct
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import sct.mst.LocalParMst
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{concat, lit,struct}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import scala.io.Source
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.expressions._
import java.io._
import sct.mst.ParMST
import sct.mst.DendroNode
import scala.util.control.Breaks._

class VDJprocess extends Serializable   {
  
  
  def processTop10Time(thres:Double) : Unit = {
    
    //for calculating clustering accuracy for top 10 groups
    val t0 = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._
    /*val conf = new SparkConf().setAppName("Histogram").setMaster("local")
		val sc1 = new SparkContext(conf)
   */
    val sc = spark.sparkContext
    var df = spark.read.option("sep", "\t").option("header", "true").csv("data/influenzaIndex.tsv")// reading stat file in dataframe
     //add new column which is combination of index,id,junction
    val dfNew = df.withColumn("Seq", struct(df("Index"), df("SEQUENCE_ID"),df("JUNCTION")))
   
    val groups = dfNew.select("V_CALL","J_CALL","JUNCTION_LENGTH","Seq").groupBy("V_CALL","J_CALL","JUNCTION_LENGTH");
    val groupList = groups.agg(collect_list("Seq").as("SeqList")).collect
    
    val top10 = groupList.sortBy(x=>x.getList(3).size).reverse.take(10)
     
    for (k <- 0 to 9){
      println("==============")
      println("group:"+k)
      println("groupSize:"+top10(k).getList(3).size())
     
      var a = new ArrayBuffer[Sequence]
      var count = 0
      
        for( i <- 0 to top10(k).getList(3).size()-1){
         var record = top10(k).getList(3).get(i).toString().replaceAll("^\\[|\\]$", "").split(",")
          a+= new Sequence(count, record(1),record(2))
         count+=1
         }
      
      val localMst = new LocalParMst[Sequence]
     // val bLocal = new BufferedWriter(new FileWriter(new File("output/All/influenza/locMst"+k)))
     // val hcvp = new BufferedWriter(new FileWriter(new File("output/All/mysthesiaHD/hcvp"+k)))
      
     /*//label local mst
      var m1 = new  Array[Int](count)
      var l1 = 0
      val res = localMst.cluster(a ,4,0,thres)
      println("mst size :"+res.size)
      for (c <- res){
        
         c.getRecords.foreach( y => m1(y) = l1)
        l1+= 1
      }
      m1.foreach( x => bLocal.write(x+"\n"))
      bLocal.close() 
      
     */ 
      //label vp-hc
      var t0= System.currentTimeMillis()
       val h = new HClusteringVPTree[Sequence]
       h.clustering(0,thres, a)
       var m2 = new  Array[Int](count)
       var l2 = 0
       println("vp-hc :"+h.clusters.size)
      var t1 = System.currentTimeMillis()
      println("vp-hc time :"+(t1-t0)/1000)
       /*h.clusters.foreach(x =>{
        x.members.foreach(y =>m2(y)= l2)
            l2+=1})
      m2.foreach(l => hcvp.write(l+"\n"))
      hcvp.close() */
      
      //label sct :
      var fanout = 10
      var threshold = 0.03 //97% threshold to generate tree
      var maxlen = 270
      var centerMethod = 0
      var distMethod = 0
      var debug = 0
      var summaryLevel = -1
      var secondStage = 0
      
     var t3= System.currentTimeMillis()
     var objTree = new SeqCondenseTree(fanout, threshold, maxlen, centerMethod, distMethod, debug)
      a.foreach(x => objTree.insert(x))
      
      objTree.calcRadius
      //objTree.printTree
      var listT = List(0.14)
      for (cut <-listT){
      //val bSct = new BufferedWriter(new FileWriter(new File("output/All/d1/d1thres/sctCent"+k+"C"+cut)))
      var clusters = objTree.getSummary(cut,0)
      //println("Cut level:"+cut)
      println("SCT size : "+clusters.size)
      var t4= System.currentTimeMillis()
      println("SCT time:"+(t4-t3)/1000)
      /*clusters.foreach(x =>{
        x.members.foreach(y =>{m3(y)= l3})
            l3+=1})
      m3.foreach( x => bSct.write(x+"\n"))
      bSct.close()*/
      //Apply HC on clusters
       val h = new HClusteringVPTree[Cluster]
       h.clustering(0,thres, clusters.to[ArrayBuffer])
       var m2 = new  Array[Int](count)
       var l2 = 0
       println("SCT-hc :"+h.clusters.size)
      var t5= System.currentTimeMillis()
      println("SCT-hc time :"+(t5-t4)/1000)
       /*h.clusters.foreach(x =>{
        x.members.foreach(y =>m2(y)= l2)
            l2+=1})
      m2.foreach(l => bSct.write(l+"\n"))
      bSct.close() 
      println("===================")*/
      }
   
    }
       
  }
  
  def processTop10(thres:Double) : Unit = {
    
    //for calculating clustering accuracy for top 10 groups
    val t0 = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._
    /*val conf = new SparkConf().setAppName("Histogram").setMaster("local")
		val sc1 = new SparkContext(conf)
   */
    val sc = spark.sparkContext
    var df = spark.read.option("sep", "\t").option("header", "true").csv("data/mysthesiaHCHD.tsv")// reading stat file in dataframe
     //add new column which is combination of index,id,junction
    val dfNew = df.withColumn("Seq", struct(df("Index"), df("SEQUENCE_ID"),df("JUNCTION")))
   
    val groups = dfNew.select("V_CALL","J_CALL","JUNCTION_LENGTH","Seq").groupBy("V_CALL","J_CALL","JUNCTION_LENGTH");
    val groupList = groups.agg(collect_list("Seq").as("SeqList")).collect
    
    val top10 = groupList.sortBy(x=>x.getList(3).size).reverse.take(10)
     
    for (k <- 0 to 9){
      println("==============")
      println("group:"+k)
      println("groupSize:"+top10(k).getList(3).size())
     
      var a = new ArrayBuffer[Sequence]
      var count = 0
      
        for( i <- 0 to top10(k).getList(3).size()-1){
         var record = top10(k).getList(3).get(i).toString().replaceAll("^\\[|\\]$", "").split(",")
          a+= new Sequence(count, record(1),record(2))
         count+=1
         }
      
      val localMst = new LocalParMst[Sequence]
     // val bLocal = new BufferedWriter(new FileWriter(new File("output/All/influenza/locMst"+k)))
     // val hcvp = new BufferedWriter(new FileWriter(new File("output/All/mysthesiaHD/hcvp"+k)))
      
    /* //label local mst
      var m1 = new  Array[Int](count)
      var l1 = 0
      val res = localMst.cluster(a ,4,0,thres)
      println("mst size :"+res.size)
      for (c <- res){
        
         c.getRecords.foreach( y => m1(y) = l1)
        l1+= 1
      }
      m1.foreach( x => bLocal.write(x+"\n"))
      bLocal.close() 
      
      
      //label vp-hc
      var t0= System.currentTimeMillis()
       val h = new HClusteringVPTree[Sequence]
       h.clustering(0,thres, a)
       var m2 = new  Array[Int](count)
       var l2 = 0
       println("vp-hc :"+h.clusters.size)
      var t1 = System.currentTimeMillis()
      println("vp-hc time :"+(t1-t0)/1000)*/
       /*h.clusters.foreach(x =>{
        x.members.foreach(y =>m2(y)= l2)
            l2+=1})
      m2.foreach(l => hcvp.write(l+"\n"))
      hcvp.close() */
      
      //label sct :
      var fanout = 10
      var threshold = 0.03 //97% threshold to generate tree
      var maxlen = 270
      var centerMethod = 0
      var distMethod = 1
      var debug = 0
      var summaryLevel = -1
      var secondStage = 0
      
     var t3= System.currentTimeMillis()
     var objTree = new SeqCondenseTree(fanout, threshold, maxlen, centerMethod, distMethod, debug)
      a.foreach(x => objTree.insert(x))
      
      objTree.calcRadius
      //objTree.printTree
      var listT = List(0.15)
      for (cut <-listT){
      val bSct = new BufferedWriter(new FileWriter(new File("output/All/mysthesiaHD/mystThres/sctCent"+k+"C"+cut)))
      var clusters = objTree.getSummary(cut,0)
      //println("Cut level:"+cut)
      println("SCT size : "+clusters.size)
      var t4= System.currentTimeMillis()
      println("SCT time:"+(t4-t3)/1000)
      /*clusters.foreach(x =>{
        x.members.foreach(y =>{m3(y)= l3})
            l3+=1})
      m3.foreach( x => bSct.write(x+"\n"))
      bSct.close()*/
      //Apply HC on clusters
       val h = new HClusteringVPTree[Cluster]
       h.clustering(0,thres, clusters.to[ArrayBuffer])
       var m2 = new  Array[Int](count)
       var l2 = 0
       println("SCT-hc :"+h.clusters.size)
      var t5= System.currentTimeMillis()
      println("SCT-hc time :"+(t5-t4)/1000)
       h.clusters.foreach(x =>{
        x.members.foreach(y =>m2(y)= l2)
            l2+=1})
      m2.foreach(l => bSct.write(l+"\n"))
      bSct.close() 
      println("===================") 
      }
   
    }
       
  }
  
  def processGidSeq( thres:Double,data:RDD[String],outfile:String, method:Int) = {
    //spark based processing of entire data ( pre-processed data) , preprossed data is in the form (groupId, Sequence). 
    val header = data.first()
    val t1 = System.currentTimeMillis()
    val rdd1 = data.filter(row => row != header)
   
    val gidMap =  rdd1.map(x => {
      
      val str = x.split("\t")
      val gid = str(0).toInt
      val seqStr = str(1).replaceAll("\"", "").split(",")
      val sequence = new Sequence(seqStr(0).toInt, seqStr(1),seqStr(2))
      (gid,sequence)
    })
    var count = rdd1.count
    //gidMap is  : RDD[(Int, Sequence)] cinnsist of groupId and sequence
     
    //use different algorithms:
    method match {
        		case 1 => useLocalMst( count.toInt,gidMap,outfile,thres) //localMST
        		case 2 => useVPTreeHC(count.toInt,gidMap,outfile,thres) //VPTreeHC
        		case 3 => useSCT_VPTreeHC(count.toInt,gidMap,outfile,thres) //SCT_VPtreeHC
        		}
    
   
    
  }
  
  def useLocalMst(count:Int, gidSeq:RDD[(Int,Sequence)],outfile:String, thres:Double) = {
    val localMst = new LocalParMst[Sequence]
    
    val groupDendroNode = gidSeq.groupByKey().map( x => {
     
      if(x._2.size < 2){
        List(new DendroNode(null, null, x._2.toList(0).id, 0.0))
        
      }else{
       // println("group")
        localMst.cluster(x._2.to[ArrayBuffer] ,4,0,thres)
      }
    }).persist
    
    groupDendroNode.saveAsTextFile(outfile)
    /*//var reses = groupDendroNode.glom()
    val res = groupDendroNode.glom().collect
    
    var labelMap = new  Array[Int](count.toInt)
    var label = 1
    res.foreach( x => { 
     
      for( listD <-x){
      for (c <- listD) {
      c.getRecords.foreach(m => labelMap(m) = label)
      label = label + 1}
    }
    })
    
    println("after localMst collect ===")
    
    
    val bwLabel = new BufferedWriter(new FileWriter(new File(outfile)))
    labelMap.foreach( x => bwLabel.write(x+"\n"))
    bwLabel.close()*/
  }
  
  def useVPTreeHC(count:Int, gidSeq:RDD[(Int,Sequence)],outfile:String, thres:Double) = {
    
    val groupClusters = gidSeq.groupByKey().map( x => {
     
      if(x._2.size < 2){
        List(new Cluster(0, x._2.toList(0), ListBuffer(x._2.toList(0).id), ListBuffer(x._2.toList(0))))
        
      }else if(x._2.size < 100) {
        
        clustSmall(x._2.to[ArrayBuffer], thres)
        
      }else{
       // println("group") 
        val h = new HClusteringVPTree[Sequence]
        h.clustering(0,thres, x._2.to[ArrayBuffer])
      }
    })
    
    
    groupClusters.saveAsTextFile(outfile)
    /*val res = groupClusters.glom().collect
    
    var labelMap = new  Array[Int](count.toInt)
    var label = 1
    
    res.foreach(x => {
      for( listC <- x){
            for (c <- listC) {
      c.members.foreach(m => labelMap(m) = label)
      label = label + 1
      }
      }
    })
   
    
    println("VPT-HC")
    val bwLabel = new BufferedWriter(new FileWriter(new File(outfile)))
    labelMap.foreach( x => bwLabel.write(x+"\n"))
    bwLabel.close()*/
  }
  
  def useSCT_VPTreeHC(count:Int, groups:RDD[(Int,Sequence)],outfile:String, thres:Double) = {
    
    var fanout = 12
      var threshold = 0.02
      var maxlen = 270
      var centerMethod = 0
      var distMethod = 0
      var debug = 0
      var summaryLevel = -1
      var secondStage = 0
      
      var t3 = System.currentTimeMillis()
 
      val groupClusters = groups.groupByKey.map( x => {
     
      if(x._2.size < 2){
        List(new Cluster(0, x._2.toList(0), ListBuffer(x._2.toList(0).id), ListBuffer(x._2.toList(0))))
        
      }else if (x._2.size < 10){
        
        clustSmall(x._2.to[ArrayBuffer], thres)
        
      } else if (x._2.size < 1000){
          
        val h = new HClusteringVPTree[Sequence]
        h.clustering(0,thres, x._2.to[ArrayBuffer])
        
      }else{
       // println("group")
        var objTree = new SeqCondenseTree(fanout, threshold, maxlen, centerMethod, distMethod, debug)
      
        x._2.foreach(x => objTree.insert(x))
        objTree.calcRadius
        var summaries = objTree.getSummary(thres,0)
        val h = new HClusteringVPTree[Cluster]
        
        h.clustering(0,thres, summaries.to[ArrayBuffer])
        //h.clustering(0,thres, x._2.to[ArrayBuffer])
      }
    })
    groupClusters.saveAsTextFile(outfile)
    /*val res = groupClusters.glom().collect
   
    var labelMap = new  Array[Int](count.toInt)
    var label = 1
    
    res.foreach(x => {
      for( listC <- x){
            for (c <- listC) {
      c.members.foreach(m => labelMap(m) = label)
      label = label + 1
      }
      }
    })
    println("SCT-Vp-HC")
    val bwLabel = new BufferedWriter(new FileWriter(new File(outfile)))
    labelMap.foreach( x => bwLabel.write(x+"\n"))
    bwLabel.close()*/
  }
  
   def clustSmall( records: ArrayBuffer[Sequence], thres:Double) :List[Cluster] = {
     var  clusters  : List[Cluster] = List()
     var seqListMap =scala.collection.mutable.Map[ Sequence, ListBuffer[Int]]() 
       
     if ( records.size ==2){
       var dist = records(0).distance(records(1))
       if( dist < thres){
         clusters = List(new Cluster(dist,records(0), ListBuffer(records(0).id,records(1).id), ListBuffer(records(0)))) 
       }else {
         clusters = records.map( x => new Cluster(0, x, ListBuffer(x.id), ListBuffer(x))).toList
       }
       
     }else {
       records.foreach( x => {
         if( seqListMap.size > 0){
           breakable {
             seqListMap.foreach( y=> {
             if (x.distance(y._1) < thres) {seqListMap(y._1) += x.id ;break}
               //else seqListMap(x) = ListBuffer(x.id) ;break
             
           })
           if(!seqListMap.contains(x)) seqListMap(x) = ListBuffer(x.id) ;break
           }
         
            
         }else{
           seqListMap(x) = ListBuffer(x.id)
         }  
       })
       clusters = seqListMap.map(x => new Cluster(0, x._1, x._2, ListBuffer(x._1))).toList
     } 
     return clusters
   }
  def testSct(  seqFile:String,thresSct:Double,distMethod:Int) = {
      
     
      val seqs = new ArrayBuffer[Sequence]
      var count =0
      for (pairs<- Source.fromFile(seqFile).getLines().sliding(2,2)){
      seqs += new Sequence(count, pairs.head, pairs.last)
      count += 1
      }
      
      
      
      /*val t1 = System.currentTimeMillis()
      val h1 = new  HClusteringVPTree[Sequence]
       h1.clustering(0,thres, seqs)
      val t2 = System.currentTimeMillis()
      println("VP-HC size: "+h1.clusters.size)
       println("VP-HC time : " +(t2-t1)/1000)*/
      
      var fanout = 12
      var threshold = thresSct
      var maxlen = 270
      var centerMethod = 0
      //var distMethod = 3
      var debug = 0
      var summaryLevel = -1
      var secondStage = 0
      
      var t3 = System.currentTimeMillis()
      var objTree = new SeqCondenseTree(fanout, threshold, maxlen, centerMethod, distMethod, debug)
      seqs.foreach(x => objTree.insert(x))
     
      var thres = List(0.03,0.06,0.09,0.12,0.15)
      objTree.calcRadius
      for ( t<- thres){
        println("threshold : " +t)
        var clusters = objTree.getSummary(t ,0)
        println("size cluster : " +clusters.size)
        
      }
      
      
     /* var m3 = new  Array[Int](count)
      var l3 = 0
      objTree.calcRadius
      //objTree.printTree
      var clusters = objTree.getSummary(thres ,0)
      var t4 = System.currentTimeMillis()
      
      println("SCT size : "+clusters.size)
      println("SCT time : "+(t4-t3)/1000)
      
       val h = new HClusteringVPTree[Cluster]
       h.clustering(0,thres, clusters.to[ArrayBuffer])
      
       var t5 = System.currentTimeMillis()
       println("SCT-HC size: "+h.clusters.size)
       println("SCT-HC time : " +(t5-t3)/1000)*/
       
      /*val bSct = new BufferedWriter(new FileWriter(new File("output/label.txt")))
      
      clusters.foreach(x =>{
        x.members.foreach(y =>{m3(y)= l3})
            l3+=1})
      m3.foreach( x => bSct.write(x+"\n"))
      bSct.close()*/
  }
}
object VDJTest extends App {
  val vdj = new VDJprocess()
  //vdj.processTop10(0.15)
  //vdj.processTop10Time(0.14)
  
  
  
  //testSct
  vjProcess
  //smallClus
  def testSct = {
    /*var seqFile = args(0)
    var thresSct = args(1).toDouble
    var thres = args(2).toDouble
    var distMethod = args(3).toInt*/
    
    var seqFile ="data/bigG2k.fasta"
    var thresSct = 0.25
    var distMethod = 0
    vdj.testSct(seqFile, thresSct, distMethod)
  }
  def vjProcess = {
    /*val spark = SparkSession.builder().appName("parmst").master("local").getOrCreate()
    val data = "data/gidSeq/gidSeqD3.tsv"
    val p = 10
    val outfile = "label"
    val method = 2
    val thres = 0.14*/
    val spark = SparkSession.builder().appName("parmst").getOrCreate()
    var data = args(0)
    val p = args(1).toInt
    val outfile = args(2)
    val method = args(3).toInt
    val thres = args(4).toDouble 
    
    
    val t1 = System.currentTimeMillis()
    val sc = spark.sparkContext
   
    val rdd = sc.textFile(data,p)
    vdj.processGidSeq(thres,rdd,outfile,method)
    
    val t2 = System.currentTimeMillis()
    
    println("algorithm run time: " + (t2-t1)/1000)
  }
  
  def smallClus = {
    var seqFile = "data/c50shuf.fasta"
    val seqs = new ArrayBuffer[Sequence]
    var count =0
    for (pairs<- Source.fromFile(seqFile).getLines().sliding(2,2)){
      seqs += new Sequence(count, pairs.head, pairs.last)
      count += 1
    }
    
    var clus = vdj.clustSmall(seqs, 0.08)
    var sum = 0
    clus.foreach( x => {x.members.foreach(println);println("======"); sum+=x.members.size} )
    println ("sum is : "+sum)
  }
  
}
