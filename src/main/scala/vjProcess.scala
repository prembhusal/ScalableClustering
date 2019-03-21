
import org.apache.spark.sql.SparkSession
object vjProcess extends App{
  def processVJsequences ={
    val t0 = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = spark.sparkContext
    var df = spark.read.option("sep", "\t").option("header", "true").csv("data/d1vjIndex.tsv")// reading stat file in dataframe
    val groups = df.select("V_CALL","J_CALL","JUNCTION_LENGTH","JUNCTION","Index").groupBy("V_CALL","J_CALL","JUNCTION_LENGTH");
    
    val records = df.count()
    //New dataframe which has columns IDs and JUNC_SEQS having v,j length group 
    val groupList = groups.agg(collect_list("Index").as("IDS"), collect_list("JUNCTION").as("JUNC_SEQS"))
    groupList.show(10) // shows only 10 entries
  }
  
  def getGroupsClusterParallel(thres:Double) : Unit = {
    val t0 = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = spark.sparkContext
    var df = spark.read.option("sep", "\t").option("header", "true").csv("data/d1vjIndex.tsv")// reading stat file in dataframe
     //add new column which is combination of index,id,junction
    val dfNew = df.withColumn("Seq", struct(df("Index"), df("ID"),df("JUNCTION")))
   
    val groups = dfNew.select("V_CALL","J_CALL","JUNCTION_LENGTH","seq").groupBy("V_CALL","J_CALL","JUNCTION_LENGTH");
   
    val groupList = groups.agg(collect_list("seq").as("SeqList"))
    
    val pmst = new LocalParMst[Sequence]
    
    groupList.foreach(x => {
      if ( x.getList(3).size < 2){
        //assign the label
      }
      else if( x.getList(3).size == 2) {
         var a = new ArrayBuffer[Sequence]
        for( i <- 0 to x.getList(3).size()-1){
          var record = x.getList(3).get(i).toString().replaceAll("^\\[|\\]$", "").split(",")
          val id:Int = Integer.parseInt(record(0))
          a+= new Sequence(id, record(1),record(2))
        }
      }
      else {
        var a = new ArrayBuffer[Sequence]
        for( i <- 0 to x.getList(3).size()-1){
         var record = x.getList(3).get(i).toString().replaceAll("^\\[|\\]$", "").split(",")
          val id:Int = Integer.parseInt(record(0))
          a+= new Sequence(id, record(1),record(2))
         }
        val res = pmst.cluster(a ,2,2,thres)
      }
    })
    val t1 = System.currentTimeMillis()
    println("total time :"+ ( t1-t0)/1000)
    
  }
  
  def processGidSeq( thres:Double,data:RDD[String]) = {
    
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
     val localMst = new LocalParMst[Sequence]
    
    val groups = gidMap.groupByKey.map( x => {
      val seq = x._2.toList(0).id
      if(x._2.size < 1){
        List(new DendroNode(null, null, x._2.toList(0).id, 0.0))
      }else{
       // println("group")
        localMst.cluster(x._2.to[ArrayBuffer] ,4,0,thres)
      }
    })
    
   val res = groups.collect
   
    var labelMap = new  Array[Int](count.toInt)
    var label = 0
    
    res.foreach(x => {
       for (c <- x) {
      c.getRecords.foreach(m => labelMap(m) = label)
      label = label + 1
    }
    })
    /*val bwLabel = new BufferedWriter(new FileWriter(new File("output/gidSeq")))
    labelMap.foreach( x => bwLabel.write(x+"\n"))
    bwLabel.close()*/
    println("count:"+count)
   val t2 = System.currentTimeMillis()
   println("total time : "+(t2-t1)/1000)
    
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
    var df = spark.read.option("sep", "\t").option("header", "true").csv("data/d1vjIndex.tsv")// reading stat file in dataframe
     //add new column which is combination of index,id,junction
    val dfNew = df.withColumn("Seq", struct(df("Index"), df("ID"),df("JUNCTION")))
   
    val groups = dfNew.select("V_CALL","J_CALL","JUNCTION_LENGTH","Seq").groupBy("V_CALL","J_CALL","JUNCTION_LENGTH");
    val groupList = groups.agg(collect_list("Seq").as("SeqList")).collect
    
    val top10 = groupList.sortBy(x=>x.getList(3).size).reverse.take(10)
    for (k <- 0 to 9){
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
      val bLocal = new BufferedWriter(new FileWriter(new File("output/All/d1/locMst"+k)))
      val hcvp = new BufferedWriter(new FileWriter(new File("output/All/d1/hcvp"+k)))
      val bSct = new BufferedWriter(new FileWriter(new File("output/All/d1/sctMin"+k)))
      val bSctVp = new BufferedWriter(new FileWriter(new File("output/All/d1/sctMinVp"+k)))
     
     
     //label local mst
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
       val h = new HClusteringVPTree[Sequence]
       h.clustering(0,thres, a)
       var m2 = new  Array[Int](count)
       var l2 = 0
       println("vp-size :"+h.clusters.size)
       h.clusters.foreach(x =>{
        x.members.foreach(y =>m2(y)= l2)
            l2+=1})
      m2.foreach(l => hcvp.write(l+"\n"))
      hcvp.close()
      
      //label sct :
      var fanout = 10
      var threshold = thres
      var maxlen = 270
      var centerMethod = 0
      var distMethod = 3
      var debug = 0
      var summaryLevel = -1
      var secondStage = 1
       
      var objTree = new SeqCondenseTree(fanout, threshold, maxlen, centerMethod, distMethod, debug)
      a.foreach(x => objTree.insert(x))
     
      var m3 = new  Array[Int](count)
      var l3 = 0
      //objTree.calcRadius
      var clusters = objTree.getSummary(summaryLevel)
      
      clusters.foreach(x =>{
        x.members.foreach(y =>{m3(y)= l3})
            l3+=1})
      m3.foreach( x => bSct.write(x+"\n"))
      bSct.close()
      
      var m4 = new  Array[Int](count)
      var l4 = 0
      val h1 = new HClusteringVPTree[Cluster]
       h1.clustering(0,thres, clusters.to[ArrayBuffer])
       
       
       println("SCT-vp size :"+h1.clusters.size)
       h1.clusters.foreach(x =>{
        x.members.foreach(y =>{m4(y)= l4})
            l4+=1})      
      
      m4.foreach( x => bSctVp.write(x+"\n"))
      bSctVp.close() 
      
    }
       
  }
