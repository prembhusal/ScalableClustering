
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
}
