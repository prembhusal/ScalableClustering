
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
}
