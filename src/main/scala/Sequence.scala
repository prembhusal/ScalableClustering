//
class Sequence (var id:Int, var label:String, var seq:String) extends  Serializable with Measurable {
  // seq should be in upper case 
  
  def distance(s: Any) = 
   s match {
      case s :Sequence => if (Config.dist == 0) hamming(s)  else edit_distance(s).toDouble
      case _ => println("distance with non-sequence type"); -1
    }

  def length = seq.length
  override def equals(that : Any):Boolean = 
    that match {
      case that: Sequence => seq == that.seq // cares about the actual sequence only
      case _ => false
    }

  private def edit_distance(s:Sequence): Int = { // use the distance method only
    val m = seq.size
    val n = s.seq.size

    if(m==0) return n
    if(n==0) return m

    val costs = new Array[Int]( n+1 )
    for ( k <- 0 to n ) costs(k) = k
    
    var i = 0
    
    for(it1 <- 0  until m){
        costs(0) = i+1
        var corner = i
        var j = 0
        for(it2 <- 0 until n){
            val upper = costs(j+1);
            if(seq(it1) == s.seq(it2)) costs(j+1) = corner
            else{
                val t = if (upper<corner) upper else corner
                costs(j+1)= if (costs(j)<t) costs(j)+1 else t+1
            }
            corner = upper
            j += 1 
        }
        i += 1
    }
    
    costs(n)
  }
  private def hamming(s:Sequence): Double = {
    //normalized hamming distance with length
    if (seq.length != s.seq.length) throw new IllegalArgumentException()
    seq.zip(s.seq).count(pair => pair._1 != pair._2)/s.seq.length.toDouble
  }

  def toJSON:Js.Obj = Js.Obj("id" -> Js.Num(id),
      "label" -> Js.Str(label),
      "seq" -> Js.Str(seq))
  
  def fromJSON(obj:Js.Obj): Unit = {
    id = obj("id").num.toInt
    label = obj("label").str
    seq = obj("seq").str
  }
}
