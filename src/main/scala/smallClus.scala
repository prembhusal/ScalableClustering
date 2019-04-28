
def clustSmall( records: ArrayBuffer[Sequence], thres:Double) :List[Cluster] = {
     var  clusters  : List[Cluster] = List()
     var seqListMap =scala.collection.mutable.Map[ Sequence, ListBuffer[Int]]() 
       
     if ( records.size ==2){
       var dist = records(0).distance(records(1))
       if( dist < thres){
         clusters = List(new Cluster(dist,records(0), records.map(x => x.id).to[ListBuffer], ListBuffer(records(0)))) 
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
