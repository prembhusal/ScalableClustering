
package sct
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Stack

import scala.io.Source
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import sct.vptree._
import java.io._

class NNChain {
  
    /**
     * Clusters the elements represented as symmetric adjacency matrix.  Values in
     * {@code adj} represent the similarity between any two points using a
     * symmetric similarity metric.  This returns sets of points assigned to the
     * same cluster.
     */
    def cluster(adj: Array[Array[Double]],
                numClusters: Int) = {
        // A mapping from cluster id's to their point sets.
        val clusterMap = new HashMap[Int, HashSet[Int]]()

        // A set of clusters to be considered for merging.
        val remaining = new HashSet[Int]()

        // Create a cluster for every data point and add it to the cluster map
        // and to the examine set.
        for (r <- 0 until adj.size) {
            remaining.add(r)
            clusterMap(r) = HashSet(r)
        }

        // Create a stack to represent the nearest neighbor.  The tuple stores
        // the similarity from this node to it's parent in the chain and the id
        // of the current node.
        val chain = new Stack[(Double, Int)]()

        // Initializes the chain.
        initializeChain(chain, remaining);

        while (clusterMap.size > numClusters) {
            // Get the last link in the chain.
            val (parentSim, current) = chain.top

            // Find the nearest neighbor using the clusters not in the chain
            // already.
            val (linkSim, next) = findBest(remaining, adj, current)

            // Check the similarity for the best neighbor and compare it to that of
            // the current node in the chain.  If the neighbor sim is larger, then
            // the current node and it's parent aren't RNNs.  Otherwise, the current
            // node is RNNs with it's parent.
            if (linkSim > parentSim) {
                // Not RNNs.  So push the best node and it's similarity to the
                // current node onto the chain and remove it from the examine
                // set.
                chain.push((linkSim, next))
                remaining.remove(next)
            } else {
                // Yes top two on stack are RNNs.
                
                // Pop the current node from the top. 
                chain.pop

                // Pop the parent of the best node.
                val (_, parent) = chain.pop

                // Remove the current and parent clusters from the cluster map
                // and extract the sizes.
                val (c1Points, c1Size) = removeCluster(clusterMap, current)
                val (c2Points, c2Size) = removeCluster(clusterMap, parent)
                val total = c1Size + c2Size

                // Update the similarity between the new merged cluster and all
                // other existing clusters.  We can do this in constant time by
                // computing the weighted average of the similarities to to the
                // constituent clusters.  For other agglomerative criteria, this
                // method includes different terms and different scaling values,
                // but is otherwise computable in constant time.
                for (key <- clusterMap.keys) {
                    val s1 = adj(current)(key)
                    val s2 = adj(parent)(key)
                    val newSim = (c1Size*s1 + c2Size*s2) / total
                    adj(current)(key) = newSim
                }

                // Replace the mapping from current to now point to the merged
                // cluster and add current back into the set of remaining
                // clusters so that it's compared to nodes in the chain.
                clusterMap(current) = c1Points ++ c2Points
                remaining.add(current)

                // If the chain is now empty, re-initialize it.
                if (chain.size == 0)
                    initializeChain(chain, remaining)
            }
        }

        // Once we found the right number of clusters, simply return the list of
        // point sets in a list.
        clusterMap.values.toList.foreach(println)
        
        
    }

    /**
     * Adds an arbitrary node to the neighbor chain and removes it from the set
     * of remaining clusters.  For simplicity, we just use the most easily
     * accesible element in {@code remaining}.
     */ 
    def initializeChain(chain: Stack[(Double, Int)], remaining: HashSet[Int]) {
        chain.push((-2.0, remaining.last))
        remaining.remove(remaining.last)
    }

    /**
     * Computes the distance between {@code current} and each cluster in {@code
     * remaining} and returns the cluster with the largest similarity to {@code
     * current}.
     */
    def findBest(remaining: HashSet[Int],
                 adj: Array[Array[Double]],
                 current: Int) = 
        remaining.map(i => (adj(current)(i), i)).max

    def removeCluster(clusterMap: HashMap[Int, HashSet[Int]], id: Int) = {
        val s = clusterMap.remove(id).get
        (s, s.size)
}
}

object NNC extends App {
  val nnc = new NNChain();
  val seqFile = "data/d4.fasta"
    val k = 0
    val thres = 0.06
    
    //val seqFile = "data/c50.fasta"
    val seqs = new ArrayBuffer[Sequence]
    var count =0
    for (pairs<- Source.fromFile(seqFile).getLines().sliding(2,2)){
      seqs += new Sequence(count, pairs.head, pairs.last)
      count += 1
    }
  var t1 = System.currentTimeMillis()
  var n = seqs.size
  var distanceMatrix = Array.ofDim[Double](n,n)
  for (i <- 0 to n-1){
    for (j<- 0 to n-1)
        distanceMatrix(i)(j) = seqs(i).distance(seqs(j))
  }
  
  //distanceMatrix.foreach(x => {x.foreach(print);println; println})
  nnc.cluster(distanceMatrix, 10)
  
  var t2 = System.currentTimeMillis()
  
  print("time by NNC :" +(t2-t1)/1000)
}
