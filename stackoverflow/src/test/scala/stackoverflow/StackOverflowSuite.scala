package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import stackoverflow.StackOverflow.sc

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  val rawPostings = testObject.rawPostings(sc.textFile("../../src/main/resources/stackoverflow/stackoverflow.csv"))
  val groupedPostings = testObject.groupedPostings(rawPostings)
  val scoredPostings = testObject.scoredPostings(groupedPostings)
  val vectors = testObject.vectorPostings(scoredPostings).cache()

  test("grouped postings") {
    groupedPostings.takeSample(true,20,0) foreach {
      case (qid: Int, qapairs) => qapairs foreach {
        case (q: Posting, a: Posting) => {
          assert(qid === q.id, "grouped question id must match key")
          assert(Some(qid) === a.parentId, "grouped answer's parentID must match key")
        }
      }
    }
  }

  test("scored postings are all questions") {
    scoredPostings.takeSample(true, 20, 0) foreach (p =>
      assert(p._1.postingType === 1, "scored posting must be a question") )
  }

  test("given results contained in scored postings") {
    val myScoredSample = (scoredPostings.filter {case ((p,_)) => List(6,42,72,126,174) contains p.id}
      ).collectAsMap

    assert( myScoredSample( Posting(1,6,None,None,140,Some("CSS")) ) === 67 )
    assert( myScoredSample( Posting(1,42,None,None,155, Some("PHP"))) === 89 )
    assert( myScoredSample( Posting(1,72,None,None,16,Some("Ruby"))) === 3 )
    assert( myScoredSample( Posting(1,126,None,None,33,Some("Java"))) === 30 )
    assert( myScoredSample( Posting(1,174,None,None,38,Some("C#"))) === 20 )
  }

  test("given results contained in vectors") {
    assert( vectors.lookup(350000) contains 67 )
    assert( vectors.lookup(100000) contains 89 )
    assert( vectors.lookup(300000) contains 3 )
    assert( vectors.lookup(50000) contains 30 )
    assert( vectors.lookup(200000) contains 20 )
  }

  val initialMeans = testObject.sampleVectors(vectors)
  println("**** Clusters before running k-means: ****")
  testObject.printResults(testObject.clusterResults(initialMeans, vectors))

  println("**** Clusters after one iteration: ****")
  val means1 = testObject.kmeans(initialMeans, vectors,
    testObject.kmeansMaxIterations, false)
  testObject.printResults(testObject.clusterResults(means1, vectors))


}
