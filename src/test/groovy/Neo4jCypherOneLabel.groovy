import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

class NeoCypherOneLabel extends spock.lang.Specification {
  // class
  @Shared GraphDatabaseService graphDb
  @Shared ExecutionEngine engine

  // instance
  String cypher
  def result = [] 
  ExecutionResult er
  QueryStatistics qs

  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
  }
 
  def "create Plato"() {

  setup: "query to create plato with philosopher label"
    cypher = """
      CREATE (n:Philosopher { name : 'Plato' , url : 'http://dbpedia.org/resource/Plato' })
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "create Aristotle"() {

  setup: "query to create Aristotle with philosopher label"
    cypher = """
      CREATE (n:Philosopher { name : 'Aristotle' , url : 'http://dbpedia.org/resource/Aristotle' })
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 2
    qs.labelsAdded == 1

  }

  def "get All Philosphers"() {
    // This query makes no changes but still requires a transaction
    // as of 2.0.0M4 which introduced mandatory transactions
    // perhaps this is a bug?

  setup: "query to return all nodes with label philosopher"
    cypher = """
      MATCH p:Philosopher
      RETURN p as philosopher
  """
  Transaction tx
  def philosophers = []

  when: "execute query and capture stats"
    tx = graphDb.beginTx()
    try {
      er = engine.execute(cypher)
      qs = er.queryStatistics
      philosophers = er.columnAs("philosopher").toList().collect { it.getProperty("name") }.sort()
      tx.success()
    } finally {
      tx.finish()
    }
  then: "validate expected stats"
    ! qs.containsUpdates()
    philosophers.size == 2
    philosophers.equals([ 'Aristotle', 'Plato' ])
  }

}
