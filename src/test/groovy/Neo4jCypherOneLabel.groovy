import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.*
import org.neo4j.test.*

class Neo4jCypherOneLabel extends spock.lang.Specification {
  // class
  @Shared GraphDatabaseService graphDb

  // instance
  String cypher
  def result = []
  Result er
  QueryStatistics qs

  def setupSpec() {
    // for parallel execution of tests, each test class needs its own test database
    // or locking issues will occur
    // see https://neo4j.com/docs/java-reference/current/

    def ostempdir = System.getProperty('java.io.tmpdir')
    // Get the executed script as a (java) File
    File scriptFile = new File(getClass().protectionDomain.codeSource.location.path)
    // This holds the file name like "myscript.groovy"
    def scriptName = scriptFile.getName()
    def suffix = scriptName.take(scriptName.lastIndexOf('.'))
    File tempDir = new File(ostempdir, suffix)

    // better to create a testDirectory utility that supplies a test directory
    // graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase( testDirectory.directory() )
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase( tempDir )
  }

  def cleanupSpec() {
    graphDb.shutdown()
    // no need to remove temporary directory, it appears that the shutdown does that
  }

  def "create Plato"() {

  setup: "query to create plato with philosopher label"
    cypher = """
      CREATE (n:Philosopher { name : 'Plato' , url : 'http://dbpedia.org/resource/Plato' })
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
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
    er = graphDb.execute(cypher)
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
      MATCH (p:Philosopher)
      RETURN p as philosopher
  """
  // Transaction tx
  def philosophers = []

  when: "execute query and capture stats"
    Transaction tx = graphDb.beginTx()
    try {
      er = graphDb.execute(cypher)
      qs = er.queryStatistics
      philosophers = er.columnAs("philosopher").toList().collect { it.getProperty("name") }.sort()
      tx.success()
   } finally {
     tx.close()
   }
  then: "validate expected stats"
    ! qs.containsUpdates()
    philosophers.size == 2
    philosophers.equals([ 'Aristotle', 'Plato' ])
  }

}
