import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.*
import org.neo4j.test.*

class Neo4jCypherExceptions extends spock.lang.Specification {

  // class
  @Shared GraphDatabaseService graphDb

  // instance
  String cypher
  Result er
  QueryStatistics qs
  def result = []

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

  def "invalid cypher syntax"() {

  setup: "invalid cypher malformed query"
    cypher = "CRETE (n {name : 'Kant'})"

  when: "execute query and throw exception"
    er = graphDb.execute(cypher)

  then: "validate correct exception thrown"
    thrown org.neo4j.graphdb.QueryExecutionException

  }

  def "access result outside of transaction"() {

  setup: "create and return"
    def cypherCreate = """
      CREATE (n:Philosopher {name : 'Immanuel'})
    """

    def cypherQuery = """
      START n=node(*)
      WHERE n.name = 'Immanuel'
      SET n.lastName = 'Kant'
      RETURN n as kant
    """

  when: "execute query and throw exception"
    er = graphDb.execute(cypherCreate)
    er = graphDb.execute(cypherQuery)
    result = er.columnAs("kant").toList()
    // accessing properties outside of transaction throws exeception
    // all iterators get closed automatically when using
    // implicit cypher transactions
    println result.first().getProperty("lastName")

  then: "validate correct exception thrown"
    thrown org.neo4j.graphdb.NotInTransactionException

  }

  def "transaction required for read only access"() {

  setup: "get philosophers"
    def cypher = """
      MATCH (p:Philosopher)
      RETURN p as philosopher
    """

  when: "execute query and throw exception"
    er = graphDb.execute(cypher)
    // accessing properties outside of transaction throws exeception
    result = er.columnAs("philosopher").toList().collect { it.getProperty("name") }.sort()

  then: "validate correct exception thrown"
    thrown org.neo4j.graphdb.NotInTransactionException

  }

  def "unique contraint violation"() {
    /* unique constraints are new in 2.0.0M5
     * as part of option schema feature of 2.0
     * verify exception thrown on duplicate
     */

  setup: "get philosophers"
    // define contraint
    def cypherConstraint = """
      CREATE CONSTRAINT ON (p:Philosopher)
      ASSERT p.name IS UNIQUE
    """
    // duplicate names
    def cypher = """
      CREATE
      ( Ockham:Philosopher {name:'William'})
      , ( James:Philosopher { name: 'William'})
    """

  when: "create contraint and violate it to throw exception"
    er = graphDb.execute(cypherConstraint)
    qs = er.queryStatistics
    er = graphDb.execute(cypher)

  then: "validate correct exception thrown"
    qs.constraintsAdded == 1
    thrown org.neo4j.graphdb.QueryExecutionException

  }

}
