import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

class Neo4jCypherExceptions extends spock.lang.Specification {

  // class
  @Shared GraphDatabaseService graphDb
  @Shared ExecutionEngine engine

  // instance
  String cypher
  ExecutionResult er
  def result = []

  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
  }

  def "invalid cypher syntax"() {

  setup: "invalid cypher malformed query"
    cypher = "CRETE (n {name : 'Kant'})"

  when: "execute query and throw exception"
    er = engine.execute(cypher)

  then: "validate correct exception thrown"
    thrown org.neo4j.cypher.SyntaxException 

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
    er = engine.execute(cypherCreate)
    er = engine.execute(cypherQuery)
    result = er.columnAs("kant").toList()
    // accessing properties outside of transaction throws exeception
    // all iterators get closed automatically when using 
    // implicit cypher transactions
    println result.first().getProperty("lastName")

  then: "validate correct exception thrown"
    thrown org.neo4j.graphdb.NotInTransactionException 

  }

  def "transaction bug?"() {
  /* 
   * just select gives NotInTransactionException 
   * no transaction should be created
   * I think this is a bug in 2.0.0M4
   * creating test to detect if it is fixed
   */

  setup: "get philosophers"
    def cypher = """
      MATCH p:Philosopher
      RETURN p as philosopher
    """

  when: "execute query and throw exception"
    er = engine.execute(cypher)
    // accessing properties outside of transaction throws exeception
    result = er.columnAs("philosopher").toList().collect { it.getProperty("name") }.sort()

  then: "validate correct exception thrown"
    thrown org.neo4j.graphdb.NotInTransactionException 

  }

  def cleanupSpec() {
    graphDb.shutdown()
  }
}  
