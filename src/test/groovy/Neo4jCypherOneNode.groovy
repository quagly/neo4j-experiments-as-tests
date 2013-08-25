import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

class Neo4jCypherOneNode extends spock.lang.Specification {

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
 
  def "create one node"() {

  setup: "query to create one node with one property"
    cypher = """
      CREATE (n {name : 'Immanuel'})
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics

  then: "validate expected stats"
    qs.containsUpdates()
    qs.nodesCreated == 1 
    qs.propertiesSet == 1 

  }

  def "add property to node"() {

    setup: "query to add property to node"
    cypher = """
      START n=node(*)
      WHERE n.name = 'Immanuel'
      SET n.lastName = 'Kant'
      RETURN n as kant 
    """
    Transaction tx
    def lastName

    when: "execute query and capture stats"
    /*
     * new in 2.0.0M4 is mandatory transactions
     * cypher will automatically create a transaction as neccessary
     * however, the result will be closed when the transaction is 
     * automatically closed so cannot be tested
     * I believe that two cypher queries could be simpler
     * one to modify, and one to query
     * then explicit transactions could be avoided
     */
    tx = graphDb.beginTx()
    try {
      er = engine.execute(cypher)
      qs = er.queryStatistics
      result = er.columnAs("kant").toList()
      lastName = result.first().getProperty("lastName")
      tx.success()
    } finally {
      tx.finish()
    }

    then: "one property updated and lastName is Kant"
    qs.containsUpdates()
    qs.propertiesSet == 1
    result.size == 1
    lastName.equals('Kant')
  }

  def "remove name property and add firstName property"() {

    setup: "query to add and remove property from node"
    cypher = """
      START n=node(*)
      WHERE n.name = 'Immanuel'
      SET n.firstName = 'Immanuel'
      REMOVE n.name
      RETURN n as kant 
    """
    Transaction tx
    Boolean hasName
    def firstName

    when: "execute query and capture stats"
    tx = graphDb.beginTx()
    try {
      er = engine.execute(cypher)
      qs = er.queryStatistics
      result = er.columnAs("kant").toList()
      hasName = result.first().hasProperty("name")
      firstName = result.first().getProperty("firstName")
      tx.success()
    } finally {
      tx.finish()
    }

    then: "name property removed and firstName added"
    qs.containsUpdates()
    qs.propertiesSet == 2
    result.size == 1
    ! hasName 
    firstName.equals('Immanuel')
  }

  def "use AND HAS and RegEx"() {

    setup: "query to filter with AND HAS and RegEx"
    cypher = """
      START n=node(*)
      WHERE
        HAS(n.firstName)           // test existence
        AND n.firstName = 'Immanuel'  // equal
        AND n.lastName =~ '^K.*'   // RegEx
        AND n.firstName =~ '(?i)immANuEl'      // case insensitive RegEx
        AND n.lastName IN [ 'Kant', 'Kan', 'NoIKant', 'YesIKan' ]  // in list
      RETURN n as kant 
    """
    Transaction tx
    def firstName
    def lastName

    when: "execute query and capture stats"
    tx = graphDb.beginTx()
    try {
      er = engine.execute(cypher)
      qs = er.queryStatistics
      result = er.columnAs("kant").toList()
      firstName = result.first().getProperty("firstName")
      lastName = result.first().getProperty("lastName")
      tx.success()
    } finally {
      tx.finish()
    }
  
    then: "returns Immanuel Kant node"
    !qs.containsUpdates()
    qs.propertiesSet == 0 
    result.size == 1
    firstName.equals('Immanuel')
    lastName.equals('Kant')
  }

  def "delete node"() {

    setup: "query to delete node"
    cypher = """
      START n=node(*)
      WHERE n.firstName = 'Immanuel'
      DELETE n
    """

    when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
  
    then: "node deleted"
    qs.containsUpdates()
    qs.deletedNodes == 1
  }

  def cleanupSpec() {
    graphDb.shutdown()
  }
}  
