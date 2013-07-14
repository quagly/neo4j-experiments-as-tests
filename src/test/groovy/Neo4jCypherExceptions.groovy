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

  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine( graphDb )
  }

  def "invalid cypher syntax"() {

  setup: "invalid cypher malformed query"
    cypher = "CRETE (n {name : 'Mike'})"

  when: "execute query and throw exception"
    er = engine.execute(cypher)

  then: "validate correct exception thrown"
    thrown org.neo4j.cypher.SyntaxException 

  }

  def cleanupSpec() {
    graphDb.shutdown()
  }
}  
