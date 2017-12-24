import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.*
import org.neo4j.test.*

/**
 * Demonstrate philosophers, schools and schoolType
 **/
class Neo4jCypherSameSchoolTypeInfluence extends spock.lang.Specification {
  // class
  @Shared GraphDatabaseService graphDb

  static String staticCypher
  static QueryStatistics staticQs

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

    staticCypher = """
      CREATE
          ( plato:Philosopher     {name:'Plato', uri: 'http://dbpedia.org/resource/Plato' })
        , ( aristotle:Philosopher { name: 'Aristotle' , uri: 'http://dbpedia.org/resource/Aristotle' })
        , ( platonism_school:School { name: 'Platonism', uri: 'http://dbpedia.org/resource/Platonism' })
        , ( peripatetic_school:School { name: 'Peripatetic school', uri: 'http://dbpedia.org/resource/Peripatetic_school' })
        , ( philo_tradition:SchoolType { name: 'Philosophical traditions', uri: 'http://dbpedia.org/class/yago/PhilosophicalTraditions' })
        , ( philo_movement:SchoolType { name: 'Philosophical movements', uri: 'http://dbpedia.org/class/yago/PhilosophicalMovements' })
        , ( philo_ancient_school:SchoolType { name: 'Ancient philosophical schools and traditions', uri: 'http://dbpedia.org/class/yago/AncientPhilosophicalSchoolsAndTraditions' })
        , (plato)-[:INFLUENCES]->(aristotle)
        , (plato)-[:MEMBER_OF]->(platonism_school)
        , (aristotle)-[:MEMBER_OF]->(peripatetic_school)
        , (platonism_school)-[:TYPE_OF]->(philo_tradition)
        , (platonism_school)-[:TYPE_OF]->(philo_movement)
        , (peripatetic_school)-[:TYPE_OF]->(philo_movement)
        , (peripatetic_school)-[:TYPE_OF]->(philo_ancient_school)
    """

    staticQs = graphDb.execute(staticCypher).queryStatistics

  }

  def cleanupSpec() {
    graphDb.shutdown()
    // no need to remove temporary directory, it appears that the shutdown does that
  }

  def "validate setupSpec"() {

  expect: "validate expected stats"
    staticQs.containsUpdates()
    staticQs.nodesCreated == 7
    staticQs.relationshipsCreated == 7
    staticQs.propertiesSet == 14
    staticQs.labelsAdded == 7
  }

  def "school type with philosophers with influence"() {

  setup: "query for influential philosophers from the same school type"
    /*
     * shows how two variables can reference the same node ( school type )
     * this does not always work
     * but I do not understand when it works and when it doesn't
     */
    cypher = """
       MATCH (st2:SchoolType)<-[:TYPE_OF]-(s2:School)<-[:MEMBER_OF]-(p2:Philosopher)<-[:INFLUENCES]-(p1:Philosopher)-[:MEMBER_OF]->(s1:School)-[:TYPE_OF]->(st1:SchoolType)
       WHERE  st2 = st1 // node equality
       RETURN p1.name as p1Name, s1.name as s1Name, st1.name as st1Name, p2.name as p2Name, s2.name as s2Name, st2.name as st2Name
    """

  when: "execute query and capture stats"
    er = graphDb.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 1
    result.equals([
      [ p1Name:'Plato'
       , s1Name:'Platonism'
       , st1Name:'Philosophical movements'
       , p2Name:'Aristotle'
       , s2Name:'Peripatetic school'
       , st2Name:'Philosophical movements'
      ]
    ])

  }

}
