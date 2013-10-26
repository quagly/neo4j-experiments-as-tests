import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate philosophers and schools 
 **/
class Neo4jCypherSameSchoolInfluence extends spock.lang.Specification {

  // class 
  @Shared GraphDatabaseService graphDb
  @Shared ExecutionEngine engine

  static String staticCypher
  static QueryStatistics staticQs

  // instance
  String cypher
  ExecutionResult er
  QueryStatistics qs
  def result = []

  def setupSpec() {
    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine  = new ExecutionEngine( graphDb )

    staticCypher = """ 
      CREATE
          ( plato:Philosopher     {name:'Plato', uri: 'http://dbpedia.org/resource/Plato' })
        , ( aristotle:Philosopher { name: 'Aristotle' , uri: 'http://dbpedia.org/resource/Aristotle' })
        , ( platonism_school:School { name: 'Platonism', uri: 'http://dbpedia.org/resource/Platonism' }) 
        , ( peripatetic_school:School { name: 'Peripatetic school', uri: 'http://dbpedia.org/resource/Peripatetic_school' }) 
        , ( ancient_greek_school:School { name: 'Ancient Greek philosophy', uri: 'http://dbpedia.org/resource/Ancient_Greek_philosophy' })
        , plato-[:INFLUENCES]->aristotle
        , plato-[:MEMBER_OF]->platonism_school
        , plato-[:MEMBER_OF]->ancient_greek_school
        , aristotle-[:MEMBER_OF]->peripatetic_school
        , aristotle-[:MEMBER_OF]->ancient_greek_school
    """

   staticQs = engine.execute(staticCypher).queryStatistics
 
  }

  def "validate SetupSpec"() {

  expect: "validate expected stats"
    staticQs.containsUpdates()
    staticQs.nodesCreated == 5 
    staticQs.relationshipsCreated == 5 
    staticQs.propertiesSet == 10 
    staticQs.labelsAdded == 5 

  }

  def "schools with philosophers with influence"() {

  setup: "query for movements that influenced with philosophers"
    /* 
     * shows how two variables can reference the same node ( school )
     * this does not always work 
     * but I do not understand when it works and when it doesn't 
     */
    cypher = """
       MATCH (s2:School)<-[:MEMBER_OF]-(p2:Philosopher)<-[:INFLUENCES]-(p1:Philosopher)-[:MEMBER_OF]->(s1:School)
       WHERE  s2 = s1 // node equality
       RETURN p1.name as p1Name, s1.name as s1Name, p2.name as p2Name, s2.name as s2Name
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 1
    result.equals([
      [  s2Name:'Ancient Greek philosophy'
       , p2Name:'Aristotle'
       , p1Name:'Plato'
       , s1Name:'Ancient Greek philosophy'
     ]
    ])
  }

}
