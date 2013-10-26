import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate philosophers, schools, schoolType, and schoolTypeClass 
 **/
class Neo4jCypherSameSchoolTypeClassInfluence extends spock.lang.Specification {
   
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
        , ( philo_tradition:SchoolType { name: 'Philosophical traditions', uri: 'http://dbpedia.org/class/yago/PhilosophicalTraditions' })
        , ( philo_movement:SchoolType { name: 'Philosophical movements', uri: 'http://dbpedia.org/class/yago/PhilosophicalMovements' })
        , ( philo_ancient_school:SchoolType { name: 'Ancient philosophical schools and traditions', uri: 'http://dbpedia.org/class/yago/AncientPhilosophicalSchoolsAndTraditions' })
        , ( tradition:SchoolType { name: 'tradition', uri: 'http://dbpedia.org/class/yago/Tradition105809745' })
        , ( movement:SchoolType { name: 'movement', uri: 'http://dbpedia.org/class/yago/Motion100331950' })
        , ( school:SchoolType { name: 'school', uri: 'http://dbpedia.org/class/yago/School108276720' })
        , plato-[:INFLUENCES]->aristotle       
        , plato-[:MEMBER_OF]->platonism_school
        , aristotle-[:MEMBER_OF]->peripatetic_school
        , platonism_school-[:TYPE_OF]->philo_tradition
        , platonism_school-[:TYPE_OF]->philo_movement
        , peripatetic_school-[:TYPE_OF]->philo_movement
        , peripatetic_school-[:TYPE_OF]->philo_ancient_school
        , philo_ancient_school-[:SUBCLASS_OF]->school
        , philo_movement-[:SUBCLASS_OF]->movement
        , philo_tradition-[:SUBCLASS_OF]->tradition
    """

    staticQs = engine.execute(staticCypher).queryStatistics

  }

  def "validate setupSpec"() {

  expect: "validate expected stats"
    staticQs.containsUpdates()
    staticQs.nodesCreated == 10 
    staticQs.relationshipsCreated == 10 
    staticQs.propertiesSet == 20 
    staticQs.labelsAdded == 10 
  }

  def "philosophers with school type class"() {

  setup: "query for philosophers with school type class"
    cypher = """
       MATCH (p1:Philosopher)-[:MEMBER_OF]->(s1:School)-[:TYPE_OF]->(st1:SchoolType)-[:SUBCLASS_OF]->(stc1:SchoolType)
       WHERE  stc1.name = 'movement'
       RETURN p1.name as p1Name, s1.name as s1Name, st1.name as st1Name, stc1.name as stc1Name
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result 

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 2
    result.equals([
        [st1Name:'Philosophical movements', p1Name:'Plato', s1Name:'Platonism', stc1Name:'movement']
      , [st1Name:'Philosophical movements', p1Name:'Aristotle', s1Name:'Peripatetic school', stc1Name:'movement']
    ])

  }

  def "philosophers from the same school type with influence"() {

  setup: "query for influential philosophers from the same school type"
    cypher = """
       MATCH (st2:SchoolType)<-[:TYPE_OF]-(s2:School)<-[:MEMBER_OF]-(p2:Philosopher)<-[:INFLUENCES]-(p1:Philosopher)-[:MEMBER_OF]->(s1:School)-[:TYPE_OF]->(st1:SchoolType)
       WHERE  st2 = st1 // node equality
       RETURN p1.name as p1Name, s1.name as s1Name, st1.name as st1Name, p2.name as p2Name, s2.name as s2Name, st2.name as st2Name
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
      [ p1Name:'Plato'
       , s1Name:'Platonism'
       , st1Name:'Philosophical movements'
       , p2Name:'Aristotle'
       , s2Name:'Peripatetic school'
       , st2Name:'Philosophical movements'
      ]
    ])
       
  }

  def "philosophers from the same school type class with influence"() {
  /* when testing for the same school type class cannot use just a MATCH.
   * need to filter that the school type class is the same on the where clause
   * see stackoverflow 
   * http://stackoverflow.com/questions/17536904/cypher-query-returns-nothing-but-data-is-there
   */
  setup: "query for influential philosophers from the same school type class"

    cypher = """
       MATCH p=(st2:SchoolType)<-[:TYPE_OF]-(s2:School)<-[:MEMBER_OF]-(p2:Philosopher)<-[:INFLUENCES]-(p1:Philosopher)-[:MEMBER_OF]->(s1:School)-[:TYPE_OF]->(st1:SchoolType)-[:SUBCLASS_OF]->(stc:SchoolType)
       WHERE (stc)<-[:SUBCLASS_OF]-(st2)
       RETURN p1.name as p1Name, s1.name as s1Name, st1.name as st1Name, p2.name as p2Name, s2.name as s2Name, st2.name as st2Name, stc.name as stcName
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
      [  s1Name:'Platonism'
       , p1Name:'Plato'
       , st2Name:'Philosophical movements'
       , s2Name:'Peripatetic school'
       , stcName:'movement'
       , p2Name:'Aristotle'
       , st1Name:'Philosophical movements'
     ]
   ])

  }

}
