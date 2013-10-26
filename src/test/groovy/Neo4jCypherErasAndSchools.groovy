import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate more complex examples 
 * using philosophers, eras, and schools
 * examples with direct relationships
 **/
class Neo4jCypherErasAndSchools extends spock.lang.Specification {

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
          ( socrates:Philosopher  {name:'Socrates', uri: 'http://dbpedia.org/resource/Socrates' })
        , ( plato:Philosopher     {name:'Plato', uri: 'http://dbpedia.org/resource/Plato' })
        , ( aristotle:Philosopher { name: 'Aristotle' , uri: 'http://dbpedia.org/resource/Aristotle' })
        , ( ancient_era:Era       { name: 'Ancient philosophy', uri: 'http://dbpedia.org/resource/Ancient_philosophy' }) 
        , ( platonism_school:School { name: 'Platonism', uri: 'http://dbpedia.org/resource/Platonism' }) 
        , ( peripatetic_school:School { name: 'Peripatetic school', uri: 'http://dbpedia.org/resource/Peripatetic_school' }) 
        , ( ancient_greek_school:School { name: 'Ancient Greek philosophy', uri: 'http://dbpedia.org/resource/Ancient_Greek_philosophy' })
        , ( philo_tradition:SchoolType { name: 'Philosophical traditions', uri: 'http://dbpedia.org/class/yago/PhilosophicalTraditions' })
        , ( philo_movement:SchoolType { name: 'Philosophical movements', uri: 'http://dbpedia.org/class/yago/PhilosophicalMovements' })
        , ( philo_ancient_school:SchoolType { name: 'Ancient philosophical schools and traditions', uri: 'http://dbpedia.org/class/yago/AncientPhilosophicalSchoolsAndTraditions' })
        , ( tradition:SchoolType { name: 'tradition', uri: 'http://dbpedia.org/class/yago/Tradition105809745' })
        , ( movement:SchoolType { name: 'movement', uri: 'http://dbpedia.org/class/yago/Motion100331950' })
        , ( school:SchoolType { name: 'school', uri: 'http://dbpedia.org/class/yago/School108276720' })
        , ( content:SchoolType { name: 'content', uri: 'http://dbpedia.org/class/yago/Content105809192' })
        , ( knowledge:SchoolType { name: 'knowledge', uri: 'http://dbpedia.org/class/yago/Cognition100023271' })
        , ( change:SchoolType { name: 'change', uri: 'http://dbpedia.org/class/yago/Change100191142' })
        , socrates-[:INFLUENCES]->plato
        , socrates-[:INFLUENCES]->aristotle
        , plato-[:INFLUENCES]->aristotle
        , socrates-[:MEMBER_OF]->ancient_greek_school
        , plato-[:MEMBER_OF]->platonism_school
        , aristotle-[:MEMBER_OF]->peripatetic_school
        , socrates-[:MEMBER_OF]->ancient_era
        , plato-[:MEMBER_OF]->ancient_era
        , aristotle-[:MEMBER_OF]->ancient_era
        , platonism_school-[:TYPE_OF]->philo_tradition
        , platonism_school-[:TYPE_OF]->philo_movement
        , peripatetic_school-[:TYPE_OF]->philo_movement
        , peripatetic_school-[:TYPE_OF]->philo_ancient_school
        , philo_ancient_school-[:SUBCLASS_OF]->school
        , philo_movement-[:SUBCLASS_OF]->movement
        , philo_tradition-[:SUBCLASS_OF]->tradition
        , tradition-[:SUBCLASS_OF]->content
        , content-[:SUBCLASS_OF]->knowledge
        , movement-[:SUBCLASS_OF]->change
    """
    staticQs = engine.execute(staticCypher).queryStatistics

  }

  def "validate setupSpec"() {
  expect: 
    staticQs.containsUpdates()
    staticQs.nodesCreated == 16 
    staticQs.relationshipsCreated == 19
    staticQs.propertiesSet == 32 
    staticQs.labelsAdded == 16 
  }

  def "validate nodes"() {

  setup: "query to return all nodes with label philosopher"
    cypher = """
       MATCH (p:Philosopher)
       RETURN p.name as PhilosopherNames
       ORDER BY p.name
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList()

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals(['Aristotle', 'Plato', 'Socrates'])

  }

  def "all ancient era phiosophers"() {

  setup: "query to return all ancient philosophers "
    // get philosopher with any direct relationship to 
    // ancient philosophy era
    cypher = """
       MATCH (p:Philosopher)-[]->(e:Era)
       WHERE e.name = 'Ancient philosophy' 
       RETURN p.name as PhilosopherNames
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList()

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals(['Aristotle', 'Plato', 'Socrates'])
  }

  def "all philosophers who are members of a school with a school type"() {

  setup: "query to return all school type philosophers"
    /* get philosophers who are a member of a 
     * school that has a school type 
     */
    cypher = """
       MATCH (p:Philosopher)-[:MEMBER_OF]->(s:School)-[:TYPE_OF]->(st:SchoolType)
       RETURN DISTINCT p.name as PhilosopherNames
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList()

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals(['Aristotle', 'Plato'])
  }

  def "all philosophers who are members of school of type Philosophical movements"() {

  setup: "query to return all school type philosophers"
    /* get philosophers who are a member of a 
     * school that has a school type of 'Philosophical movements' 
     */
    cypher = """
       MATCH (p:Philosopher)-[:MEMBER_OF]->(s:School)-[:TYPE_OF]->(st:SchoolType)
       WHERE st.name = 'Philosophical movements'
       RETURN p.name as PhilosopherName, s.name as SchoolName
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result 


  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals([
      [   SchoolName:'Peripatetic school'
        , PhilosopherName:'Aristotle'
      ],[ 
          SchoolName:'Platonism'
        , PhilosopherName:'Plato'
      ]
    ])

  }

  def "all philosophers who are members of school of type movements"() {

  setup: "query to return all movement school type philosophers"
    /* get philosophers who are a member of a 
     * school that has a school type of 'movements' 
     */
    cypher = """
       MATCH (p:Philosopher)-[r:MEMBER_OF]->(s:School)-[:TYPE_OF]->(st:SchoolType)-[:SUBCLASS_OF]->(st2:SchoolType)
       WHERE st2.name = 'movement'
       RETURN p.name as pName , type(r) as rType , s.name as sName
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.iterator().toList().sort()
    //println result

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.equals([
      [   pName:'Plato'
        , rType:'MEMBER_OF'
        , sName:'Platonism'
      ],[
          pName:'Aristotle'
        , rType:'MEMBER_OF'
        , sName:'Peripatetic school']
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
