import spock.lang.*
import org.neo4j.graphdb.*
import org.neo4j.cypher.javacompat.* 
import org.neo4j.test.*

/**
 * Demonstrate simple examples of working with paths
 * using three nodes and three relationships
 **/
class Neo4jCypherCollection extends spock.lang.Specification {

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
    engine = new ExecutionEngine( graphDb )

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

  expect: "validate expected stats"
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
    result = er.columnAs("PhilosopherNames").toList().sort()

  then: "validate expected stats"
    ! qs.containsUpdates()
    result.size() == 3
    result.equals(['Aristotle', 'Plato', 'Socrates'])

  }

  def "collect nodes"() {

  setup: "query to collect all nodes between Socrates and Aristotle inclusive"
  // shows how to collect all NODES in the path and extract thier names
    cypher = """
       MATCH p=(a)-->(b)-->(c) 
       WHERE a.name = 'Socrates' and c.name = 'Aristotle'
       RETURN EXTRACT( n IN NODES(p) | n.name )  as PhilosopherNames
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("PhilosopherNames").toList()

  then: "validate expected stats"
    ! qs.containsUpdates()
    // result is a single List of three nodes so size is one array
    // actually it is scala.collection.convert.Wrappers$SeqWrapper
    // instead of List which is why toList() is called below
    // consider converting result to list of lists in when: 
    result.size() == 1 
    result.first().toList().sort().equals(['Aristotle', 'Plato', 'Socrates'])

  }

  def "collect relationship types"() {
  // collect all relationship types for all paths.  Get distinct in code.
  // see "collect distinct relationship types" for an example of doing all
  // processing in cypher

  setup: "query to collect all Relationship Types from Philosopher Nodes to SchoolType nodes"
  // store distinct relationship types as a set
  def relTypes = [] as Set
  // shows how to collect all RELATIONSHIPS in a path and extract thier names
    cypher = """
       MATCH p=(a:Philosopher)-[*]->(b:SchoolType)
       RETURN DISTINCT EXTRACT( r in RELATIONSHIPS(p)| type(r) ) as RelationshipTypes  
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    // result is a list of lists, need to get distinct values from all lists
    result = er.columnAs("RelationshipTypes").toList()
    result.each{ relTypes.addAll(it) }
    //println relTypes.sort()

  then: "validate expected stats"
    ! qs.containsUpdates()
    relTypes.size() == 4 
    relTypes.sort().equals(['INFLUENCES', 'MEMBER_OF', 'SUBCLASS_OF', 'TYPE_OF'])
  }

  def "collect distinct relationship types"() {

  setup: "query to collect all Relationship Types from Philosopher Nodes to SchoolType nodes"
  // shows how to collect all RELATIONSHIPS for all paths and extract thier distinct types 
  // note use of reduce to simulate a nested loop 
  // and CASE to append an empty list if type exists for distinct 
    cypher = """
      MATCH p=(a:Philosopher)-[rel*]->(b:SchoolType) 
      WITH collect(rel) AS allr 
      RETURN (REDUCE(allDistR =[], rcol IN allr | 
        reduce(distR = allDistR, r IN rcol | 
          distR + CASE WHEN type(r) IN distR  THEN []  ELSE type(r) END
        )
      )) as RelationshipTypes
    """

  when: "execute query and capture stats"
    er = engine.execute(cypher)
    qs = er.queryStatistics
    result = er.columnAs("RelationshipTypes").toList()
    //println result.toList().first().toList().sort()

  then: "validate expected stats"
    ! qs.containsUpdates()
    // result contains one collection
    result.size() == 1 
    result.toList().first().toList().sort().equals(['INFLUENCES', 'MEMBER_OF', 'SUBCLASS_OF', 'TYPE_OF'])
  }

}

