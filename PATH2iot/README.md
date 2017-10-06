# README #

*PATHfinder* is a self-contained module of PATH2iot system. It takes a high-level declarative description of computation, set of non-functional requirements, state of the infrastructure and return an optimised (best) plan to fulfil these criteria.
 
### Functionality ###
 * EPL decomposition,
 * Non-functional requirements parsing
 * Energy model evaluation
 * Device specific compilation (coming)
   * Pebble Watch
   * iPhone
   * Esper node (via d2ESPer)

#### Requirements ####
 * configuration file (usually input/pathFinder.conf)
 * Neo4j server
 
### How do I get set up? ###

* Clone
* Compile
* Fill in the configuration files (there is a template in input/pathFinder_template.conf)
* Run

### How do I run unit tests? ###

* provide a 'neoconnectionstring' parameter as you run junit tests, e.g. ```-Dneoconnectionstring=127.0.0.1:7687``` - if neo4j server is running localy.

### Upcoming functionality ###

* support for multi-hop sxfer operator (infrastructure nodes.

### Who do I talk to? ###

* author: Peter Michalák (P.Michalak1@newcastle.ac.uk)