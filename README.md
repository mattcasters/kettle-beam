# kettle-beam
Kettle plugins for Apache Beam

## First

build/install project kettle-beam-core

    https://github.com/mattcasters/kettle-beam-core


## Build

mvn clean install

Note you need the Pentaho settings.xml in your ~/.m2 : https://github.com/pentaho/maven-parent-poms/blob/master/maven-support-files/settings.xml

## Install

* Create a new directory called kettle-beam in \<PDI-DIR\>plugins/ 
* Copy target/kettle-beam-<version>.jar to \<PDI-DIR\>/plugins/kettle-beam/
* Copy the other jar files in target/lib to \<PDI-DIR\>/plugins/kettle-beam/lib/
  
  
## Configure

### File Definitions

Describe the file layout for the input and output of your pipeline using : 
    
    Spoon menu Beam / Create a file definition

Specify this file layout in your "Beam Input" and "Beam Output" steps.
If you do not specify the file definition in the "Beam Output" step, all fields arriving at the step will be written with comma for separator and double quotes as enclosure.  The formatting in the fields will be used.  

### Beam Job Configurations

A Beam Job configuration is needed to run your transformation on Apache Beam.
Specify which Runner to use (Direct and Dataflow are supported).  
You can use the variables to make your transformations completely generic.  For example you can set an INPUT_LOCATION location variable
* /some/folder/* for a Direct execution during testing
* gs://mybucket/input/* for an execution on GCP Dataflow


## Supported

* Input: Beam Input, Google Pub/Sub Subscribe and Google BigQuery Input
* Output: Beam Output, Google Pub/Sub Publish and Google BigQuery Output
* Windowing with the Beam Window step and adding timestamps to bounded data for streaming (Beam Timestamp)
* Sort rows is not yet supported and will never be supported in a generic sense like in Kettle.
* Group By step : experimental, SUM (Integer, Number), COUNT, MIN, MAX, FIRST (throws errors for not-supported stuff)
* Merge Join
* Stream Lookup (side loading data)
* Filter rows (including targeting steps for true/false)
* Switch/Case
* Plugin support through the Beam Job Configuration: specify which plugins to include in the runtime

## Runners
* Beam Direct : working
* Google Cloud DataFlow : working
* Apache Spark : mostly untested, configurable (feedback welcome)
* Apache Flink : not started yet, stubbed out code
* Aache Apex : not started yet, stubbed out code
* JStorm : not started yet

## More information

http://diethardsteiner.github.io/pdi/2018/12/01/Kettle-Beam.html


