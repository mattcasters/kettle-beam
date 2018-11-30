# kettle-beam
Kettle plugins for Apache Beam

## First

build/install project kettle-beam-core

    https://github.com/mattcasters/kettle-beam-core


## Build

mvn clean install

Note you need the Pentaho settings.xml in your ~/.m2 : https://github.com/pentaho/maven-parent-poms/blob/master/maven-support-files/settings.xml

## Install

* Create a new directory called kettle-beam in <PDI>plugins/ 
* Copy target/kettle-beam-<version>.jar to <PDI>/plugins/kettle-beam/
* Copy the other jar files in target/lib to <PDI>/lib
  
  I know it's dirty, fixing it later
  
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

* Only straight line between Beam Input and Output is supported.  
* Sort and Group by are not yet supported.
* Group By step : experimental, SUM (Integer, Number), COUNT
* External plugins are not yet supported.


