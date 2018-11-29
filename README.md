# kettle-beam
Kettle plugins for Apache Beam

## First

build/install project kettle-beam-core

    https://github.com/mattcasters/kettle-beam-core


## Build

mvn clean install

## Install

* Create a new directory called kettle-beam in <PDI>plugins/ 
* Copy target/kettle-beam-<version>.jar to <PDI>/plugins/kettle-beam/
* Copy the other jar files in target/lib to <PDI>/lib
  
  I know it's dirty, fixing it later
  
## Configure

Describe the file layout for the input and output of your pipeline using : 
    
    Spoon menu Beam / Create a file definition

Specify this file layout in your "Beam Input" and "Beam Output" steps.

## Supported

* Only straight line between Beam Input and Output is supported.  
* Sort and Group by are not yet supported.
* External plugins are not yet supported.


