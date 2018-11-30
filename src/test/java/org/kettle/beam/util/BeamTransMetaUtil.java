package org.kettle.beam.util;

import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.List;

public class BeamTransMetaUtil {

  public static final TransMeta generateBeamInputOutputTransMeta( String transname, String inputStepname, String outputStepname, IMetaStore metaStore ) throws Exception {

    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE );
    FileDefinition customerFileDefinition = createCustomersInputFileDefinition();
    factory.saveElement( customerFileDefinition );

    TransMeta transMeta = new TransMeta(  );
    transMeta.setName( transname );
    transMeta.setMetaStore( metaStore );

    // Add the input step
    //
    BeamInputMeta beamInputMeta = new BeamInputMeta();
    beamInputMeta.setInputLocation( "/tmp/customers/input/customers-100.txt" );
    beamInputMeta.setFileDescriptionName( customerFileDefinition.getName() );
    StepMeta beamInputStepMeta = new StepMeta(inputStepname, beamInputMeta);
    beamInputStepMeta.setStepID( "BeamInput" );
    transMeta.addStep( beamInputStepMeta );


    // Add a dummy in between to get started...
    //
    DummyTransMeta dummyTransMeta = new DummyTransMeta();
    StepMeta dummyStepMeta = new StepMeta("Dummy", dummyTransMeta);
    transMeta.addStep( dummyStepMeta );
    transMeta.addTransHop(new TransHopMeta( beamInputStepMeta, dummyStepMeta ) );


    // Add the output step
    //
    BeamOutputMeta beamOutputMeta = new BeamOutputMeta();
    beamOutputMeta.setOutputLocation( "/tmp/customers/output/" );
    beamOutputMeta.setFileDescriptionName( customerFileDefinition.getName() );
    beamOutputMeta.setFilePrefix( "customers" );
    beamOutputMeta.setFileSuffix( ".csv" );
    beamOutputMeta.setWindowed( false ); // Not yet supported
    StepMeta beamOutputStepMeta = new StepMeta(outputStepname, beamOutputMeta);
    beamOutputStepMeta.setStepID( "BeamOutput" );
    transMeta.addStep( beamOutputStepMeta );
    transMeta.addTransHop(new TransHopMeta( dummyStepMeta, beamOutputStepMeta ) );

    return transMeta;
  }

  public static final TransMeta generateBeamGroupByTransMeta( String transname, String inputStepname, String outputStepname, IMetaStore metaStore ) throws Exception {

    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE );
    FileDefinition customerFileDefinition = createCustomersInputFileDefinition();
    factory.saveElement( customerFileDefinition );

    TransMeta transMeta = new TransMeta(  );
    transMeta.setName( transname );
    transMeta.setMetaStore( metaStore );

    // Add the input step
    //
    BeamInputMeta beamInputMeta = new BeamInputMeta();
    beamInputMeta.setInputLocation( "/tmp/customers/input/customers-100.txt" );
    beamInputMeta.setFileDescriptionName( customerFileDefinition.getName() );
    StepMeta beamInputStepMeta = new StepMeta(inputStepname, beamInputMeta);
    beamInputStepMeta.setStepID( "BeamInput" );
    transMeta.addStep( beamInputStepMeta );


    // Add a dummy in between to get started...
    //
    MemoryGroupByMeta memoryGroupByMeta = new MemoryGroupByMeta();
    memoryGroupByMeta.allocate(1, 2 );
    memoryGroupByMeta.getGroupField()[0] = "state";
    // count(id)
    memoryGroupByMeta.getAggregateField()[0] = "nrIds";
    memoryGroupByMeta.getSubjectField()[0] = "id";
    memoryGroupByMeta.getAggregateType()[0] = MemoryGroupByMeta.TYPE_GROUP_COUNT_ALL;
    // sum(id)
    memoryGroupByMeta.getAggregateField()[1] = "sumIds";
    memoryGroupByMeta.getSubjectField()[1] = "id";
    memoryGroupByMeta.getAggregateType()[1] = MemoryGroupByMeta.TYPE_GROUP_SUM;

    StepMeta memoryGroupByStepMeta = new StepMeta("Group By", memoryGroupByMeta);
    transMeta.addStep( memoryGroupByStepMeta );
    transMeta.addTransHop(new TransHopMeta( beamInputStepMeta, memoryGroupByStepMeta ) );

    // Add the output step
    //
    BeamOutputMeta beamOutputMeta = new BeamOutputMeta();
    beamOutputMeta.setOutputLocation( "/tmp/customers/output/" );
    beamOutputMeta.setFileDescriptionName( null );
    beamOutputMeta.setFilePrefix( "grouped" );
    beamOutputMeta.setFileSuffix( ".csv" );
    beamOutputMeta.setWindowed( false ); // Not yet supported
    StepMeta beamOutputStepMeta = new StepMeta(outputStepname, beamOutputMeta);
    beamOutputStepMeta.setStepID( "BeamOutput" );
    transMeta.addStep( beamOutputStepMeta );
    transMeta.addTransHop(new TransHopMeta( memoryGroupByStepMeta, beamOutputStepMeta ) );

    return transMeta;
  }


  public static FileDefinition createCustomersInputFileDefinition() {
    FileDefinition fileDefinition = new FileDefinition();
    fileDefinition.setName( "Customers" );
    fileDefinition.setDescription( "File description of customers.txt" );

    // id;name;firstname;zip;city;birthdate;street;housenr;stateCode;state

    fileDefinition.setSeparator( ";" );
    fileDefinition.setEnclosure( null ); // NOT SUPPORTED YET

    List<FieldDefinition> fields = fileDefinition.getFieldDefinitions();
    fields.add( new FieldDefinition( "id", "Integer", 9, 0, " #" ) );
    fields.add( new FieldDefinition( "name", "String", 50, 0 ) );
    fields.add( new FieldDefinition( "firstname", "String", 50, 0 ) );
    fields.add( new FieldDefinition( "zip", "String", 20, 0 ) );
    fields.add( new FieldDefinition( "city", "String", 100, 0 ) );
    fields.add( new FieldDefinition( "birthdate", "Date", -1, -1, "yyyy/MM/dd" ) );
    fields.add( new FieldDefinition( "street", "String", 100, 0 ) );
    fields.add( new FieldDefinition( "housenr", "String", 20, 0 ) );
    fields.add( new FieldDefinition( "stateCode", "String", 10, 0 ) );
    fields.add( new FieldDefinition( "state", "String", 100, 0 ) );

    return fileDefinition;
  }

}