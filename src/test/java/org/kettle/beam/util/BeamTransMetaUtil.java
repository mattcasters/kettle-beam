package org.kettle.beam.util;

import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.filterrows.FilterRowsMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.io.FileOutputStream;
import java.util.List;

public class BeamTransMetaUtil {

  public static final TransMeta generateBeamInputOutputTransMeta( String transname, String inputStepname, String outputStepname, IMetaStore metaStore ) throws Exception {

    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE );
    FileDefinition customerFileDefinition = createCustomersInputFileDefinition();
    factory.saveElement( customerFileDefinition );

    TransMeta transMeta = new TransMeta(  );
    transMeta.setName( transname );
    transMeta.setMetaStore( metaStore );

    int startX = 100;

    // Add the input step
    //
    BeamInputMeta beamInputMeta = new BeamInputMeta();
    beamInputMeta.setInputLocation( "/tmp/customers/input/customers-100.txt" );
    beamInputMeta.setFileDescriptionName( customerFileDefinition.getName() );
    StepMeta beamInputStepMeta = new StepMeta(inputStepname, beamInputMeta);
    beamInputStepMeta.setLocation( startX+=75, 100 );
    beamInputStepMeta.setDraw( true );
    beamInputStepMeta.setStepID( "BeamInput" );
    transMeta.addStep( beamInputStepMeta );


    // Add a dummy in between to get started...
    //
    DummyTransMeta dummyTransMeta = new DummyTransMeta();
    StepMeta dummyStepMeta = new StepMeta("Dummy", dummyTransMeta);
    dummyStepMeta.setLocation( startX+=75, 100 );
    dummyStepMeta.setDraw(true);
    transMeta.addStep( dummyStepMeta );
    transMeta.addTransHop(new TransHopMeta( beamInputStepMeta, dummyStepMeta ) );

    // Add a dummy in between to get started...
    //
    FilterRowsMeta filterRowsMeta = new FilterRowsMeta();
    filterRowsMeta.setCondition(new Condition( "stateCode", Condition.FUNC_EQUAL, null, new ValueMetaAndData( new ValueMetaString(), "CA" ) ) );
    StepMeta filterRowsStepMeta = new StepMeta("Only CA", filterRowsMeta);
    filterRowsStepMeta.setLocation( startX+=75, 100 );
    filterRowsStepMeta.setDraw(true);
    transMeta.addStep( filterRowsStepMeta);
    transMeta.addTransHop(new TransHopMeta( dummyStepMeta, filterRowsStepMeta ) );

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
    beamOutputStepMeta.setLocation( startX+=75, 100 );
    beamOutputStepMeta.setDraw(true);
    transMeta.addStep( beamOutputStepMeta );
    transMeta.addTransHop(new TransHopMeta( filterRowsStepMeta, beamOutputStepMeta ) );

    FileOutputStream fos = new FileOutputStream( "/tmp/beam.ktr" );
    fos.write( transMeta.getXML().getBytes( "UTF-8" ) );
    fos.flush();
    fos.close();

    return transMeta;
  }

  public static final TransMeta generateBeamCloudDataFlowTransMeta(String transname, String inputStepname, String outputStepname, IMetaStore metaStore ) throws Exception {

    TransMeta transMeta = generateBeamInputOutputTransMeta( transname, inputStepname, outputStepname, metaStore );

    // Modify the input/output locations
    //
    BeamInputMeta inputMeta = (BeamInputMeta) transMeta.findStep( inputStepname ).getStepMetaInterface();
    inputMeta.setInputLocation( "gs://kettledataflow/input/customers-noheader-1M.txt" );

    BeamOutputMeta outputMeta = (BeamOutputMeta) transMeta.findStep( outputStepname ).getStepMetaInterface();
    outputMeta.setOutputLocation( "gs://kettledataflow/output/" );

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
