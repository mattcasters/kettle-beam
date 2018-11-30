package org.kettle.beam.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.coder.KettleRowCoder;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.transform.BeamInputTransform;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.transform.GroupByTransform;
import org.kettle.beam.core.transform.StepTransform;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.groupby.GroupByMeta;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByMeta;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;
import org.pentaho.di.trans.steps.streamlookup.StreamLookupMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseMeta;
import org.pentaho.di.trans.steps.uniquerows.UniqueRowsMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransMetaPipelineConverter {

  private TransMeta transMeta;
  private IMetaStore metaStore;

  public TransMetaPipelineConverter( TransMeta transMeta, IMetaStore metaStore ) {
    this.transMeta = transMeta;
    this.metaStore = metaStore;
  }

  public Pipeline createPipeline( Class<? extends PipelineRunner<?>> runnerClass, PipelineOptions pipelineOptions ) throws Exception {

    LogChannelInterface log = LogChannel.GENERAL;

    // Create a new Pipeline
    //
    pipelineOptions.setRunner( runnerClass );
    Pipeline pipeline = Pipeline.create( pipelineOptions );

    pipeline.getCoderRegistry().registerCoderForClass( KettleRow.class, new KettleRowCoder());

    log.logBasic( "Created pipeline job with name '" + pipelineOptions.getJobName() + "'" );

    // Handle input
    //
    PCollection<KettleRow> afterInput = handlePipelineInput(log, pipeline);

    // Transform all the other steps...
    //
    PCollection<KettleRow> afterTransform = afterInput;
    afterTransform = addTransforms( afterTransform );

    // Output handling
    //
    handleOutputAfterTransforms(log, afterTransform);

    return pipeline;
  }

  private PCollection<KettleRow> handlePipelineInput( LogChannelInterface log, Pipeline pipeline ) throws KettleException, IOException {
    // Input handling
    //
    StepMeta beamInputStepMeta = findBeamInput();
    BeamInputMeta beamInputMeta = (BeamInputMeta) beamInputStepMeta.getStepMetaInterface();
    FileDefinition inputFileDefinition = beamInputMeta.loadFileDefinition( metaStore );
    RowMetaInterface fileRowMeta = inputFileDefinition.getRowMeta();

    // Apply the PBegin to KettleRow transform:
    //
    if ( inputFileDefinition == null ) {
      throw new KettleException( "We couldn't find or load the Beam Input step file definition" );
    }
    String fileInputLocation = transMeta.environmentSubstitute( beamInputMeta.getInputLocation() );

    BeamInputTransform beamInputTransform = new BeamInputTransform(
      beamInputStepMeta.getName(),
      beamInputStepMeta.getName(),
      fileInputLocation,
      transMeta.environmentSubstitute( inputFileDefinition.getSeparator() ),
      fileRowMeta.getMetaXML()
    );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );
    return afterInput;
  }

  private void handleOutputAfterTransforms( LogChannelInterface log, PCollection<KettleRow> afterTransform ) throws KettleException, IOException {
    StepMeta beamOutputStepMeta = findBeamOutput();
    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getStepMetaInterface();
    FileDefinition outputFileDefinition;
    if ( StringUtils.isEmpty(beamOutputMeta.getFileDescriptionName())) {
      // Create a default file definition using standard output and sane defaults...
      //
      outputFileDefinition = getDefaultFileDefition(beamOutputStepMeta);
    } else {
      outputFileDefinition = beamOutputMeta.loadFileDefinition( metaStore );
    }
    RowMetaInterface outputStepRowMeta = transMeta.getStepFields( beamOutputStepMeta );

    // Apply the output transform from KettleRow to PDone
    //
    if ( outputFileDefinition == null ) {
      throw new KettleException( "We couldn't find or load the Beam Output step file definition" );
    }
    if ( outputStepRowMeta == null || outputStepRowMeta.isEmpty() ) {
      throw new KettleException( "No output fields found in the file definition" );
    }

    BeamOutputTransform beamOutputTransform = new BeamOutputTransform(
      findBeamInput().getName(),
      transMeta.environmentSubstitute( beamOutputMeta.getOutputLocation() ),
      transMeta.environmentSubstitute( beamOutputMeta.getFilePrefix() ),
      transMeta.environmentSubstitute( beamOutputMeta.getFileSuffix() ),
      transMeta.environmentSubstitute( outputFileDefinition.getSeparator() ),
      transMeta.environmentSubstitute( outputFileDefinition.getEnclosure() ),
      outputStepRowMeta.getMetaXML()
    );
    afterTransform.apply( beamOutputTransform );
  }


  private FileDefinition getDefaultFileDefition( StepMeta beamOutputStepMeta ) throws KettleStepException {
    FileDefinition fileDefinition = new FileDefinition(  );

    fileDefinition.setName( "Default" );
    fileDefinition.setEnclosure( "\"" );
    fileDefinition.setSeparator( "," );

    RowMetaInterface rowMeta = transMeta.getPrevStepFields( beamOutputStepMeta );
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList()) {
      fileDefinition.getFieldDefinitions().add(new FieldDefinition(
        valueMeta.getName(),
        valueMeta.getTypeDesc(),
        valueMeta.getLength(),
        valueMeta.getPrecision(),
        valueMeta.getConversionMask()
        )
      );
    }

    return fileDefinition;
  }

  private String buildDataFlowJobName( String name ) {
    return name.replace( " ", "_" );
  }

  private PCollection<KettleRow> addTransforms( PCollection<KettleRow> collection ) throws KettleException, IOException {

    LogChannelInterface log = LogChannel.GENERAL;

    // Perform topological sort
    //
    transMeta.sortStepsNatural();

    PCollection<KettleRow> currentCollection = collection;

    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID ) &&
        !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID ) ) {

        validateStepBeamUsage(stepMeta.getStepMetaInterface());

        RowMetaInterface rowMeta = transMeta.getPrevStepFields( stepMeta );

        log.logBasic( "Adding step '" + stepMeta.getName() + "' to the pipeline, row metadata: " + rowMeta.toString() );

        // Wrap it in a tag, otherwise the XML is not valid...
        //
        String stepMetaInterfaceXml = XMLHandler.openTag( StepMeta.XML_TAG ) + stepMeta.getStepMetaInterface().getXML() + XMLHandler.closeTag( StepMeta.XML_TAG );

        PTransform<PCollection<KettleRow>, PCollection<KettleRow>> stepTransform;

        if ( stepMeta.getStepMetaInterface() instanceof MemoryGroupByMeta) {

          MemoryGroupByMeta meta = (MemoryGroupByMeta) stepMeta.getStepMetaInterface();

          String[] aggregates = new String[meta.getAggregateType().length];
          for (int i=0;i<aggregates.length;i++) {
            aggregates[i] = MemoryGroupByMeta.getTypeDesc( meta.getAggregateType()[i] );
          }

          stepTransform = new GroupByTransform(
            rowMeta.getMetaXML(),  // The input row
            meta.getGroupField(),
            meta.getSubjectField(),
            aggregates,
            meta.getAggregateField()
            );

        } else {

          // Get the list of variables from the TransMeta variable space:
          //
          List<VariableValue> variableValues = getVariableValues(transMeta);

          // Send all the information on their way to the right nodes
          //
          stepTransform = new StepTransform( variableValues, stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, rowMeta.getMetaXML() );
        }

        // We read a bunch of Strings, one per line basically
        //
        currentCollection = currentCollection.apply( stepMeta.getName(), stepTransform );
      }
    }

    return currentCollection;
  }

  private void validateStepBeamUsage( StepMetaInterface meta ) throws KettleException {
    if (meta instanceof GroupByMeta) {
      throw new KettleException( "Group By is not supported.  Use the Memory Group By step instead.  It comes closest to Beam functionality." );
    }
    if (meta instanceof SortRowsMeta ) {
      throw new KettleException( "Sort rows is not yet supported on Beam." );
    }
    if (meta instanceof UniqueRowsMeta ) {
      throw new KettleException( "Unique rows is not yet supported on Beam" );
    }
    if (meta instanceof StreamLookupMeta ) {
      throw new KettleException( "Stream Lookup is not yet supported on Beam" );
    }
    if (meta instanceof SwitchCaseMeta ) {
      throw new KettleException( "Switch/Case is not yet supported on Beam" );
    }
  }

  private List<VariableValue> getVariableValues( VariableSpace space) {

    List<VariableValue> variableValues = new ArrayList<>(  );
    for (String variable : space.listVariables()) {
      String value = space.getVariable( variable );
      variableValues.add(new VariableValue( variable, value ));
    }
    return variableValues;
  }

  /**
   * Find a Beam Input step, handle it
   *
   * @throws KettleException
   */
  private StepMeta findBeamInput() throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID ) ) {
        return stepMeta;
      }
    }
    throw new KettleException( "No Beam Input step was found in the Kettle Beam Transformation" );
  }

  private StepMeta findBeamOutput() throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID ) ) {
        return stepMeta;
      }
    }
    throw new KettleException( "No Beam Output step was found in the Kettle Beam Transformation" );
  }
}