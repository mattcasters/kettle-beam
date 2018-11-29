package org.kettle.beam.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleToStringFn;
import org.kettle.beam.core.transform.BeamInputTransform;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.transform.StepTransform;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.io.IOException;

public class TransMetaPipelineConverter {

  private TransMeta transMeta;
  private IMetaStore metaStore;

  public TransMetaPipelineConverter( TransMeta transMeta, IMetaStore metaStore ) {
    this.transMeta = transMeta;
    this.metaStore = metaStore;
  }

  public Pipeline createPipeline() throws Exception {
    return createPipeline( DirectRunner.class, PipelineOptionsFactory.create() );
  }

  public Pipeline createPipeline( Class<? extends PipelineRunner<?>> runnerClass, PipelineOptions pipelineOptions ) throws Exception {

    LogChannelInterface log = LogChannel.GENERAL;

    // Create a new Pipeline
    //
    pipelineOptions.setRunner( runnerClass );
    pipelineOptions.setJobName( buildDataFlowJobName(transMeta.getName()) );
    pipelineOptions.setUserAgent( BeamDefaults.STRING_KETTLE_BEAM );

    Pipeline pipeline = Pipeline.create( pipelineOptions );


    log.logBasic( "Created pipeline job with name '"+pipelineOptions.getJobName()+"'" );

    // Input handling
    //
    StepMeta beamInputStepMeta = findBeamInput();
    BeamInputMeta beamInputMeta = (BeamInputMeta) beamInputStepMeta.getStepMetaInterface();
    FileDefinition inputFileDefinition = beamInputMeta.loadFileDefinition( metaStore );

    // Apply the PBegin to KettleRow transform:
    //
    if (inputFileDefinition==null) {
      throw new KettleException( "We couldn't find or load the Beam Input step file definition" );
    }
    log.logBasic( "Input file definition found : '"+inputFileDefinition.getName()+"'" );
    BeamInputTransform beamInputTransform = new BeamInputTransform( beamInputStepMeta, inputFileDefinition );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );

    // Transform all the other steps...
    //
    PCollection<KettleRow> afterTransform = afterInput;
    afterTransform = addTransforms( afterTransform );


    // Output handling
    //
    StepMeta beamOutputStepMeta = findBeamOutput();
    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getStepMetaInterface();
    FileDefinition outputFileDefinition = beamOutputMeta.loadFileDefinition( metaStore );
    RowMetaInterface outputStepRowMeta = transMeta.getStepFields(beamOutputStepMeta);

    // Apply the output transform from KettleRow to PDone
    //
    if (outputFileDefinition==null) {
      throw new KettleException( "We couldn't find or load the Beam Output step file definition" );
    }
    if (outputStepRowMeta==null || outputStepRowMeta.isEmpty()) {
      throw new KettleException( "No output fields found in the file definition" );
    }

    BeamOutputTransform beamOutputTransform = new BeamOutputTransform( beamOutputStepMeta, outputFileDefinition, outputStepRowMeta );
    afterTransform.apply( beamOutputTransform );

    return pipeline;
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

        RowMetaInterface rowMeta = transMeta.getPrevStepFields( stepMeta );

        log.logBasic( "Adding step '"+stepMeta.getName()+"' to the pipeline, row metadata: "+rowMeta.toString() );

        // Wrap it in a tag, otherwise the XML is not valid...
        //
        String stepMetaInterfaceXml = XMLHandler.openTag(StepMeta.XML_TAG)+stepMeta.getStepMetaInterface().getXML()+XMLHandler.closeTag(StepMeta.XML_TAG);

        StepTransform stepTransform = new StepTransform( stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, rowMeta.getMetaXML() );

        // We read a bunch of Strings, one per line basically
        //
        currentCollection = currentCollection.apply( stepMeta.getName() + " TRANSFORM", stepTransform );
      }
    }

    return currentCollection;
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
