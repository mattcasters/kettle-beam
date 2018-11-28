package org.kettle.beam.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

public class TransMetaConverter {

  private TransMeta transMeta;
  private IMetaStore metaStore;

  public TransMetaConverter( TransMeta transMeta, IMetaStore metaStore ) {
    this.transMeta = transMeta;
    this.metaStore = metaStore;
  }

  public Pipeline createPipeline() throws Exception {

    // Create a new Pipeline
    //
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setJobName( transMeta.getName() );
    pipelineOptions.setUserAgent( BeamDefaults.STRING_KETTLE_BEAM );

    Pipeline pipeline = Pipeline.create( pipelineOptions );



    // Input handling
    //
    StepMeta beamInputStepMeta = findBeamInput();
    BeamInputMeta beamInputMeta = (BeamInputMeta) beamInputStepMeta.getStepMetaInterface();
    FileDefinition inputFileDefinition = beamInputMeta.loadFileDefinition( metaStore );

    // Output handling
    //
    StepMeta beamOutputStepMeta = findBeamOutput();
    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getStepMetaInterface();
    FileDefinition outputFileDefinition = beamOutputMeta.loadFileDefinition( metaStore );
    RowMetaInterface outputStepRowMeta = transMeta.getStepFields(beamOutputStepMeta);



    // Apply the PBegin to KettleRow transform:
    //
    BeamInputTransform beamInputTransform = new BeamInputTransform( beamInputStepMeta, inputFileDefinition );
    PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );

    // Transform all the other steps...
    //
    PCollection<KettleRow> afterTransform = afterInput;
    addTransforms( afterTransform );

    // Apply the output transform from KettleRow to PDone
    //
    BeamOutputTransform beamOutputTransform = new BeamOutputTransform( beamOutputStepMeta, outputFileDefinition, outputStepRowMeta );
    afterTransform.apply( beamOutputTransform );

    return pipeline;
  }

  private PCollection<KettleRow> addTransforms( PCollection<KettleRow> collection ) throws KettleException {

    // Perform topological sort
    //
    transMeta.sortStepsNatural();

    PCollection<KettleRow> currentCollection = collection;

    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID ) &&
        !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID ) ) {

        RowMetaInterface rowMeta = transMeta.getPrevStepFields( stepMeta );

        StepTransform stepTransform = new StepTransform( transMeta, stepMeta.getName() );

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
