package org.kettle.beam.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.fn.StringToKettleFn;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

public class TransMetaConverter {

  private TransMeta transMeta;
  private IMetaStore metaStore;

  public TransMetaConverter( TransMeta transMeta, IMetaStore metaStore) {
    this.transMeta = transMeta;
    this.metaStore = metaStore;
  }

  public Pipeline createPipeline() throws Exception {

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setJobName( transMeta.getName() );
    pipelineOptions.setUserAgent( BeamDefaults.STRING_KETTLE_BEAM );

    Pipeline p = Pipeline.create(pipelineOptions);

    addInput(p);
    addOutput(p);

    return p;
  }

  /**
   * Find a Beam Input step, handle it
   *
   * @param pipeline
   * @throws KettleException
   */
  private void addInput( Pipeline pipeline ) throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps()) {
      if (stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID )) {

        BeamInputMeta inputMeta = (BeamInputMeta) stepMeta.getStepMetaInterface();

        String fileLocation = inputMeta.getInputLocation();
        FileDefinition fileDefinition = inputMeta.loadFileDefinition(metaStore);

        pipeline

        // We read a bunch of Strings, one per line basically
        //
        .apply( stepMeta.getName()+" READ FILE", TextIO.read().from(fileLocation) )

        // We need to transform these lines into Kettle fields
        //
        .apply( ParDo.of(new StringToKettleFn(metaStore, fileDefinition)));

        // From here on out the pipeline contains KettleRow but we need to know what the metadata is
        // Since it's just an Object[]
        //

      }
    }
    throw new KettleException( "No Beam Input step was found in the Kettle Beam Transformation" );
  }

  private void addOutput( Pipeline pipeline ) throws KettleException {
    for ( StepMeta stepMeta : transMeta.getSteps()) {
      if (stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID )) {

        BeamOutputMeta outputMeta = (BeamOutputMeta) stepMeta.getStepMetaInterface();

        String fileLocation = outputMeta.getOutputLocation();
        FileDefinition fileDefinition = outputMeta.loadFileDefinition(metaStore);

        // Convert Kettle fields back to String
        // TODO : implement

        // pipeline.apply( stepMeta.getName(), new KettleToStringFn( metaStore, fileDefinition ) );

        // TODO: Write to file, specify folder, base file, ...
        //
        return;
      }
    }
    throw new KettleException( "No Beam Output step was found in the Kettle Beam Transformation" );
  }
}
