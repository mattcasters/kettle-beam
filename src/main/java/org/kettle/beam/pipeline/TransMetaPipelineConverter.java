package org.kettle.beam.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
import org.pentaho.di.trans.step.StepMetaDataCombi;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // Keep track of which step outputs which Collection
    //
    Map<String, PCollection<KettleRow>> stepCollectionMap = new HashMap<>();

    // Handle input
    //
    handleBeamInputSteps(log, stepCollectionMap, pipeline);

    // Transform all the other steps...
    //
    addTransforms( stepCollectionMap );

    // Output handling
    //
    handleBeamOutputSteps(log, stepCollectionMap );

    return pipeline;
  }

  private void handleBeamInputSteps( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap, Pipeline pipeline ) throws KettleException, IOException {

    List<StepMeta> beamInputStepMetas = findBeamInputs();
    for (StepMeta stepMeta : beamInputStepMetas) {
      handlePipelineInput( log, stepCollectionMap, pipeline, stepMeta);
    }
  }

  private void handlePipelineInput( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap, Pipeline pipeline, StepMeta beamInputStepMeta ) throws KettleException, IOException {
    // Input handling
    //
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
    stepCollectionMap.put(beamInputStepMeta.getName(), afterInput);
    log.logBasic( "Handled step (INPUT) : "+beamInputStepMeta.getName() );

  }

  private void handleBeamOutputSteps( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap ) throws KettleException, IOException {

    List<StepMeta> beamOutputStepMetas = findBeamOutputs();
    for (StepMeta beamOutputStepMeta : beamOutputStepMetas) {
      BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getStepMetaInterface();
      FileDefinition outputFileDefinition;
      if ( StringUtils.isEmpty( beamOutputMeta.getFileDescriptionName() ) ) {
        // Create a default file definition using standard output and sane defaults...
        //
        outputFileDefinition = getDefaultFileDefition( beamOutputStepMeta );
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
        beamOutputStepMeta.getName(),
        transMeta.environmentSubstitute( beamOutputMeta.getOutputLocation() ),
        transMeta.environmentSubstitute( beamOutputMeta.getFilePrefix() ),
        transMeta.environmentSubstitute( beamOutputMeta.getFileSuffix() ),
        transMeta.environmentSubstitute( outputFileDefinition.getSeparator() ),
        transMeta.environmentSubstitute( outputFileDefinition.getEnclosure() ),
        outputStepRowMeta.getMetaXML()
      );

      // Which step do we apply this transform to?
      // Ignore info hops until we figure that out.
      //
      List<StepMeta> previousSteps = transMeta.findPreviousSteps( beamOutputStepMeta, false );
      if (previousSteps.size()>1) {
        throw new KettleException( "Combining data from multiple steps is not supported yet!" );
      }
      StepMeta previousStep = previousSteps.get( 0 );

      // Where does the output step read from?
      //
      PCollection<KettleRow> previousPCollection = stepCollectionMap.get( previousStep.getName() );
      if (previousPCollection==null) {
        throw new KettleException( "Previous PCollection for step "+previousStep.getName()+" could not be found" );
      }

      // No need to store this, it's PDone.
      //
      previousPCollection.apply( beamOutputTransform );
      log.logBasic( "Handled step (OUTPUT) : "+beamOutputStepMeta.getName()+", gets data from "+previousStep.getName() );

    }
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

  private void addTransforms( Map<String, PCollection<KettleRow>> stepCollectionMap ) throws KettleException, IOException {

    LogChannelInterface log = LogChannel.GENERAL;

    // Perform topological sort
    //
    List<StepMeta> steps = getSortedStepsList();

    for ( StepMeta stepMeta : steps ) {
      if ( !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID ) &&
        !stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID ) ) {

        validateStepBeamUsage(stepMeta.getStepMetaInterface());

        RowMetaInterface rowMeta = transMeta.getPrevStepFields( stepMeta );

        // Lookup all the previous steps for this one, excluding info steps like StreamLookup...
        // So the usecase is : we read from multiple input steps and join to one location...
        //
        List<StepMeta> previousSteps = transMeta.findPreviousSteps( stepMeta, false);
        if (previousSteps.size()>1) {
          throw new KettleException( "Combining data from multiple steps is not supported yet!" );
        }

        // Wrap current step metadata in a tag, otherwise the XML is not valid...
        //
        String stepMetaInterfaceXml = XMLHandler.openTag( StepMeta.XML_TAG ) + stepMeta.getStepMetaInterface().getXML() + XMLHandler.closeTag( StepMeta.XML_TAG );

        PTransform<PCollection<KettleRow>, PCollection<KettleRow>> stepTransform;

        if ( stepMeta.getStepMetaInterface() instanceof MemoryGroupByMeta ) {

          MemoryGroupByMeta meta = (MemoryGroupByMeta) stepMeta.getStepMetaInterface();

          String[] aggregates = new String[ meta.getAggregateType().length ];
          for ( int i = 0; i < aggregates.length; i++ ) {
            aggregates[ i ] = MemoryGroupByMeta.getTypeDesc( meta.getAggregateType()[ i ] );
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
          List<VariableValue> variableValues = getVariableValues( transMeta );

          // Send all the information on their way to the right nodes
          //
          stepTransform = new StepTransform( variableValues, stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, rowMeta.getMetaXML() );
        }

        // The previous step to connect to ...
        //
        StepMeta previousStep = previousSteps.get(0);

        // Lookup the previous collection to apply this step to.
        //
        PCollection<KettleRow> previousPCollection = stepCollectionMap.get( previousStep.getName() );
        if (previousPCollection==null) {
          throw new KettleException( "Previous PCollection for step "+previousStep.getName()+" could not be found" );
        }

        // We read a bunch of Strings, one per line basically
        //
        PCollection<KettleRow> stepPCollection = previousPCollection.apply( stepMeta.getName(), stepTransform );

        // Save this in the map
        //
        stepCollectionMap.put(stepMeta.getName(), stepPCollection);
        log.logBasic( "Handled step (STEP) : "+stepMeta.getName()+", gets data from "+previousStep.getName() );

      }
    }

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
   * Find the Beam Input steps, return them
   *
   * @throws KettleException
   */
  private List<StepMeta> findBeamInputs() throws KettleException {
    List<StepMeta> steps = new ArrayList<>(  );
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID ) ) {
        steps.add(stepMeta);
      }
    }
    return steps;
  }

  private List<StepMeta> findBeamOutputs() throws KettleException {
    List<StepMeta> steps = new ArrayList<>(  );
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      if ( stepMeta.getStepID().equals( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID ) ) {
        steps.add(stepMeta);
      }
    }
    return steps;
  }


  /**
   * Sort the steps from start to finish...
   */
  private List<StepMeta> getSortedStepsList() {

    // Create a copy of the steps
    //
    List<StepMeta> steps = new ArrayList<>( transMeta.getSteps() );

    // The bubble sort algorithm in contrast to the QuickSort or MergeSort
    // algorithms
    // does indeed cover all possibilities.
    // Sorting larger transformations with hundreds of steps might be too slow
    // though.
    // We should consider caching TransMeta.findPrevious() results in that case.
    //
    transMeta.clearCaches();

    //
    // Cocktail sort (bi-directional bubble sort)
    //
    // Original sort was taking 3ms for 30 steps
    // cocktail sort takes about 8ms for the same 30, but it works :)

    // set these to true if you are working on this algorithm and don't like
    // flying blind.
    //

    int stepsMinSize = 0;
    int stepsSize = steps.size();

    // Noticed a problem with an immediate shrinking iteration window
    // trapping rows that need to be sorted.
    // This threshold buys us some time to get the sorting close before
    // starting to decrease the window size.
    //
    // TODO: this could become much smarter by tracking row movement
    // and reacting to that each outer iteration verses
    // using a threshold.
    //
    // After this many iterations enable trimming inner iteration
    // window on no change being detected.
    //
    int windowShrinkThreshold = (int) Math.round( stepsSize * 0.75 );

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = stepsSize * 2;

    boolean isBefore = false;
    boolean forwardChange = false;
    boolean backwardChange = false;

    boolean lastForwardChange = true;
    boolean keepSortingForward = true;

    StepMeta one = null;
    StepMeta two = null;

    long startTime = System.currentTimeMillis();

    for ( int x = 0; x < totalIterations; x++ ) {

      // Go forward through the list
      //
      if ( keepSortingForward ) {
        for ( int y = stepsMinSize; y < stepsSize - 1; y++ ) {
          one = steps.get( y );
          two = steps.get( y + 1 );
          isBefore = transMeta.findPrevious( one, two );
          if ( isBefore ) {
            // two was found to be positioned BEFORE one so we need to
            // switch them...
            //
            steps.set( y, two );
            steps.set( y + 1, one );
            forwardChange = true;

          }
        }
      }

      // Go backward through the list
      //
      for ( int z = stepsSize - 1; z > stepsMinSize; z-- ) {
        one = steps.get( z );
        two = steps.get( z - 1 );

        isBefore = transMeta.findPrevious( one, two );
        if ( !isBefore ) {
          // two was found NOT to be positioned BEFORE one so we need to
          // switch them...
          //
          steps.set( z, two );
          steps.set( z - 1, one );
          backwardChange = true;
        }
      }

      // Shrink stepsSize(max) if there was no forward change
      //
      if ( x > windowShrinkThreshold && !forwardChange ) {

        // should we keep going? check the window size
        //
        stepsSize--;
        if ( stepsSize <= stepsMinSize ) {
          break;
        }
      }

      // shrink stepsMinSize(min) if there was no backward change
      //
      if ( x > windowShrinkThreshold && !backwardChange ) {

        // should we keep going? check the window size
        //
        stepsMinSize++;
        if ( stepsMinSize >= stepsSize ) {
          break;
        }
      }

      // End of both forward and backward traversal.
      // Time to see if we should keep going.
      //
      if ( !forwardChange && !backwardChange ) {
        break;
      }

      //
      // if we are past the first iteration and there has been no change twice,
      // quit doing it!
      //
      if ( keepSortingForward && x > 0 && !lastForwardChange && !forwardChange ) {
        keepSortingForward = false;
      }
      lastForwardChange = forwardChange;
      forwardChange = false;
      backwardChange = false;

    } // finished sorting

    return steps;
  }
}