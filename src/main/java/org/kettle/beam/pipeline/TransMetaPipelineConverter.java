package org.kettle.beam.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.coder.KettleRowCoder;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.transform.StepTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.KettleBeamUtil;
import org.kettle.beam.pipeline.handler.BeamGroupByStepHandler;
import org.kettle.beam.pipeline.handler.BeamInputStepHandler;
import org.kettle.beam.pipeline.handler.BeamMergeJoinStepHandler;
import org.kettle.beam.pipeline.handler.BeamOutputStepHandler;
import org.kettle.beam.pipeline.handler.BeamPublisherStepHandler;
import org.kettle.beam.pipeline.handler.BeamStepHandler;
import org.kettle.beam.pipeline.handler.BeamSubscriberStepHandler;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.JarFileCache;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.steps.groupby.GroupByMeta;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;
import org.pentaho.di.trans.steps.uniquerows.UniqueRowsMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.scannotation.AnnotationDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransMetaPipelineConverter {

  private TransMeta transMeta;
  private SerializableMetaStore metaStore;
  private String metaStoreJson;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;
  private String pluginsToStage;
  private Map<String, BeamStepHandler> stepHandlers;

  public TransMetaPipelineConverter( TransMeta transMeta, IMetaStore metaStore, String pluginsToStage ) throws MetaStoreException {
    this.transMeta = transMeta;
    this.metaStore = new SerializableMetaStore( metaStore );
    this.metaStoreJson = this.metaStore.toJson();
    this.pluginsToStage = pluginsToStage;

    this.stepPluginClasses = new ArrayList<>();
    this.xpPluginClasses = new ArrayList<>();

    this.stepHandlers = new HashMap<>();

    PluginRegistry registry = PluginRegistry.getInstance();

    // Find the plugins in the jar files in the plugin folders to stage...
    //
    if ( StringUtils.isEmpty( pluginsToStage ) ) {
      return;
    }

    String[] pluginFolders = pluginsToStage.split( "," );

    for ( String pluginFolder : pluginFolders ) {
      List<String> stepClasses = findAnnotatedClasses( pluginFolder, Step.class.getName() );
      stepPluginClasses.addAll( stepClasses );
      List<String> xpClasses = findAnnotatedClasses( pluginFolder, ExtensionPoint.class.getName() );
      xpPluginClasses.addAll( xpClasses );
    }


  }

  private List<String> findAnnotatedClasses( String folder, String annotationClassName ) {
    JarFileCache jarFileCache = JarFileCache.getInstance();
    List<String> classnames = new ArrayList<>();

    // Scan only jar files with @Step and @ExtensionPointPlugin annotations
    // No plugin.xml format supported for the moment
    //
    PluginFolder pluginFolder = new PluginFolder( "plugins/" + folder, false, true, false );

    try {
      // Get all the jar files in the plugin folder...
      //
      FileObject[] fileObjects = jarFileCache.getFileObjects( pluginFolder );
      if ( fileObjects != null ) {
        // System.out.println( "Found " + fileObjects.length + " jar files in folder " + pluginFolder.getFolder() );

        for ( FileObject fileObject : fileObjects ) {

          // These are the jar files : find annotations in it...
          //
          AnnotationDB annotationDB = jarFileCache.getAnnotationDB( fileObject );

          // These are the jar files : find annotations in it...
          //
          Set<String> impls = annotationDB.getAnnotationIndex().get( annotationClassName );
          if ( impls != null ) {

            for ( String fil : impls ) {
              classnames.add( fil );
            }
          }
        }
      } else {
        System.out.println( "No jar files found in plugin folder " + pluginFolder.getFolder() );
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }

    return classnames;
  }

  public Pipeline createPipeline( Class<? extends PipelineRunner<?>> runnerClass, PipelineOptions pipelineOptions ) throws Exception {

    LogChannelInterface log = LogChannel.GENERAL;

    // Add the step handlers for the special cases, functionality which Beams handles specifically
    //
    stepHandlers.put( BeamDefaults.STRING_BEAM_INPUT_PLUGIN_ID, new BeamInputStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamDefaults.STRING_BEAM_OUTPUT_PLUGIN_ID, new BeamOutputStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamDefaults.STRING_BEAM_PUBLISH_PLUGIN_ID, new BeamPublisherStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamDefaults.STRING_BEAM_SUBSCRIBE_PLUGIN_ID, new BeamSubscriberStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamDefaults.STRING_MERGE_JOIN_PLUGIN_ID, new BeamMergeJoinStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamDefaults.STRING_MEMORY_GROUP_BY_PLUGIN_ID, new BeamGroupByStepHandler( metaStore, transMeta, stepPluginClasses, xpPluginClasses ) );

    // Create a new Pipeline
    //
    pipelineOptions.setRunner( runnerClass );
    Pipeline pipeline = Pipeline.create( pipelineOptions );

    pipeline.getCoderRegistry().registerCoderForClass( KettleRow.class, new KettleRowCoder() );

    log.logBasic( "Created pipeline job with name '" + pipelineOptions.getJobName() + "'" );

    // Keep track of which step outputs which Collection
    //
    Map<String, PCollection<KettleRow>> stepCollectionMap = new HashMap<>();

    // Handle io
    //
    handleBeamInputSteps( log, stepCollectionMap, pipeline );

    // Transform all the other steps...
    //
    handleGenericStep( stepCollectionMap, pipeline );

    // Output handling
    //
    handleBeamOutputSteps( log, stepCollectionMap, pipeline );

    return pipeline;
  }

  private void handleBeamInputSteps( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap, Pipeline pipeline ) throws KettleException, IOException {

    List<StepMeta> beamInputStepMetas = findBeamInputs();
    for ( StepMeta stepMeta : beamInputStepMetas ) {
      BeamStepHandler stepHandler = stepHandlers.get( stepMeta.getStepID() );
      stepHandler.handleStep( log, stepMeta, stepCollectionMap, pipeline, transMeta.getStepFields( stepMeta ), null, null );
    }
  }

  private void handleBeamOutputSteps( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap, Pipeline pipeline ) throws KettleException, IOException {
    List<StepMeta> beamOutputStepMetas = findBeamOutputs();
    for ( StepMeta stepMeta : beamOutputStepMetas ) {
      BeamStepHandler stepHandler = stepHandlers.get( stepMeta.getStepID() );

      List<StepMeta> previousSteps = transMeta.findPreviousSteps( stepMeta, false );
      if ( previousSteps.size() > 1 ) {
        throw new KettleException( "Combining data from multiple steps is not supported yet!" );
      }
      StepMeta previousStep = previousSteps.get( 0 );

      PCollection<KettleRow> input = stepCollectionMap.get( previousStep.getName() );
      if ( input == null ) {
        throw new KettleException( "Previous PCollection for step " + previousStep.getName() + " could not be found" );
      }

      // What fields are we getting from the previous step(s)?
      //
      RowMetaInterface rowMeta = transMeta.getStepFields( previousStep );

      stepHandler.handleStep( log, stepMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input );
    }
  }

  private void handleGenericStep( Map<String, PCollection<KettleRow>> stepCollectionMap, Pipeline pipeline ) throws KettleException, IOException {

    LogChannelInterface log = LogChannel.GENERAL;

    // Perform topological sort
    //
    List<StepMeta> steps = getSortedStepsList();

    for ( StepMeta stepMeta : steps ) {

      // Input and output steps are handled else where.
      //
      BeamStepHandler stepHandler = stepHandlers.get( stepMeta.getStepID() );
      if ( stepHandler == null || ( !stepHandler.isInput() && !stepHandler.isOutput() ) ) {

        // Generic step
        //
        validateStepBeamUsage( stepMeta.getStepMetaInterface() );

        // Lookup all the previous steps for this one, excluding info steps like StreamLookup...
        // So the usecase is : we read from multiple io steps and join to one location...
        //
        List<StepMeta> previousSteps = transMeta.findPreviousSteps( stepMeta, false );

        StepMeta firstPreviousStep;
        RowMetaInterface rowMeta;
        PCollection<KettleRow> input = null;

        // Steps like Merge Join or Merge have no io, only info steps reaching in
        //
        if ( previousSteps.isEmpty() ) {
          firstPreviousStep = null;
          rowMeta = new RowMeta();
        } else {

          // Lookup the previous collection to apply this steps transform to.
          // We can take any of the inputs so the first one will do.
          //
          firstPreviousStep = previousSteps.get( 0 );

          // No fuss with info fields sneaking in, all previous steps need to emit the same layout anyway
          //
          rowMeta = transMeta.getStepFields( firstPreviousStep );
          // System.out.println("STEP FIELDS for '"+firstPreviousStep.getName()+"' : "+rowMeta);

          // Check in the map to see if previousStep isn't targeting this one
          //
          String targetName = KettleBeamUtil.createTargetTupleId( firstPreviousStep.getName(), stepMeta.getName() );
          input = stepCollectionMap.get( targetName );
          if ( input == null ) {
            input = stepCollectionMap.get( firstPreviousStep.getName() );
          } else {
            log.logBasic( "Step " + stepMeta.getName() + " reading from previous step targetting this one using : " + targetName );
          }

          // If there are multiple io streams into this step, flatten all the data sources by default
          // This means to simply merge the data.
          //
          if ( previousSteps.size() > 1 ) {
            List<PCollection<KettleRow>> extraInputs = new ArrayList<>();
            for ( int i = 1; i < previousSteps.size(); i++ ) {
              StepMeta previousStep = previousSteps.get( i );
              PCollection<KettleRow> previousPCollection;
              targetName = KettleBeamUtil.createTargetTupleId( previousStep.getName(), stepMeta.getName() );
              previousPCollection = stepCollectionMap.get( targetName );
              if ( previousPCollection == null ) {
                previousPCollection = stepCollectionMap.get( previousStep.getName() );
              } else {
                log.logBasic( "Step " + stepMeta.getName() + " reading from previous step targetting this one using : " + targetName );
              }
              if ( previousPCollection == null ) {
                throw new KettleException( "Previous collection was not found for step " + previousStep.getName() + ", a previous step to " + stepMeta.getName() );
              }
              extraInputs.add( previousPCollection );
            }

            // Flatten the extra inputs...
            //
            PCollectionList<KettleRow> inputList = PCollectionList.of( input );

            for ( PCollection<KettleRow> extraInput : extraInputs ) {
              inputList = inputList.and( extraInput );
            }

            // Flatten all the collections.  It's business as usual behind this.
            //
            input = inputList.apply( stepMeta.getName() + " Flatten", Flatten.<KettleRow>pCollections() );
          }
        }

        if ( stepHandler != null ) {

          stepHandler.handleStep( log, stepMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input );

        } else {

          String stepMetaInterfaceXml = XMLHandler.openTag( StepMeta.XML_TAG ) + stepMeta.getStepMetaInterface().getXML() + XMLHandler.closeTag( StepMeta.XML_TAG );

          handleGenericStep( log, stepCollectionMap, stepMeta, rowMeta, previousSteps, input, stepMetaInterfaceXml );

        }
      }
    }

  }

  private void handleGenericStep( LogChannelInterface log, Map<String, PCollection<KettleRow>> stepCollectionMap, StepMeta stepMeta, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                  PCollection<KettleRow> input, String stepMetaInterfaceXml ) throws KettleException, IOException {
    // See if the step has Info steps
    //
    List<StepMeta> infoStepMetas = transMeta.findPreviousSteps( stepMeta, true );
    List<String> infoSteps = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<PCollectionView<List<KettleRow>>> infoCollectionViews = new ArrayList<>();
    for ( StepMeta infoStepMeta : infoStepMetas ) {
      if ( !previousSteps.contains( infoStepMeta ) ) {
        infoSteps.add( infoStepMeta.getName() );
        infoRowMetaJsons.add( JsonRowMeta.toJson( transMeta.getStepFields( infoStepMeta ) ) );
        PCollection<KettleRow> infoCollection = stepCollectionMap.get( infoStepMeta.getName() );
        if ( infoCollection == null ) {
          throw new KettleException( "Unable to find collection for step '" + infoStepMeta.getName() + " providing info for '" + stepMeta.getName() + "'" );
        }
        infoCollectionViews.add( infoCollection.apply( View.asList() ) );
      }
    }

    // Get the list of variables from the TransMeta variable space:
    //
    List<VariableValue> variableValues = getVariableValues( transMeta );

    // Find out all the target steps for this step...
    //
    StepIOMetaInterface ioMeta = stepMeta.getStepMetaInterface().getStepIOMeta();
    List<String> targetSteps = new ArrayList<String>();
    for ( StreamInterface targetStream : ioMeta.getTargetStreams() ) {
      if ( targetStream.getStepMeta() != null ) {
        targetSteps.add( targetStream.getStepMeta().getName() );
      }
    }

    // Send all the information on their way to the right nodes
    //
    StepTransform stepTransform = new StepTransform( variableValues, metaStoreJson, stepPluginClasses, xpPluginClasses,
      stepMeta.getName(), stepMeta.getStepID(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );


    // Apply the step transform to the previous io step PCollection(s)
    //
    PCollectionTuple tuple = input.apply( stepMeta.getName(), stepTransform );

    // The main collection
    //
    PCollection<KettleRow> mainPCollection = tuple.get( new TupleTag<KettleRow>( KettleBeamUtil.createMainOutputTupleId( stepMeta.getName() ) ) );

    // Save this in the map
    //
    stepCollectionMap.put( stepMeta.getName(), mainPCollection );

    // Were there any targeted steps in this step?
    //
    for ( String targetStep : targetSteps ) {
      String tupleId = KettleBeamUtil.createTargetTupleId( stepMeta.getName(), targetStep );
      PCollection<KettleRow> targetPCollection = tuple.get( new TupleTag<KettleRow>( tupleId ) );

      // Store this in the map as well
      //
      stepCollectionMap.put( tupleId, targetPCollection );
    }

    log.logBasic( "Handled step (STEP) : " + stepMeta.getName() + ", gets data from " + previousSteps.size() + " previous step(s), targets=" + targetSteps.size() + ", infos=" + infoSteps.size() );
  }

  private void validateStepBeamUsage( StepMetaInterface meta ) throws KettleException {
    if ( meta instanceof GroupByMeta ) {
      throw new KettleException( "Group By is not supported.  Use the Memory Group By step instead.  It comes closest to Beam functionality." );
    }
    if ( meta instanceof SortRowsMeta ) {
      throw new KettleException( "Sort rows is not yet supported on Beam." );
    }
    if ( meta instanceof UniqueRowsMeta ) {
      throw new KettleException( "The unique rows step is not yet supported on Beam, for now use a Memory Group By to get distrinct rows" );
    }
  }

  private List<VariableValue> getVariableValues( VariableSpace space ) {

    List<VariableValue> variableValues = new ArrayList<>();
    for ( String variable : space.listVariables() ) {
      String value = space.getVariable( variable );
      variableValues.add( new VariableValue( variable, value ) );
    }
    return variableValues;
  }

  /**
   * Find the Beam Input steps, return them
   *
   * @throws KettleException
   */
  private List<StepMeta> findBeamInputs() throws KettleException {
    List<StepMeta> steps = new ArrayList<>();
    for ( StepMeta stepMeta : transMeta.getTransHopSteps( false ) ) {

      BeamStepHandler stepHandler = stepHandlers.get( stepMeta.getStepID() );
      if ( stepHandler != null && stepHandler.isInput() ) {
        steps.add( stepMeta );
      }
    }
    return steps;
  }

  private List<StepMeta> findBeamOutputs() throws KettleException {
    List<StepMeta> steps = new ArrayList<>();
    for ( StepMeta stepMeta : transMeta.getTransHopSteps( false ) ) {
      BeamStepHandler stepHandler = stepHandlers.get( stepMeta.getStepID() );
      if ( stepHandler != null && stepHandler.isOutput() ) {
        steps.add( stepMeta );
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
    List<StepMeta> steps = new ArrayList<>( transMeta.getTransHopSteps( false ) );

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

  /**
   * Gets stepHandlers
   *
   * @return value of stepHandlers
   */
  public Map<String, BeamStepHandler> getStepHandlers() {
    return stepHandlers;
  }

  /**
   * @param stepHandlers The stepHandlers to set
   */
  public void setStepHandlers( Map<String, BeamStepHandler> stepHandlers ) {
    this.stepHandlers = stepHandlers;
  }
}