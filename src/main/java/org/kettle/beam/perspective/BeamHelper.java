/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.kettle.beam.perspective;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.metastore.FileDefinitionDialog;
import org.kettle.beam.pipeline.TransMetaPipelineConverter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BeamHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = BeamHelper.class; // for i18n

  private static BeamHelper instance = null;

  private Spoon spoon;

  private BeamHelper() {
    spoon = Spoon.getInstance();
  }

  public static BeamHelper getInstance() {
    if ( instance == null ) {
      instance = new BeamHelper(); ;
      instance.spoon.addSpoonMenuController( instance );
    }
    return instance;
  }

  @Override public void updateMenu( Document doc ) {

  }

  public String getName() {
    return "beamHelper";
  }


  public void runLocal() {
    TransMeta transMeta = spoon.getActiveTransformation();
    if ( transMeta == null ) {
      showMessage( "Sorry", "The Apache Beam implementation works with transformations only." );
      return;
    }
    try {

      PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

      TransMetaPipelineConverter converter = new TransMetaPipelineConverter( transMeta, spoon.getMetaStore() );

      Pipeline pipeline = converter.createPipeline();
      PipelineResult pipelineResult = pipeline.run();
      pipelineResult.waitUntilFinish();

      logMetrics( pipelineResult );

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error running local Beam Engine", e );
    }

  }

  public void runDataflow() {
    TransMeta transMeta = spoon.getActiveTransformation();
    if ( transMeta == null ) {
      showMessage( "Sorry", "The Apache Beam implementation works with transformations only." );
      return;
    }
    try {

      showMessage( "Sorry",
        "This is hardcoded to work with my environment." + Const.CR + "I'll add configuration options soon so you can try this as well." + Const.CR + "Thanks for your understanding," + Const.CR
          + Const.CR + "Matt" );

      TransMetaPipelineConverter converter = new TransMetaPipelineConverter( transMeta, spoon.getMetaStore() );
      DataflowPipelineOptions options = PipelineOptionsFactory.as( DataflowPipelineOptions.class );

      // Add all the jar files in lib/ to the classpath.
      // Later we'll add the plugins as well...
      //
      List<String> libraries = new ArrayList<>(  );
      File libFolder = new File("lib");
      File[] files = libFolder.listFiles( new FilenameFilter() {
        @Override public boolean accept( File dir, String name ) {
          return name.endsWith( ".jar" );
        }
      } );
      for (File file : files) {
        libraries.add(file.getAbsolutePath());
        System.out.println("Adding library : "+file.getAbsolutePath());
      }
      options.setFilesToStage( libraries );

      options.setProject( "kettledataflow" );
      options.setAppName( "Kettle" );
      options.setStagingLocation( "gs://kettledataflow/binaries" );
      options.setTempLocation( "gs://kettledataflow/tmp/" );
      Pipeline pipeline = converter.createPipeline( DataflowRunner.class, options );

      PipelineResult pipelineResult = pipeline.run();

      logMetrics( pipelineResult );

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error running GCP Dataflow Beam Engine", e );
    }
  }

  private void logMetrics( PipelineResult pipelineResult ) {
    LogChannelInterface log = spoon.getLog();
    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      log.logBasic( "Name: " + result.getName() + " Attempted: " + result.getAttempted() + " Committed: " + result.getCommitted() );
    }

  }

  public void showMessage( String title, String message ) {

    MessageBox messageBox = new MessageBox( spoon.getShell(), SWT.ICON_INFORMATION | SWT.CLOSE );
    messageBox.setText( title );
    messageBox.setMessage( message );
    messageBox.open();
  }

  public void createFileDefinition() {

    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    FileDefinition fileDefinition = new FileDefinition();
    fileDefinition.setName( "My File Definition" );
    boolean ok = false;
    while ( !ok ) {
      FileDefinitionDialog dialog = new FileDefinitionDialog( spoon.getShell(), fileDefinition );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( fileDefinition.getName() ) != null ) {
            MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "BeamHelper.Error.FileDefintionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "BeamHelper.Error.FileDefintionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( fileDefinition );
              ok = true;
            }
          } else {
            factory.saveElement( fileDefinition );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( spoon.getShell(),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Title" ),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Message" ),
            exception );
          return;
        }
      } else {
        // Cancel
        return;
      }
    }
  }

  public void editFileDefinition() {
    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToEdit.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToEdit.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        FileDefinition fileDefinition = factory.loadElement( choice );

        FileDefinitionDialog dialog = new FileDefinitionDialog( spoon.getShell(), fileDefinition );
        if ( dialog.open() ) {
          // write to metastore...
          try {
            factory.saveElement( fileDefinition );
          } catch ( Exception exception ) {
            new ErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Message" ),
              exception );
            return;
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.ErrorEditingDefinition.Message" ), e );
    }
  }

  public void deleteFileDefinition() {
    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToDelete.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToDelete.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        FileDefinition fileDefinition = factory.loadElement( choice );

        MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
        box.setText( BaseMessages.getString( PKG, "BeamHelper.DeleteDefinitionConfirmation.Title" ) );
        box.setMessage( BaseMessages.getString( PKG, "BeamHelper.DeleteDefinitionConfirmation.Message", choice ) );
        int answer = box.open();
        if ( ( answer & SWT.YES ) != 0 ) {
          try {
            factory.deleteElement( choice );
          } catch ( Exception exception ) {
            new ErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Message", choice ),
              exception );
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Message" ), e );
    }
  }

  public void testDataflow() {
    try {

      Storage storage = StorageOptions.getDefaultInstance().getService();

      System.out.println("Buckets:");
      Page<Bucket> buckets = storage.list();
      for (Bucket bucket : buckets.iterateAll()) {
        System.out.println(bucket.toString());
      }

      MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec( "/tmp/" );
      System.out.println("Scheme for /tmp: "+metadata.resourceId().getScheme());
      metadata = FileSystems.matchSingleFileSpec( "gs://kettledataflow" );
      System.out.println("Scheme for gs://kettledataflow: "+metadata.resourceId().getScheme());

    } catch(Exception e) {
      new ErrorDialog(spoon.getShell(), "Error", "Error", e);
    }
  }
}
