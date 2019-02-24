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

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.BeamJobConfigDialog;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.metastore.FileDefinitionDialog;
import org.kettle.beam.metastore.JobParameter;
import org.kettle.beam.metastore.RunnerType;
import org.kettle.beam.pipeline.KettleBeamPipelineExecutor;
import org.kettle.beam.pipeline.TransMetaPipelineConverter;
import org.kettle.beam.pipeline.fatjar.FatJarBuilder;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorAdapter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.plugins.KettleURLClassLoader;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

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


  public void runBeam() {

    TransMeta transMeta = spoon.getActiveTransformation();
    if ( transMeta == null ) {
      showMessage( "Sorry", "The Apache Beam implementation works with transformations only." );
      return;
    }

    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectBeamJobConfigToRun.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectBeamJobConfigToRun.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        final BeamJobConfig config = factory.loadElement( choice );

        Runnable runnable = new Runnable() {
          @Override public void run() {

            try {
              // KettleURLClassLoader kettleURLClassLoader = createKettleURLClassLoader(
              //  BeamConst.findLibraryFilesToStage( null, transMeta.environmentSubstitute( config.getPluginsToStage() ), true, true )
              //);

              ClassLoader pluginClassLoader = BeamHelper.this.getClass().getClassLoader();
              KettleBeamPipelineExecutor executor = new KettleBeamPipelineExecutor( spoon.getLog(), transMeta, config, spoon.getMetaStore(), pluginClassLoader );
              executor.execute();
            } catch ( Exception e ) {
              spoon.getDisplay().asyncExec( new Runnable() {
                @Override public void run() {
                  new ErrorDialog( spoon.getShell(), "Error", "There was an error building or executing the pipeline. Use the 'Details' button for more information: "+e.getMessage(), e );
                }
              } );
            }
          }
        };

        // Create a new thread on the class loader
        //
        Thread thread = new Thread(runnable);
        thread.start();

        showMessage( "Transformation started",
          "Your transformation was started with the selected Beam Runner." + Const.CR +
            "Now check the spoon console logging for execution feedback and metrics on the various steps." + Const.CR +
            "Not all steps are supported, check the project READ.me and Wiki for up-to-date information" + Const.CR + Const.CR +
            "Enjoy Kettle!"
        );

        TransGraph transGraph = spoon.getActiveTransGraph();
        if ( !transGraph.isExecutionResultsPaneVisible() ) {
          transGraph.showExecutionResults();
          CTabItem transLogTab = transGraph.transLogDelegate.getTransLogTab();
          transLogTab.getParent().setSelection( transLogTab );
        }


      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(),
        BaseMessages.getString( PKG, "BeamHelper.ErrorRunningTransOnBeam.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.ErrorRunningTransOnBeam.Message" ),
        e
      );
    }

  }

  private KettleURLClassLoader createKettleURLClassLoader(List<String> jarFilenames) throws MalformedURLException {

    URL[] urls = new URL[jarFilenames.size()];
    for (int i=0;i<urls.length;i++) {
      urls[i] = new File( jarFilenames.get(i) ).toURI().toURL();
    }
    return new KettleURLClassLoader( urls, ClassLoader.getSystemClassLoader() );
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


  public void createBeamJobConfig() {

    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    BeamJobConfig config = new BeamJobConfig();
    config.setName( "My Beam Job Config" );
    boolean ok = false;
    while ( !ok ) {
      BeamJobConfigDialog dialog = new BeamJobConfigDialog( spoon.getShell(), config );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( config.getName() ) != null ) {
            MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "BeamHelper.Error.BeamJobConfigExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "BeamHelper.Error.BeamJobConfigExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( config );
              ok = true;
            }
          } else {
            factory.saveElement( config );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( spoon.getShell(),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Title" ),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Message" ),
            exception );
          return;
        }
      } else {
        // Cancel
        return;
      }
    }
  }

  public void editBeamJobConfig() {
    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToEdit.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToEdit.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        BeamJobConfig beamJobConfig = factory.loadElement( choice );

        BeamJobConfigDialog dialog = new BeamJobConfigDialog( spoon.getShell(), beamJobConfig );
        if ( dialog.open() ) {
          // write to metastore...
          try {
            factory.saveElement( beamJobConfig );
          } catch ( Exception exception ) {
            new ErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Message" ),
              exception );
            return;
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.ErrorEditingJobConfig.Message" ), e );
    }
  }

  public void deleteBeamJobConfig() {
    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToDelete.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToDelete.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
        box.setText( BaseMessages.getString( PKG, "BeamHelper.DeleteJobConfigConfirmation.Title" ) );
        box.setMessage( BaseMessages.getString( PKG, "BeamHelper.DeleteJobConfigConfirmation.Message", choice ) );
        int answer = box.open();
        if ( ( answer & SWT.YES ) != 0 ) {
          try {
            factory.deleteElement( choice );
          } catch ( Exception exception ) {
            new ErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Message", choice ),
              exception );
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Message" ), e );
    }
  }

  public void generateFatJar() {

    final Shell shell = Spoon.getInstance().getShell();

    // TODO: Ask the user about these 2 next lines...
    //
    final String filename = "/tmp/kettle-beam-fat.jar";
    final String pluginFolders = "kettle-beam,kettle-json-plugin,kettle-json-plugin,Neo4JOutput";

    try {
      IRunnableWithProgress op = new IRunnableWithProgress() {
        public void run( IProgressMonitor monitor ) throws InvocationTargetException, InterruptedException {
          try {

            VariableSpace space = Variables.getADefaultVariableSpace();
            List<String> files = BeamConst.findLibraryFilesToStage( null, pluginFolders, true, true );
            files.removeIf( s -> s.contains( "commons-logging" ) || s.contains( "log4j" ) || s.contains("xml-apis") );

            FatJarBuilder fatJarBuilder = new FatJarBuilder( filename, files );
            fatJarBuilder.buildTargetJar();

          } catch ( Exception e ) {
            throw new InvocationTargetException( e, "Error building fat jar: "+e.getMessage());
          }
        }
      };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );

      MessageBox box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
      box.setText( "Fat jar created" );
      box.setMessage( "A fat jar was successfully created : "+filename+Const.CR+"Included plugin folders: "+pluginFolders );
      box.open();

    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Error creating fat jar", e );
    }

  }

  public void exportMetaStore() {
    final Shell shell = Spoon.getInstance().getShell();
    final IMetaStore metaStore = Spoon.getInstance().getMetaStore();
    final String filename = "/tmp/metastore.json";

    try {
      SerializableMetaStore sms = new SerializableMetaStore( metaStore );
      FileOutputStream fos = new FileOutputStream( filename );
      fos.write( sms.toJson().getBytes( "UTF-8" ));
      fos.flush();
      fos.close();

      MessageBox box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
      box.setText( "Metastore exported" );
      box.setMessage( "All current metastore entries were exported to "+filename);
      box.open();

    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Error exporting metastore json", e );
    }

  }

}
