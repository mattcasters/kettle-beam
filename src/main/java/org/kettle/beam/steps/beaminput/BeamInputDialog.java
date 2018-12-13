
package org.kettle.beam.steps.beaminput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.Collections;
import java.util.List;


public class BeamInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamInput.class; // for i18n purposes, needed by Translator2!!
  private final BeamInputMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private TextVar wInputLocation;
  private Combo wFileDefinition;

  public BeamInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BeamInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    String fileDefinitionNames[];
    try {
      List<String> fileDefinitionNameList = new MetaStoreFactory<FileDefinition>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE).getElementNames();
      Collections.sort(fileDefinitionNameList);

      fileDefinitionNames = fileDefinitionNameList.toArray(new String[0]);
    } catch(Exception e) {
      log.logError("Error getting file definitions list", e);
      fileDefinitionNames = new String[] {};
    }

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    Label wlInputLocation = new Label( shell, SWT.RIGHT );
    wlInputLocation.setText( BaseMessages.getString( PKG, "BeamInputDialog.InputLocation" ) );
    props.setLook( wlInputLocation );
    FormData fdlInputLocation = new FormData();
    fdlInputLocation.left = new FormAttachment( 0, 0 );
    fdlInputLocation.top = new FormAttachment( lastControl, margin );
    fdlInputLocation.right = new FormAttachment( middle, -margin );
    wlInputLocation.setLayoutData( fdlInputLocation );
    wInputLocation = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputLocation );
    FormData fdInputLocation = new FormData();
    fdInputLocation.left = new FormAttachment( middle, 0 );
    fdInputLocation.top = new FormAttachment( wlInputLocation, 0, SWT.CENTER );
    fdInputLocation.right = new FormAttachment( 100, 0 );
    wInputLocation.setLayoutData( fdInputLocation );
    lastControl = wInputLocation;

    Label wlFileDefinition = new Label( shell, SWT.RIGHT );
    wlFileDefinition.setText( BaseMessages.getString( PKG, "BeamInputDialog.FileDefinition" ) );
    props.setLook( wlFileDefinition );
    FormData fdlFileDefinition = new FormData();
    fdlFileDefinition.left = new FormAttachment( 0, 0 );
    fdlFileDefinition.top = new FormAttachment( lastControl, margin );
    fdlFileDefinition.right = new FormAttachment( middle, -margin );
    wlFileDefinition.setLayoutData( fdlFileDefinition );
    wFileDefinition = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileDefinition );
    wFileDefinition.setItems(fileDefinitionNames);
    FormData fdFileDefinition = new FormData();
    fdFileDefinition.left = new FormAttachment( middle, 0 );
    fdFileDefinition.top = new FormAttachment( wlFileDefinition, 0, SWT.CENTER );
    fdFileDefinition.right = new FormAttachment( 100, 0 );
    wFileDefinition.setLayoutData( fdFileDefinition );
    lastControl = wFileDefinition;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wFileDefinition.addSelectionListener( lsDef );
    wInputLocation.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( );
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

 

  /**
   * Populate the widgets.
   */
  public void getData( ) {
    wStepname.setText( stepname );
    wFileDefinition.setText(Const.NVL(input.getFileDescriptionName(), ""));
    wInputLocation.setText(Const.NVL(input.getInputLocation(), ""));

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    getInfo( input );

    dispose();
  }

  private void getInfo( BeamInputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setFileDescriptionName( wFileDefinition.getText() );
    in.setInputLocation( wInputLocation.getText() );

    input.setChanged();
  }
}