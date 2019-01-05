
package org.kettle.beam.steps.io;

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


public class BeamOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamOutput.class; // for i18n purposes, needed by Translator2!!
  private final BeamOutputMeta input;

  int middle;
  int margin;

  private boolean getpreviousFields = false;

  private TextVar wOutputLocation;
  private Combo wFileDefinition;
  private TextVar wFilePrefix;
  private TextVar wFileSuffix;
  private Button wWindowed;

  public BeamOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamOutputMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamOutputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    String fileDefinitionNames[];
    try {
      List<String> fileDefinitionNameList = new MetaStoreFactory<FileDefinition>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE).getElementNames();
      Collections.sort(fileDefinitionNameList);

      fileDefinitionNames = fileDefinitionNameList.toArray(new String[0]);
    } catch(Exception e) {
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

    Label wlOutputLocation = new Label( shell, SWT.RIGHT );
    wlOutputLocation.setText( BaseMessages.getString( PKG, "BeamOutputDialog.OutputLocation" ) );
    props.setLook( wlOutputLocation );
    FormData fdlOutputLocation = new FormData();
    fdlOutputLocation.left = new FormAttachment( 0, 0 );
    fdlOutputLocation.top = new FormAttachment( lastControl, margin );
    fdlOutputLocation.right = new FormAttachment( middle, -margin );
    wlOutputLocation.setLayoutData( fdlOutputLocation );
    wOutputLocation = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputLocation );
    FormData fdOutputLocation = new FormData();
    fdOutputLocation.left = new FormAttachment( middle, 0 );
    fdOutputLocation.top = new FormAttachment( wlOutputLocation, 0, SWT.CENTER );
    fdOutputLocation.right = new FormAttachment( 100, 0 );
    wOutputLocation.setLayoutData( fdOutputLocation );
    lastControl = wOutputLocation;

    Label wlFilePrefix = new Label( shell, SWT.RIGHT );
    wlFilePrefix.setText( BaseMessages.getString( PKG, "BeamOutputDialog.FilePrefix" ) );
    props.setLook( wlFilePrefix );
    FormData fdlFilePrefix = new FormData();
    fdlFilePrefix.left = new FormAttachment( 0, 0 );
    fdlFilePrefix.top = new FormAttachment( lastControl, margin );
    fdlFilePrefix.right = new FormAttachment( middle, -margin );
    wlFilePrefix.setLayoutData( fdlFilePrefix );
    wFilePrefix = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilePrefix );
    FormData fdFilePrefix = new FormData();
    fdFilePrefix.left = new FormAttachment( middle, 0 );
    fdFilePrefix.top = new FormAttachment( wlFilePrefix, 0, SWT.CENTER );
    fdFilePrefix.right = new FormAttachment( 100, 0 );
    wFilePrefix.setLayoutData( fdFilePrefix );
    lastControl = wFilePrefix;

    Label wlFileSuffix = new Label( shell, SWT.RIGHT );
    wlFileSuffix.setText( BaseMessages.getString( PKG, "BeamOutputDialog.FileSuffix" ) );
    props.setLook( wlFileSuffix );
    FormData fdlFileSuffix = new FormData();
    fdlFileSuffix.left = new FormAttachment( 0, 0 );
    fdlFileSuffix.top = new FormAttachment( lastControl, margin );
    fdlFileSuffix.right = new FormAttachment( middle, -margin );
    wlFileSuffix.setLayoutData( fdlFileSuffix );
    wFileSuffix = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFileSuffix );
    FormData fdFileSuffix = new FormData();
    fdFileSuffix.left = new FormAttachment( middle, 0 );
    fdFileSuffix.top = new FormAttachment( wlFileSuffix, 0, SWT.CENTER );
    fdFileSuffix.right = new FormAttachment( 100, 0 );
    wFileSuffix.setLayoutData( fdFileSuffix );
    lastControl = wFileSuffix;
    
    Label wlWindowed = new Label( shell, SWT.RIGHT );
    wlWindowed.setText( BaseMessages.getString( PKG, "BeamOutputDialog.Windowed" ) );
    props.setLook( wlWindowed );
    FormData fdlWindowed = new FormData();
    fdlWindowed.left = new FormAttachment( 0, 0 );
    fdlWindowed.top = new FormAttachment( lastControl, margin );
    fdlWindowed.right = new FormAttachment( middle, -margin );
    wlWindowed.setLayoutData( fdlWindowed );
    wWindowed = new Button( shell, SWT.CHECK );
    props.setLook( wWindowed );
    FormData fdWindowed = new FormData();
    fdWindowed.left = new FormAttachment( middle, 0 );
    fdWindowed.top = new FormAttachment( wlWindowed, 0, SWT.CENTER );
    fdWindowed.right = new FormAttachment( 100, 0 );
    wWindowed.setLayoutData( fdWindowed );
    lastControl = wWindowed;

    Label wlFileDefinition = new Label( shell, SWT.RIGHT );
    wlFileDefinition.setText( BaseMessages.getString( PKG, "BeamOutputDialog.FileDefinition" ) );
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
    wOutputLocation.addSelectionListener( lsDef );
    wFilePrefix.addSelectionListener( lsDef );
    wFileSuffix.addSelectionListener( lsDef );

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
    wOutputLocation.setText(Const.NVL(input.getOutputLocation(), ""));
    wFilePrefix.setText(Const.NVL(input.getFilePrefix(), ""));
    wFileSuffix.setText(Const.NVL(input.getFileSuffix(), ""));
    wWindowed.setSelection( input.isWindowed() );

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

  private void getInfo( BeamOutputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setFileDescriptionName( wFileDefinition.getText() );
    in.setOutputLocation( wOutputLocation.getText() );
    in.setFilePrefix( wFilePrefix.getText() );
    in.setFileSuffix( wFileSuffix.getText() );
    in.setWindowed( wWindowed.getSelection() );

    input.setChanged();
  }
}