
package org.kettle.beam.steps.window;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.core.BeamDefaults;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class BeamTimestampDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamTimestampDialog.class; // for i18n purposes, needed by Translator2!!
  private final BeamTimestampMeta input;

  int middle;
  int margin;

  private Combo wFieldName;
  private Button wReading;

  public BeamTimestampDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamTimestampMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamTimestampDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    String[] fieldNames;
    try {
      fieldNames = transMeta.getPrevStepFields( stepMeta ).getFieldNames();
    } catch( KettleException e ) {
      log.logError("Error getting fields from previous steps", e);
      fieldNames = new String[] {};
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

    Label wlFieldName = new Label( shell, SWT.RIGHT );
    wlFieldName.setText( BaseMessages.getString( PKG, "BeamTimestampDialog.FieldName" ) );
    props.setLook( wlFieldName );
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment( 0, 0 );
    fdlFieldName.top = new FormAttachment( lastControl, margin );
    fdlFieldName.right = new FormAttachment( middle, -margin );
    wlFieldName.setLayoutData( fdlFieldName );
    wFieldName = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldName );
    wFieldName.setItems( fieldNames );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, 0 );
    fdFieldName.top = new FormAttachment( wlFieldName, 0, SWT.CENTER );
    fdFieldName.right = new FormAttachment( 100, 0 );
    wFieldName.setLayoutData( fdFieldName );
    lastControl = wFieldName;

    Label wlReading = new Label( shell, SWT.RIGHT );
    wlReading.setText( BaseMessages.getString( PKG, "BeamTimestampDialog.Reading" ) );
    props.setLook( wlReading );
    FormData fdlReading = new FormData();
    fdlReading.left = new FormAttachment( 0, 0 );
    fdlReading.top = new FormAttachment( lastControl, margin );
    fdlReading.right = new FormAttachment( middle, -margin );
    wlReading.setLayoutData( fdlReading );
    wReading = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wReading );
    FormData fdReading = new FormData();
    fdReading.left = new FormAttachment( middle, 0 );
    fdReading.top = new FormAttachment( wlReading, 0, SWT.CENTER );
    fdReading.right = new FormAttachment( 100, 0 );
    wReading.setLayoutData( fdReading );
    lastControl = wReading;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );


    wOK.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wFieldName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener( SWT.Close, e->cancel());

    getData();
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
  public void getData() {
    wStepname.setText( stepname );
    wFieldName.setText( Const.NVL( input.getFieldName(), "" ) );
    wReading.setSelection( input.isReadingTimestamp() );

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

  private void getInfo( BeamTimestampMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setFieldName( wFieldName.getText() );
    in.setReadingTimestamp( wReading.getSelection() );

    input.setChanged();
  }
}