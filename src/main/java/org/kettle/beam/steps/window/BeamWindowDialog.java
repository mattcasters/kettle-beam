
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
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class BeamWindowDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamWindow.class; // for i18n purposes, needed by Translator2!!
  private final BeamWindowMeta input;

  int middle;
  int margin;

  private Combo wWindowType;
  private TextVar wDuration;
  private TextVar wEvery;

  public BeamWindowDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamWindowMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamWindowDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

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

    Label wlWindowType = new Label( shell, SWT.RIGHT );
    wlWindowType.setText( BaseMessages.getString( PKG, "BeamWindowDialog.WindowType" ) );
    props.setLook( wlWindowType );
    FormData fdlWindowType = new FormData();
    fdlWindowType.left = new FormAttachment( 0, 0 );
    fdlWindowType.top = new FormAttachment( lastControl, margin );
    fdlWindowType.right = new FormAttachment( middle, -margin );
    wlWindowType.setLayoutData( fdlWindowType );
    wWindowType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWindowType );
    wWindowType.setItems( BeamDefaults.WINDOW_TYPES );
    FormData fdWindowType = new FormData();
    fdWindowType.left = new FormAttachment( middle, 0 );
    fdWindowType.top = new FormAttachment( wlWindowType, 0, SWT.CENTER );
    fdWindowType.right = new FormAttachment( 100, 0 );
    wWindowType.setLayoutData( fdWindowType );
    lastControl = wWindowType;

    Label wlDuration = new Label( shell, SWT.RIGHT );
    wlDuration.setText( BaseMessages.getString( PKG, "BeamWindowDialog.Duration" ) );
    props.setLook( wlDuration );
    FormData fdlDuration = new FormData();
    fdlDuration.left = new FormAttachment( 0, 0 );
    fdlDuration.top = new FormAttachment( lastControl, margin );
    fdlDuration.right = new FormAttachment( middle, -margin );
    wlDuration.setLayoutData( fdlDuration );
    wDuration = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDuration );
    FormData fdDuration = new FormData();
    fdDuration.left = new FormAttachment( middle, 0 );
    fdDuration.top = new FormAttachment( wlDuration, 0, SWT.CENTER );
    fdDuration.right = new FormAttachment( 100, 0 );
    wDuration.setLayoutData( fdDuration );
    lastControl = wDuration;

    Label wlEvery = new Label( shell, SWT.RIGHT );
    wlEvery.setText( BaseMessages.getString( PKG, "BeamWindowDialog.Every" ) );
    props.setLook( wlEvery );
    FormData fdlEvery = new FormData();
    fdlEvery.left = new FormAttachment( 0, 0 );
    fdlEvery.top = new FormAttachment( lastControl, margin );
    fdlEvery.right = new FormAttachment( middle, -margin );
    wlEvery.setLayoutData( fdlEvery );
    wEvery = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEvery );
    FormData fdEvery = new FormData();
    fdEvery.left = new FormAttachment( middle, 0 );
    fdEvery.top = new FormAttachment( wlEvery, 0, SWT.CENTER );
    fdEvery.right = new FormAttachment( 100, 0 );
    wEvery.setLayoutData( fdEvery );
    lastControl = wEvery;

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
    wWindowType.addSelectionListener( lsDef );
    wDuration.addSelectionListener( lsDef );

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
    wWindowType.setText( Const.NVL( input.getWindowType(), "" ) );
    wDuration.setText( Const.NVL( input.getDuration(), "" ) );
    wEvery.setText( Const.NVL( input.getEvery(), "" ) );

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

  private void getInfo( BeamWindowMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setWindowType( wWindowType.getText() );
    in.setDuration( wDuration.getText() );
    in.setEvery( wEvery.getText() );

    input.setChanged();
  }
}