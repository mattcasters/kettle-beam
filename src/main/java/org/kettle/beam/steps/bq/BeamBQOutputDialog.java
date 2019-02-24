
package org.kettle.beam.steps.bq;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class BeamBQOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamBQOutputDialog.class; // for i18n purposes, needed by Translator2!!
  private final BeamBQOutputMeta input;

  int middle;
  int margin;

  private TextVar wProjectId;
  private TextVar wDatasetId;
  private TextVar wTableId;
  private Button wCreateIfNeeded;
  private Button wTruncateTable;
  private Button wFailIfNotEmpty;

  public BeamBQOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamBQOutputMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.DialogTitle" ) );

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

    Label wlProjectId = new Label( shell, SWT.RIGHT );
    wlProjectId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.ProjectId" ) );
    props.setLook( wlProjectId );
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment( 0, 0 );
    fdlProjectId.top = new FormAttachment( lastControl, margin );
    fdlProjectId.right = new FormAttachment( middle, -margin );
    wlProjectId.setLayoutData( fdlProjectId );
    wProjectId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProjectId );
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment( middle, 0 );
    fdProjectId.top = new FormAttachment( wlProjectId, 0, SWT.CENTER );
    fdProjectId.right = new FormAttachment( 100, 0 );
    wProjectId.setLayoutData( fdProjectId );
    lastControl = wProjectId;

    Label wlDatasetId = new Label( shell, SWT.RIGHT );
    wlDatasetId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.DatasetId" ) );
    props.setLook( wlDatasetId );
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment( 0, 0 );
    fdlDatasetId.top = new FormAttachment( lastControl, margin );
    fdlDatasetId.right = new FormAttachment( middle, -margin );
    wlDatasetId.setLayoutData( fdlDatasetId );
    wDatasetId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatasetId );
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment( middle, 0 );
    fdDatasetId.top = new FormAttachment( wlDatasetId, 0, SWT.CENTER );
    fdDatasetId.right = new FormAttachment( 100, 0 );
    wDatasetId.setLayoutData( fdDatasetId );
    lastControl = wDatasetId;

    Label wlTableId = new Label( shell, SWT.RIGHT );
    wlTableId.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.TableId" ) );
    props.setLook( wlTableId );
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment( 0, 0 );
    fdlTableId.top = new FormAttachment( lastControl, margin );
    fdlTableId.right = new FormAttachment( middle, -margin );
    wlTableId.setLayoutData( fdlTableId );
    wTableId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTableId );
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment( middle, 0 );
    fdTableId.top = new FormAttachment( wlTableId, 0, SWT.CENTER );
    fdTableId.right = new FormAttachment( 100, 0 );
    wTableId.setLayoutData( fdTableId );
    lastControl = wTableId;

    Label wlCreateIfNeeded = new Label( shell, SWT.RIGHT );
    wlCreateIfNeeded.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.CreateIfNeeded" ) );
    props.setLook( wlCreateIfNeeded );
    FormData fdlCreateIfNeeded = new FormData();
    fdlCreateIfNeeded.left = new FormAttachment( 0, 0 );
    fdlCreateIfNeeded.top = new FormAttachment( lastControl, margin );
    fdlCreateIfNeeded.right = new FormAttachment( middle, -margin );
    wlCreateIfNeeded.setLayoutData( fdlCreateIfNeeded );
    wCreateIfNeeded = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wCreateIfNeeded );
    FormData fdCreateIfNeeded = new FormData();
    fdCreateIfNeeded.left = new FormAttachment( middle, 0 );
    fdCreateIfNeeded.top = new FormAttachment( wlCreateIfNeeded, 0, SWT.CENTER );
    fdCreateIfNeeded.right = new FormAttachment( 100, 0 );
    wCreateIfNeeded.setLayoutData( fdCreateIfNeeded );
    lastControl = wCreateIfNeeded;

    Label wlTruncateTable = new Label( shell, SWT.RIGHT );
    wlTruncateTable.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.TruncateTable" ) );
    props.setLook( wlTruncateTable );
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment( 0, 0 );
    fdlTruncateTable.top = new FormAttachment( lastControl, margin );
    fdlTruncateTable.right = new FormAttachment( middle, -margin );
    wlTruncateTable.setLayoutData( fdlTruncateTable );
    wTruncateTable = new Button( shell,  SWT.CHECK | SWT.LEFT );
    props.setLook( wTruncateTable );
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.left = new FormAttachment( middle, 0 );
    fdTruncateTable.top = new FormAttachment( wlTruncateTable, 0, SWT.CENTER );
    fdTruncateTable.right = new FormAttachment( 100, 0 );
    wTruncateTable.setLayoutData( fdTruncateTable );
    lastControl = wTruncateTable;

    Label wlFailIfNotEmpty = new Label( shell, SWT.RIGHT );
    wlFailIfNotEmpty.setText( BaseMessages.getString( PKG, "BeamBQOutputDialog.FailIfNotEmpty" ) );
    props.setLook( wlFailIfNotEmpty );
    FormData fdlFailIfNotEmpty = new FormData();
    fdlFailIfNotEmpty.left = new FormAttachment( 0, 0 );
    fdlFailIfNotEmpty.top = new FormAttachment( lastControl, margin );
    fdlFailIfNotEmpty.right = new FormAttachment( middle, -margin );
    wlFailIfNotEmpty.setLayoutData( fdlFailIfNotEmpty );
    wFailIfNotEmpty = new Button( shell,  SWT.CHECK | SWT.LEFT );
    props.setLook( wFailIfNotEmpty );
    FormData fdFailIfNotEmpty = new FormData();
    fdFailIfNotEmpty.left = new FormAttachment( middle, 0 );
    fdFailIfNotEmpty.top = new FormAttachment( wlFailIfNotEmpty, 0, SWT.CENTER );
    fdFailIfNotEmpty.right = new FormAttachment( 100, 0 );
    wFailIfNotEmpty.setLayoutData( fdFailIfNotEmpty );
    lastControl = wFailIfNotEmpty;



    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );

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
    wProjectId.addSelectionListener( lsDef );
    wDatasetId.addSelectionListener( lsDef );
    wTableId.addSelectionListener( lsDef );

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
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wCreateIfNeeded.setSelection( input.isCreatingIfNeeded() );
    wTruncateTable.setSelection( input.isTruncatingTable() );
    wFailIfNotEmpty.setSelection( input.isFailingIfNotEmpty() );
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

  private void getInfo( BeamBQOutputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setProjectId( wProjectId.getText() );
    in.setDatasetId( wDatasetId.getText() );
    in.setTableId( wTableId.getText() );
    in.setCreatingIfNeeded( wCreateIfNeeded.getSelection() );
    in.setTruncatingTable( wTruncateTable.getSelection() );
    in.setFailingIfNotEmpty( wFailIfNotEmpty.getSelection() );
    input.setChanged();
  }
}
