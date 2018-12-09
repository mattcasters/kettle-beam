package org.kettle.beam.metastore;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class BeamJobConfigDialog {

  private static Class<?> PKG = BeamJobConfigDialog.class; // for i18n purposes, needed by Translator2!!

  private BeamJobConfig config;

  private Shell parent;
  private Shell shell;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab;
  private CTabItem wParametersTab;
  private CTabItem wDataflowTab;

  private ScrolledComposite wGeneralSComp;
  private ScrolledComposite wParametersSComp;
  private ScrolledComposite wDataflowSComp;

  private Composite wGeneralComp;
  private Composite wParametersComp;
  private Composite wDataflowComp;


  private ComboVar wRunner;
  private TextVar wUserAgent;
  private TextVar wTempLocation;

  private TextVar wGcpProjectId;
  private TextVar wGcpAppName;
  private TextVar wGcpStagingLocation;
  private TextVar wPluginsToStage;

  private TableView wParameters;

  private Button wOK;
  private Button wCancel;
  
  private VariableSpace space;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  public BeamJobConfigDialog( Shell parent, BeamJobConfig config ) {
    this.parent = parent;
    this.config = config;
    props = PropsUI.getInstance();
    ok = false;

    // Just environment variables right now.
    //
    space = new Variables();
    space.initializeVariablesFrom( null );
  }

  public boolean open() {
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSlave() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // Buttons at the bottom of the dialo...
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e -> ok() );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

    // The rest of the dialog is for the widgets...
    //
    addFormWidgets();

    // Add listeners

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wDescription.addSelectionListener( selAdapter );
    wRunner.addSelectionListener( selAdapter );
    wUserAgent.addSelectionListener( selAdapter );
    wTempLocation.addSelectionListener( selAdapter );
    wPluginsToStage.addSelectionListener( selAdapter );
    wGcpProjectId.addSelectionListener( selAdapter );
    wGcpAppName.addSelectionListener( selAdapter );
    wGcpStagingLocation.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  private void addFormWidgets() {

    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // The description
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Description.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER );
    fdDescription.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdDescription.right = new FormAttachment( 95, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.top = new FormAttachment( lastControl, margin*2 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin*2 );


    addGeneralTab();



    // Google Cloud Platform Group

    Group wGcpGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    props.setLook( wGcpGroup );
    wGcpGroup.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpGroup.Title" ) );
    FormLayout gcpFormLayout = new FormLayout();
    gcpFormLayout.marginHeight = 10;
    gcpFormLayout.marginWidth = 10;
    wGcpGroup.setLayout( gcpFormLayout );

    // GcpProjectId
    //
    Label wlGcpProjectId = new Label( wGcpGroup, SWT.RIGHT );
    props.setLook( wlGcpProjectId );
    wlGcpProjectId.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpProjectId.Label" ) );
    FormData fdlGcpProjectId = new FormData();
    fdlGcpProjectId.top = new FormAttachment( 0, 0 );
    fdlGcpProjectId.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlGcpProjectId.right = new FormAttachment( middle, -margin );
    wlGcpProjectId.setLayoutData( fdlGcpProjectId );
    wGcpProjectId = new TextVar( space, wGcpGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpProjectId );
    FormData fdGcpProjectId = new FormData();
    fdGcpProjectId.top = new FormAttachment( wlGcpProjectId, 0, SWT.CENTER );
    fdGcpProjectId.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpProjectId.right = new FormAttachment( 95, 0 );
    wGcpProjectId.setLayoutData( fdGcpProjectId );

    // GcpAppName
    //
    Label wlGcpAppName = new Label( wGcpGroup, SWT.RIGHT );
    props.setLook( wlGcpAppName );
    wlGcpAppName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpAppName.Label" ) );
    FormData fdlGcpAppName = new FormData();
    fdlGcpAppName.top = new FormAttachment( wGcpProjectId, margin );
    fdlGcpAppName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlGcpAppName.right = new FormAttachment( middle, -margin );
    wlGcpAppName.setLayoutData( fdlGcpAppName );
    wGcpAppName = new TextVar( space, wGcpGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpAppName );
    FormData fdGcpAppName = new FormData();
    fdGcpAppName.top = new FormAttachment( wlGcpAppName, 0, SWT.CENTER );
    fdGcpAppName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpAppName.right = new FormAttachment( 95, 0 );
    wGcpAppName.setLayoutData( fdGcpAppName );

    // GcpStagingLocation
    //
    Label wlGcpStagingLocation = new Label( wGcpGroup, SWT.RIGHT );
    props.setLook( wlGcpStagingLocation );
    wlGcpStagingLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpStagingLocation.Label" ) );
    FormData fdlGcpStagingLocation = new FormData();
    fdlGcpStagingLocation.top = new FormAttachment( wGcpAppName, margin );
    fdlGcpStagingLocation.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlGcpStagingLocation.right = new FormAttachment( middle, -margin );
    wlGcpStagingLocation.setLayoutData( fdlGcpStagingLocation );
    wGcpStagingLocation = new TextVar( space, wGcpGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpStagingLocation );
    FormData fdGcpStagingLocation = new FormData();
    fdGcpStagingLocation.top = new FormAttachment( wlGcpStagingLocation, 0, SWT.CENTER );
    fdGcpStagingLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpStagingLocation.right = new FormAttachment( 95, 0 );
    wGcpStagingLocation.setLayoutData( fdGcpStagingLocation );
    

    FormData fdGcpGroup = new FormData();
    fdGcpGroup.top = new FormAttachment( lastControl, margin*2);
    fdGcpGroup.left = new FormAttachment( 0, margin );
    fdGcpGroup.right = new FormAttachment( 100, -margin );
    wGcpGroup.setLayoutData( fdGcpGroup );
    lastControl = wGcpGroup;

    // Parameters
    //
    Label wlParameters = new Label( shell, SWT.LEFT );
    props.setLook( wlParameters );
    wlParameters.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Parameters.Label" ) );
    FormData fdlParameters = new FormData();
    fdlParameters.top = new FormAttachment( lastControl, margin );
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.right = new FormAttachment( 100, 0);
    wlParameters.setLayoutData( fdlParameters );
    lastControl = wlParameters;



  }

  private void addGeneralTab() {
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab .setText( "General" );

    wGeneralSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wGeneralSComp.setLayout( new FillLayout() );

    wGeneralComp = new Composite( wGeneralSComp, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout( fileLayout );

    // Runner
    //
    Label wlRunner = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlRunner );
    wlRunner.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Runner.Label" ) );
    FormData fdlRunner = new FormData();
    fdlRunner.top = new FormAttachment( null, 0);
    fdlRunner.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlRunner.right = new FormAttachment( middle, -margin );
    wlRunner.setLayoutData( fdlRunner );
    wRunner = new ComboVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRunner.setItems( RunnerType.getNames() );
    props.setLook( wRunner );
    FormData fdRunner = new FormData();
    fdRunner.top = new FormAttachment( wlRunner, 0, SWT.CENTER );
    fdRunner.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdRunner.right = new FormAttachment( 95, 0 );
    wRunner.setLayoutData( fdRunner );
    Control lastControl = wRunner;

    // UserAgent
    //
    Label wlUserAgent = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlUserAgent );
    wlUserAgent.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.UserAgent.Label" ) );
    FormData fdlUserAgent = new FormData();
    fdlUserAgent.top = new FormAttachment( lastControl, margin );
    fdlUserAgent.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlUserAgent.right = new FormAttachment( middle, -margin );
    wlUserAgent.setLayoutData( fdlUserAgent );
    wUserAgent = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserAgent );
    FormData fdUserAgent = new FormData();
    fdUserAgent.top = new FormAttachment( wlUserAgent, 0, SWT.CENTER );
    fdUserAgent.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdUserAgent.right = new FormAttachment( 95, 0 );
    wUserAgent.setLayoutData( fdUserAgent );
    lastControl = wUserAgent;

    // TempLocation
    //
    Label wlTempLocation = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlTempLocation );
    wlTempLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.TempLocation.Label" ) );
    FormData fdlTempLocation = new FormData();
    fdlTempLocation.top = new FormAttachment( lastControl, margin );
    fdlTempLocation.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlTempLocation.right = new FormAttachment( middle, -margin );
    wlTempLocation.setLayoutData( fdlTempLocation );
    wTempLocation = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTempLocation );
    FormData fdTempLocation = new FormData();
    fdTempLocation.top = new FormAttachment( wlTempLocation, 0, SWT.CENTER );
    fdTempLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdTempLocation.right = new FormAttachment( 95, 0 );
    wTempLocation.setLayoutData( fdTempLocation );
    lastControl = wTempLocation;

    // PluginsToStage
    //
    Label wlPluginsToStage = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlPluginsToStage );
    wlPluginsToStage.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.PluginsToStage.Label" ) );
    FormData fdlPluginsToStage = new FormData();
    fdlPluginsToStage.top = new FormAttachment( lastControl, margin );
    fdlPluginsToStage.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlPluginsToStage.right = new FormAttachment( middle, -margin );
    wlPluginsToStage.setLayoutData( fdlPluginsToStage );
    wPluginsToStage = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPluginsToStage );
    FormData fdPluginsToStage = new FormData();
    fdPluginsToStage.top = new FormAttachment( wlPluginsToStage, 0, SWT.CENTER );
    fdPluginsToStage.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPluginsToStage.right = new FormAttachment( 95, 0 );
    wPluginsToStage.setLayoutData( fdPluginsToStage );


    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.pack();
    Rectangle bounds = wGeneralComp.getBounds();

    wGeneralSComp.setContent( wGeneralComp );
    wGeneralSComp.setExpandHorizontal( true );
    wGeneralSComp.setExpandVertical( true );
    wGeneralSComp.setMinWidth( bounds.width );
    wGeneralSComp.setMinHeight( bounds.height );

    wGeneralTab.setControl( wGeneralSComp );
  }

  private void addParametersTab() {
    wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
    wParametersTab .setText( "Parameters" );

    wParametersSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wParametersSComp.setLayout( new FillLayout() );

    wParametersComp = new Composite( wParametersSComp, SWT.NONE );
    props.setLook( wParametersComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wParametersComp.setLayout( fileLayout );


    ColumnInfo[] columnInfos = new ColumnInfo[] {
      new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
    };

    wParameters = new TableView( space, wParametersComp, SWT.BORDER, columnInfos, config.getParameters().size(), null, props );
    props.setLook( wParameters );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( 100, 0 );
    fdParameters.top = new FormAttachment( 0, 0);
    fdParameters.bottom = new FormAttachment( 100, 0 );
    wParameters.setLayoutData( fdParameters );

    FormData fdParametersComp = new FormData();
    fdParametersComp.left = new FormAttachment( 0, 0 );
    fdParametersComp.top = new FormAttachment( 0, 0 );
    fdParametersComp.right = new FormAttachment( 100, 0 );
    fdParametersComp.bottom = new FormAttachment( 100, 0 );
    wParametersComp.setLayoutData( fdParametersComp );

    wParametersComp.pack();
    Rectangle bounds = wParametersComp.getBounds();

    wParametersSComp.setContent( wParametersComp );
    wParametersSComp.setExpandHorizontal( true );
    wParametersSComp.setExpandVertical( true );
    wParametersSComp.setMinWidth( bounds.width );
    wParametersSComp.setMinHeight( bounds.height );

    wParametersTab.setControl( wParametersSComp );
  }


  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( config.getName(), "" ) );
    wDescription.setText( Const.NVL( config.getDescription(), "" ) );

    // General
    wRunner.setText( Const.NVL(config.getRunnerTypeName(), "") );
    wUserAgent.setText( Const.NVL(config.getUserAgent(), "") );
    wTempLocation.setText( Const.NVL(config.getTempLocation(), "") );

    // GCP
    wGcpProjectId.setText( Const.NVL(config.getGcpProjectId(), "") );
    wGcpAppName.setText( Const.NVL(config.getGcpAppName(), "") );
    wGcpStagingLocation.setText( Const.NVL(config.getGcpStagingLocation(), "") );
    wPluginsToStage.setText( Const.NVL(config.getPluginsToStage(), "") );

    // Parameters
    //
    int rowNr=0;
    for ( JobParameter parameter : config.getParameters()) {
      TableItem item = wParameters.table.getItem( rowNr++ );
      item.setText( 1, Const.NVL(parameter.getVariable(), "") );
      item.setText( 2, Const.NVL(parameter.getValue(), "") );
    }
    wParameters.setRowNums();
    wParameters.optWidth( true );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {
    if ( StringUtils.isEmpty( wName.getText() ) ) {
      MessageBox box = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      box.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.NoNameDialog.Title" ) );
      box.setMessage( BaseMessages.getString( PKG, "BeamJobConfigDialog.NoNameDialog.Message" ) );
      box.open();
      return;
    }
    getInfo( config );
    ok = true;
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( BeamJobConfig cfg ) {
    cfg.setName( wName.getText() );
    cfg.setDescription( wDescription.getText() );
    cfg.setRunnerTypeName( wRunner.getText() );
    cfg.setUserAgent( wUserAgent.getText() );
    cfg.setTempLocation( wTempLocation.getText() );
    cfg.setGcpProjectId( wGcpProjectId.getText() );
    cfg.setGcpAppName( wGcpAppName.getText() );
    cfg.setGcpStagingLocation( wGcpStagingLocation.getText() );
    cfg.setPluginsToStage( wPluginsToStage.getText() );

    cfg.getParameters().clear();
    for (int i=0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      cfg.getParameters().add(new JobParameter( item.getText(1), item.getText(2) ));
    }

  }
}
