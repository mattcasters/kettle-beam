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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.util.BeamConst;
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

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab;
  private CTabItem wParametersTab;
  private CTabItem wDataflowTab;
  private CTabItem wSparkTab;

  private ScrolledComposite wGeneralSComp;
  private ScrolledComposite wDataflowSComp;
  private ScrolledComposite wSparkSComp;

  private Composite wGeneralComp;
  private Composite wParametersComp;
  private Composite wDataflowComp;
  private Composite wSparkComp;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private ComboVar wRunner;
  private TextVar wUserAgent;
  private TextVar wTempLocation;
  private TextVar wPluginsToStage;

  // Parameters

  private TableView wParameters;

  // GCP settings

  private TextVar wGcpProjectId;
  private TextVar wGcpAppName;
  private TextVar wGcpStagingLocation;
  private TextVar wGcpInitialNumberOfWorkers;
  private TextVar wGcpMaximumNumberOfWorkers;
  private TextVar wGcpAutoScalingAlgorithm;
  private ComboVar wGcpWorkerMachineType;
  private TextVar wGcpWorkerDiskType;
  private TextVar wGcpDiskSizeGb;
  private ComboVar wGcpRegion;
  private TextVar wGcpZone;
  private Button wGcpStreaming;

  // Spark settings

  private TextVar wSparkMaster;
  private TextVar wSparkDeployFolder;
  private TextVar wSparkBatchIntervalMillis;
  private TextVar wSparkCheckpointDir;
  private TextVar wSparkCheckpointDurationMillis;
  private Button wsparkEnableSparkMetricSinks;
  private TextVar wSparkMaxRecordsPerBatch;
  private TextVar wSparkMinReadTimeMillis;
  private TextVar wSparkReadTimePercentage;
  private TextVar wSparkBundleSize;
  private ComboVar wSparkStorageLevel;

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
    wGcpInitialNumberOfWorkers.addSelectionListener( selAdapter );
    wGcpMaximumNumberOfWorkers.addSelectionListener( selAdapter );
    wGcpStreaming.addSelectionListener( selAdapter );
    wGcpAutoScalingAlgorithm.addSelectionListener( selAdapter );
    wGcpWorkerMachineType.addSelectionListener( selAdapter );
    wGcpWorkerDiskType.addSelectionListener( selAdapter );
    wGcpDiskSizeGb.addSelectionListener( selAdapter );
    wGcpRegion.addSelectionListener( selAdapter );
    wGcpZone.addSelectionListener( selAdapter );
    wSparkMaster.addSelectionListener( selAdapter );
    wSparkDeployFolder.addSelectionListener( selAdapter );
    wSparkBatchIntervalMillis.addSelectionListener( selAdapter );
    wSparkCheckpointDir.addSelectionListener( selAdapter );
    wSparkCheckpointDurationMillis.addSelectionListener( selAdapter );
    wSparkMaxRecordsPerBatch.addSelectionListener( selAdapter );
    wSparkMinReadTimeMillis.addSelectionListener( selAdapter );
    wSparkReadTimePercentage.addSelectionListener( selAdapter );
    wSparkBundleSize.addSelectionListener( selAdapter );
    wSparkStorageLevel.addSelectionListener( selAdapter );

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

    // The name of the Beam Job Configuration
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
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
    fdlDescription.left = new FormAttachment( 0, -margin ); // First one in the left top corner
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

    // Runner
    //
    Label wlRunner = new Label( shell, SWT.RIGHT );
    props.setLook( wlRunner );
    wlRunner.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Runner.Label" ) );
    FormData fdlRunner = new FormData();
    fdlRunner.top = new FormAttachment( lastControl, margin);
    fdlRunner.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlRunner.right = new FormAttachment( middle, -margin );
    wlRunner.setLayoutData( fdlRunner );
    wRunner = new ComboVar( space, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRunner.setItems( RunnerType.getNames() );
    props.setLook( wRunner );
    FormData fdRunner = new FormData();
    fdRunner.top = new FormAttachment( wlRunner, 0, SWT.CENTER );
    fdRunner.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdRunner.right = new FormAttachment( 95, 0 );
    wRunner.setLayoutData( fdRunner );
    lastControl = wRunner;


    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.top = new FormAttachment( lastControl, margin*2 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin*2 );
    wTabFolder.setLayoutData( fdTabFolder );


    addGeneralTab();
    addParametersTab();
    addDataflowTab();
    addSparkTab();

    wTabFolder.setSelection( 0 );

  }

  private void addGeneralTab() {
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab .setText( "General" );

    wGeneralSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wGeneralSComp.setLayout( new FillLayout() );

    wGeneralComp = new Composite( wGeneralSComp, SWT.NO_BACKGROUND );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 10;
    generalLayout.marginHeight = 10;
    wGeneralComp.setLayout( generalLayout );

    // UserAgent
    //
    Label wlUserAgent = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlUserAgent );
    wlUserAgent.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.UserAgent.Label" ) );
    FormData fdlUserAgent = new FormData();
    fdlUserAgent.top = new FormAttachment( 0, 0 );
    fdlUserAgent.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlUserAgent.right = new FormAttachment( middle, -margin );
    wlUserAgent.setLayoutData( fdlUserAgent );
    wUserAgent = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserAgent );
    FormData fdUserAgent = new FormData();
    fdUserAgent.top = new FormAttachment( wlUserAgent, 0, SWT.CENTER );
    fdUserAgent.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdUserAgent.right = new FormAttachment( 95, 0 );
    wUserAgent.setLayoutData( fdUserAgent );
    Control lastControl = wUserAgent;

    // TempLocation
    //
    Label wlTempLocation = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlTempLocation );
    wlTempLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.TempLocation.Label" ) );
    FormData fdlTempLocation = new FormData();
    fdlTempLocation.top = new FormAttachment( lastControl, margin );
    fdlTempLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
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
    fdlPluginsToStage.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlPluginsToStage.right = new FormAttachment( middle, -margin );
    wlPluginsToStage.setLayoutData( fdlPluginsToStage );
    wPluginsToStage = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPluginsToStage );
    FormData fdPluginsToStage = new FormData();
    fdPluginsToStage.top = new FormAttachment( wlPluginsToStage, 0, SWT.CENTER );
    fdPluginsToStage.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPluginsToStage.right = new FormAttachment( 95, 0 );
    wPluginsToStage.setLayoutData( fdPluginsToStage );
    lastControl = wPluginsToStage;
    
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

    wParametersComp = new Composite( wTabFolder, SWT.NO_BACKGROUND );
    props.setLook( wParametersComp );
    wParametersComp.setLayout( new FormLayout(  ) );

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
    fdParametersComp.right = new FormAttachment( 100, 0 );
    fdParametersComp.top = new FormAttachment( 0, 0);
    fdParametersComp.bottom = new FormAttachment( 100, 0 );
    wParametersComp.setLayoutData( fdParametersComp );

    wParametersTab.setControl( wParametersComp );
  }

  private void addDataflowTab() {
    wDataflowTab = new CTabItem( wTabFolder, SWT.NONE );
    wDataflowTab .setText( "Google Cloud Platform Dataflow" );

    wDataflowSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wDataflowSComp.setLayout( new FillLayout() );

    wDataflowComp = new Composite( wDataflowSComp, SWT.NONE );
    props.setLook( wDataflowComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wDataflowComp.setLayout( fileLayout );

    // Project ID
    //
    Label wlGcpProjectId = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpProjectId );
    wlGcpProjectId.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpProjectId.Label" ) );
    FormData fdlGcpProjectId = new FormData();
    fdlGcpProjectId.top = new FormAttachment( 0, 0 );
    fdlGcpProjectId.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpProjectId.right = new FormAttachment( middle, -margin );
    wlGcpProjectId.setLayoutData( fdlGcpProjectId );
    wGcpProjectId = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpProjectId );
    FormData fdGcpProjectId = new FormData();
    fdGcpProjectId.top = new FormAttachment( wlGcpProjectId, 0, SWT.CENTER );
    fdGcpProjectId.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpProjectId.right = new FormAttachment( 95, 0 );
    wGcpProjectId.setLayoutData( fdGcpProjectId );
    Control lastControl = wGcpProjectId;

    // App name
    //
    Label wlGcpAppName = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpAppName );
    wlGcpAppName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpAppName.Label" ) );
    FormData fdlGcpAppName = new FormData();
    fdlGcpAppName.top = new FormAttachment( lastControl, margin );
    fdlGcpAppName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpAppName.right = new FormAttachment( middle, -margin );
    wlGcpAppName.setLayoutData( fdlGcpAppName );
    wGcpAppName = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpAppName );
    FormData fdGcpAppName = new FormData();
    fdGcpAppName.top = new FormAttachment( wlGcpAppName, 0, SWT.CENTER );
    fdGcpAppName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpAppName.right = new FormAttachment( 95, 0 );
    wGcpAppName.setLayoutData( fdGcpAppName );
    lastControl = wGcpAppName;
    
    // Staging location
    //
    Label wlGcpStagingLocation = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpStagingLocation );
    wlGcpStagingLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpStagingLocation.Label" ) );
    FormData fdlGcpStagingLocation = new FormData();
    fdlGcpStagingLocation.top = new FormAttachment( lastControl, margin );
    fdlGcpStagingLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpStagingLocation.right = new FormAttachment( middle, -margin );
    wlGcpStagingLocation.setLayoutData( fdlGcpStagingLocation );
    wGcpStagingLocation = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpStagingLocation );
    FormData fdGcpStagingLocation = new FormData();
    fdGcpStagingLocation.top = new FormAttachment( wlGcpStagingLocation, 0, SWT.CENTER );
    fdGcpStagingLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpStagingLocation.right = new FormAttachment( 95, 0 );
    wGcpStagingLocation.setLayoutData( fdGcpStagingLocation );
    lastControl = wGcpStagingLocation;

    // Initial number of workers
    //
    Label wlInitialNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlInitialNumberOfWorkers );
    wlInitialNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpInitialNumberOfWorkers.Label" ) );
    FormData fdlInitialNumberOfWorkers = new FormData();
    fdlInitialNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlInitialNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlInitialNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlInitialNumberOfWorkers.setLayoutData( fdlInitialNumberOfWorkers );
    wGcpInitialNumberOfWorkers = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpInitialNumberOfWorkers );
    FormData fdInitialNumberOfWorkers = new FormData();
    fdInitialNumberOfWorkers.top = new FormAttachment( wlInitialNumberOfWorkers, 0, SWT.CENTER );
    fdInitialNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdInitialNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wGcpInitialNumberOfWorkers.setLayoutData( fdInitialNumberOfWorkers );
    lastControl = wGcpInitialNumberOfWorkers;

    // Maximum number of workers
    //
    Label wlMaximumNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlMaximumNumberOfWorkers );
    wlMaximumNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpMaximumNumberOfWorkers.Label" ) );
    FormData fdlMaximumNumberOfWorkers = new FormData();
    fdlMaximumNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlMaximumNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlMaximumNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlMaximumNumberOfWorkers.setLayoutData( fdlMaximumNumberOfWorkers );
    wGcpMaximumNumberOfWorkers = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpMaximumNumberOfWorkers );
    FormData fdMaximumNumberOfWorkers = new FormData();
    fdMaximumNumberOfWorkers.top = new FormAttachment( wlMaximumNumberOfWorkers, 0, SWT.CENTER );
    fdMaximumNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdMaximumNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wGcpMaximumNumberOfWorkers.setLayoutData( fdMaximumNumberOfWorkers );
    lastControl = wGcpMaximumNumberOfWorkers;

    // Auto Scaling Algorithm
    //
    Label wlAutoScalingAlgorithm = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlAutoScalingAlgorithm );
    wlAutoScalingAlgorithm.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpAutoScalingAlgorithm.Label" ) );
    FormData fdlAutoScalingAlgorithm = new FormData();
    fdlAutoScalingAlgorithm.top = new FormAttachment( lastControl, margin );
    fdlAutoScalingAlgorithm.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlAutoScalingAlgorithm.right = new FormAttachment( middle, -margin );
    wlAutoScalingAlgorithm.setLayoutData( fdlAutoScalingAlgorithm );
    wGcpAutoScalingAlgorithm = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpAutoScalingAlgorithm );
    FormData fdAutoScalingAlgorithm = new FormData();
    fdAutoScalingAlgorithm.top = new FormAttachment( wlAutoScalingAlgorithm, 0, SWT.CENTER );
    fdAutoScalingAlgorithm.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdAutoScalingAlgorithm.right = new FormAttachment( 95, 0 );
    wGcpAutoScalingAlgorithm.setLayoutData( fdAutoScalingAlgorithm );
    lastControl = wGcpAutoScalingAlgorithm;


    // Worker machine type
    //
    Label wlGcpWorkerMachineType = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpWorkerMachineType );
    wlGcpWorkerMachineType.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpWorkerMachineType.Label" ) );
    FormData fdlGcpWorkerMachineType = new FormData();
    fdlGcpWorkerMachineType.top = new FormAttachment( lastControl, margin );
    fdlGcpWorkerMachineType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpWorkerMachineType.right = new FormAttachment( middle, -margin );
    wlGcpWorkerMachineType.setLayoutData( fdlGcpWorkerMachineType );
    wGcpWorkerMachineType = new ComboVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpWorkerMachineType );
    wGcpWorkerMachineType.setItems( BeamConst.getGcpWorkerMachineTypeDescriptions() );
    FormData fdGcpWorkerMachineType = new FormData();
    fdGcpWorkerMachineType.top = new FormAttachment( wlGcpWorkerMachineType, 0, SWT.CENTER );
    fdGcpWorkerMachineType.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpWorkerMachineType.right = new FormAttachment( 95, 0 );
    wGcpWorkerMachineType.setLayoutData( fdGcpWorkerMachineType );
    lastControl = wGcpWorkerMachineType;

    // Worker disk type
    //
    Label wlGcpWorkerDiskType = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpWorkerDiskType );
    wlGcpWorkerDiskType.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpWorkerDiskType.Label" ) );
    FormData fdlGcpWorkerDiskType = new FormData();
    fdlGcpWorkerDiskType.top = new FormAttachment( lastControl, margin );
    fdlGcpWorkerDiskType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpWorkerDiskType.right = new FormAttachment( middle, -margin );
    wlGcpWorkerDiskType.setLayoutData( fdlGcpWorkerDiskType );
    wGcpWorkerDiskType = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpWorkerDiskType );
    FormData fdGcpWorkerDiskType = new FormData();
    fdGcpWorkerDiskType.top = new FormAttachment( wlGcpWorkerDiskType, 0, SWT.CENTER );
    fdGcpWorkerDiskType.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpWorkerDiskType.right = new FormAttachment( 95, 0 );
    wGcpWorkerDiskType.setLayoutData( fdGcpWorkerDiskType );
    lastControl = wGcpWorkerDiskType;

    // Disk Size in GB
    //
    Label wlGcpDiskSizeGb = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpDiskSizeGb );
    wlGcpDiskSizeGb.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpDiskSizeGb.Label" ) );
    FormData fdlGcpDiskSizeGb = new FormData();
    fdlGcpDiskSizeGb.top = new FormAttachment( lastControl, margin );
    fdlGcpDiskSizeGb.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpDiskSizeGb.right = new FormAttachment( middle, -margin );
    wlGcpDiskSizeGb.setLayoutData( fdlGcpDiskSizeGb );
    wGcpDiskSizeGb = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpDiskSizeGb );
    FormData fdGcpDiskSizeGb = new FormData();
    fdGcpDiskSizeGb.top = new FormAttachment( wlGcpDiskSizeGb, 0, SWT.CENTER );
    fdGcpDiskSizeGb.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpDiskSizeGb.right = new FormAttachment( 95, 0 );
    wGcpDiskSizeGb.setLayoutData( fdGcpDiskSizeGb );
    lastControl = wGcpDiskSizeGb;

    // Region
    //
    Label wlGcpRegion = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpRegion );
    wlGcpRegion.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpRegion.Label" ) );
    FormData fdlGcpRegion = new FormData();
    fdlGcpRegion.top = new FormAttachment( lastControl, margin );
    fdlGcpRegion.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpRegion.right = new FormAttachment( middle, -margin );
    wlGcpRegion.setLayoutData( fdlGcpRegion );
    wGcpRegion = new ComboVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpRegion );
    wGcpRegion.setItems(BeamConst.getGcpRegionDescriptions());
    FormData fdGcpRegion = new FormData();
    fdGcpRegion.top = new FormAttachment( wlGcpRegion, 0, SWT.CENTER );
    fdGcpRegion.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpRegion.right = new FormAttachment( 95, 0 );
    wGcpRegion.setLayoutData( fdGcpRegion );
    lastControl = wGcpRegion;

    // Zone
    //
    Label wlGcpZone = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpZone );
    wlGcpZone.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpZone.Label" ) );
    FormData fdlGcpZone = new FormData();
    fdlGcpZone.top = new FormAttachment( lastControl, margin );
    fdlGcpZone.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpZone.right = new FormAttachment( middle, -margin );
    wlGcpZone.setLayoutData( fdlGcpZone );
    wGcpZone = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpZone );
    FormData fdGcpZone = new FormData();
    fdGcpZone.top = new FormAttachment( wlGcpZone, 0, SWT.CENTER );
    fdGcpZone.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpZone.right = new FormAttachment( 95, 0 );
    wGcpZone.setLayoutData( fdGcpZone );
    lastControl = wGcpZone;

    // Streaming?
    //
    Label wlGcpStreaming = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpStreaming );
    wlGcpStreaming.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpStreaming.Label" ) );
    FormData fdlGcpStreaming = new FormData();
    fdlGcpStreaming.top = new FormAttachment( lastControl, margin );
    fdlGcpStreaming.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpStreaming.right = new FormAttachment( middle, -margin );
    wlGcpStreaming.setLayoutData( fdlGcpStreaming );
    wGcpStreaming = new Button( wDataflowComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wGcpStreaming );
    FormData fdGcpStreaming = new FormData();
    fdGcpStreaming.top = new FormAttachment( wlGcpStreaming, 0, SWT.CENTER );
    fdGcpStreaming.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpStreaming.right = new FormAttachment( 95, 0 );
    wGcpStreaming.setLayoutData( fdGcpStreaming );
    lastControl = wGcpStreaming;

    FormData fdDataflowComp = new FormData();
    fdDataflowComp.left = new FormAttachment( 0, 0 );
    fdDataflowComp.top = new FormAttachment( 0, 0 );
    fdDataflowComp.right = new FormAttachment( 100, 0 );
    fdDataflowComp.bottom = new FormAttachment( 100, 0 );
    wDataflowComp.setLayoutData( fdDataflowComp );

    wDataflowComp.pack();
    Rectangle bounds = wDataflowComp.getBounds();

    wDataflowSComp.setContent( wDataflowComp );
    wDataflowSComp.setExpandHorizontal( true );
    wDataflowSComp.setExpandVertical( true );
    wDataflowSComp.setMinWidth( bounds.width );
    wDataflowSComp.setMinHeight( bounds.height );

    wDataflowTab.setControl( wDataflowSComp );
  }

  private void addSparkTab() {
    wSparkTab = new CTabItem( wTabFolder, SWT.NONE );
    wSparkTab .setText( "Apache Spark" );

    wSparkSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wSparkSComp.setLayout( new FillLayout() );

    wSparkComp = new Composite( wSparkSComp, SWT.NONE );
    props.setLook( wSparkComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wSparkComp.setLayout( fileLayout );

    // Spark master
    //
    Label wlSparkMaster = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMaster );
    wlSparkMaster.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMaster.Label" ) );
    FormData fdlSparkMaster = new FormData();
    fdlSparkMaster.top = new FormAttachment( 0, 0 );
    fdlSparkMaster.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMaster.right = new FormAttachment( middle, -margin );
    wlSparkMaster.setLayoutData( fdlSparkMaster );
    wSparkMaster = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMaster );
    FormData fdSparkMaster = new FormData();
    fdSparkMaster.top = new FormAttachment( wlSparkMaster, 0, SWT.CENTER );
    fdSparkMaster.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMaster.right = new FormAttachment( 95, 0 );
    wSparkMaster.setLayoutData( fdSparkMaster );
    Control lastControl = wSparkMaster;

    // Folder to deploy Spark submit artifacts in
    //
    Label wlSparkDeployFolder = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkDeployFolder );
    wlSparkDeployFolder.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkDeployFolder.Label" ) );
    FormData fdlSparkDeployFolder = new FormData();
    fdlSparkDeployFolder.top = new FormAttachment( lastControl, margin );
    fdlSparkDeployFolder.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkDeployFolder.right = new FormAttachment( middle, -margin );
    wlSparkDeployFolder.setLayoutData( fdlSparkDeployFolder );
    wSparkDeployFolder = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkDeployFolder );
    FormData fdSparkDeployFolder = new FormData();
    fdSparkDeployFolder.top = new FormAttachment( wlSparkDeployFolder, 0, SWT.CENTER );
    fdSparkDeployFolder.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkDeployFolder.right = new FormAttachment( 95, 0 );
    wSparkDeployFolder.setLayoutData( fdSparkDeployFolder );
    lastControl = wSparkDeployFolder;

    // Spark batch interval in ms
    //
    Label wlSparkBatchIntervalMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkBatchIntervalMillis );
    wlSparkBatchIntervalMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkBatchIntervalMillis.Label" ) );
    FormData fdlSparkBatchIntervalMillis = new FormData();
    fdlSparkBatchIntervalMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkBatchIntervalMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkBatchIntervalMillis.right = new FormAttachment( middle, -margin );
    wlSparkBatchIntervalMillis.setLayoutData( fdlSparkBatchIntervalMillis );
    wSparkBatchIntervalMillis = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkBatchIntervalMillis );
    FormData fdSparkBatchIntervalMillis = new FormData();
    fdSparkBatchIntervalMillis.top = new FormAttachment( wlSparkBatchIntervalMillis, 0, SWT.CENTER );
    fdSparkBatchIntervalMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkBatchIntervalMillis.right = new FormAttachment( 95, 0 );
    wSparkBatchIntervalMillis.setLayoutData( fdSparkBatchIntervalMillis );
    lastControl = wSparkBatchIntervalMillis;

    // Spark checkpoint directory
    //
    Label wlSparkCheckpointDir = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkCheckpointDir );
    wlSparkCheckpointDir.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkCheckpointDir.Label" ) );
    FormData fdlSparkCheckpointDir = new FormData();
    fdlSparkCheckpointDir.top = new FormAttachment( lastControl, margin );
    fdlSparkCheckpointDir.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkCheckpointDir.right = new FormAttachment( middle, -margin );
    wlSparkCheckpointDir.setLayoutData( fdlSparkCheckpointDir );
    wSparkCheckpointDir = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkCheckpointDir );
    FormData fdSparkCheckpointDir = new FormData();
    fdSparkCheckpointDir.top = new FormAttachment( wlSparkCheckpointDir, 0, SWT.CENTER );
    fdSparkCheckpointDir.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkCheckpointDir.right = new FormAttachment( 95, 0 );
    wSparkCheckpointDir.setLayoutData( fdSparkCheckpointDir );
    lastControl = wSparkCheckpointDir;

    // Spark checkpoint duration ms
    //
    Label wlSparkCheckpointDurationMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkCheckpointDurationMillis );
    wlSparkCheckpointDurationMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkCheckpointDurationMillis.Label" ) );
    FormData fdlSparkCheckpointDurationMillis = new FormData();
    fdlSparkCheckpointDurationMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkCheckpointDurationMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkCheckpointDurationMillis.right = new FormAttachment( middle, -margin );
    wlSparkCheckpointDurationMillis.setLayoutData( fdlSparkCheckpointDurationMillis );
    wSparkCheckpointDurationMillis = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkCheckpointDurationMillis );
    FormData fdSparkCheckpointDurationMillis = new FormData();
    fdSparkCheckpointDurationMillis.top = new FormAttachment( wlSparkCheckpointDurationMillis, 0, SWT.CENTER );
    fdSparkCheckpointDurationMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkCheckpointDurationMillis.right = new FormAttachment( 95, 0 );
    wSparkCheckpointDurationMillis.setLayoutData( fdSparkCheckpointDurationMillis );
    lastControl = wSparkCheckpointDurationMillis;

    // Spark : enable spark metrics sink
    //
    Label wlsparkEnableSparkMetricSinks = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlsparkEnableSparkMetricSinks );
    wlsparkEnableSparkMetricSinks.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.sparkEnableSparkMetricSinks.Label" ) );
    FormData fdlsparkEnableSparkMetricSinks = new FormData();
    fdlsparkEnableSparkMetricSinks.top = new FormAttachment( lastControl, margin );
    fdlsparkEnableSparkMetricSinks.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlsparkEnableSparkMetricSinks.right = new FormAttachment( middle, -margin );
    wlsparkEnableSparkMetricSinks.setLayoutData( fdlsparkEnableSparkMetricSinks );
    wsparkEnableSparkMetricSinks = new Button( wSparkComp, SWT.CHECK| SWT.LEFT | SWT.BORDER );
    props.setLook( wsparkEnableSparkMetricSinks );
    FormData fdsparkEnableSparkMetricSinks = new FormData();
    fdsparkEnableSparkMetricSinks.top = new FormAttachment( wlsparkEnableSparkMetricSinks, 0, SWT.CENTER );
    fdsparkEnableSparkMetricSinks.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdsparkEnableSparkMetricSinks.right = new FormAttachment( 95, 0 );
    wsparkEnableSparkMetricSinks.setLayoutData( fdsparkEnableSparkMetricSinks );
    lastControl = wsparkEnableSparkMetricSinks;

    // Spark: Max records per batch
    //
    Label wlSparkMaxRecordsPerBatch = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMaxRecordsPerBatch );
    wlSparkMaxRecordsPerBatch.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMaxRecordsPerBatch.Label" ) );
    FormData fdlSparkMaxRecordsPerBatch = new FormData();
    fdlSparkMaxRecordsPerBatch.top = new FormAttachment( lastControl, margin );
    fdlSparkMaxRecordsPerBatch.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMaxRecordsPerBatch.right = new FormAttachment( middle, -margin );
    wlSparkMaxRecordsPerBatch.setLayoutData( fdlSparkMaxRecordsPerBatch );
    wSparkMaxRecordsPerBatch = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMaxRecordsPerBatch );
    FormData fdSparkMaxRecordsPerBatch = new FormData();
    fdSparkMaxRecordsPerBatch.top = new FormAttachment( wlSparkMaxRecordsPerBatch, 0, SWT.CENTER );
    fdSparkMaxRecordsPerBatch.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMaxRecordsPerBatch.right = new FormAttachment( 95, 0 );
    wSparkMaxRecordsPerBatch.setLayoutData( fdSparkMaxRecordsPerBatch );
    lastControl = wSparkMaxRecordsPerBatch;


    // Spark: minimum read time in ms
    //
    Label wlSparkMinReadTimeMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMinReadTimeMillis );
    wlSparkMinReadTimeMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMinReadTimeMillis.Label" ) );
    FormData fdlSparkMinReadTimeMillis = new FormData();
    fdlSparkMinReadTimeMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkMinReadTimeMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMinReadTimeMillis.right = new FormAttachment( middle, -margin );
    wlSparkMinReadTimeMillis.setLayoutData( fdlSparkMinReadTimeMillis );
    wSparkMinReadTimeMillis = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMinReadTimeMillis );
    FormData fdSparkMinReadTimeMillis = new FormData();
    fdSparkMinReadTimeMillis.top = new FormAttachment( wlSparkMinReadTimeMillis, 0, SWT.CENTER );
    fdSparkMinReadTimeMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMinReadTimeMillis.right = new FormAttachment( 95, 0 );
    wSparkMinReadTimeMillis.setLayoutData( fdSparkMinReadTimeMillis );
    lastControl = wSparkMinReadTimeMillis;

    // Spark read time %
    //
    Label wlSparkReadTimePercentage = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkReadTimePercentage );
    wlSparkReadTimePercentage.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkReadTimePercentage.Label" ) );
    FormData fdlSparkReadTimePercentage = new FormData();
    fdlSparkReadTimePercentage.top = new FormAttachment( lastControl, margin );
    fdlSparkReadTimePercentage.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkReadTimePercentage.right = new FormAttachment( middle, -margin );
    wlSparkReadTimePercentage.setLayoutData( fdlSparkReadTimePercentage );
    wSparkReadTimePercentage = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkReadTimePercentage );
    FormData fdSparkReadTimePercentage = new FormData();
    fdSparkReadTimePercentage.top = new FormAttachment( wlSparkReadTimePercentage, 0, SWT.CENTER );
    fdSparkReadTimePercentage.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkReadTimePercentage.right = new FormAttachment( 95, 0 );
    wSparkReadTimePercentage.setLayoutData( fdSparkReadTimePercentage );
    lastControl = wSparkReadTimePercentage;

    // Bundle size
    //
    Label wlSparkBundleSize = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkBundleSize );
    wlSparkBundleSize.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkBundleSize.Label" ) );
    FormData fdlSparkBundleSize = new FormData();
    fdlSparkBundleSize.top = new FormAttachment( lastControl, margin );
    fdlSparkBundleSize.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkBundleSize.right = new FormAttachment( middle, -margin );
    wlSparkBundleSize.setLayoutData( fdlSparkBundleSize );
    wSparkBundleSize = new TextVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkBundleSize );
    FormData fdSparkBundleSize = new FormData();
    fdSparkBundleSize.top = new FormAttachment( wlSparkBundleSize, 0, SWT.CENTER );
    fdSparkBundleSize.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkBundleSize.right = new FormAttachment( 95, 0 );
    wSparkBundleSize.setLayoutData( fdSparkBundleSize );
    lastControl = wSparkBundleSize;

    // Storage level
    //
    Label wlSparkStorageLevel = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkStorageLevel );
    wlSparkStorageLevel.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkStorageLevel.Label" ) );
    FormData fdlSparkStorageLevel = new FormData();
    fdlSparkStorageLevel.top = new FormAttachment( lastControl, margin );
    fdlSparkStorageLevel.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkStorageLevel.right = new FormAttachment( middle, -margin );
    wlSparkStorageLevel.setLayoutData( fdlSparkStorageLevel );
    wSparkStorageLevel = new ComboVar( space, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkStorageLevel );
    wSparkStorageLevel.setItems( new String[] { "MEMORY_ONLY", "MEMORY_ONLY_SER", "MEMORY_AND_DISK" } );
    FormData fdSparkStorageLevel = new FormData();
    fdSparkStorageLevel.top = new FormAttachment( wlSparkStorageLevel, 0, SWT.CENTER );
    fdSparkStorageLevel.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkStorageLevel.right = new FormAttachment( 95, 0 );
    wSparkStorageLevel.setLayoutData( fdSparkStorageLevel );
    lastControl = wSparkStorageLevel;


    FormData fdSparkComp = new FormData();
    fdSparkComp.left = new FormAttachment( 0, 0 );
    fdSparkComp.top = new FormAttachment( 0, 0 );
    fdSparkComp.right = new FormAttachment( 100, 0 );
    fdSparkComp.bottom = new FormAttachment( 100, 0 );
    wSparkComp.setLayoutData( fdSparkComp );

    wSparkComp.pack();
    Rectangle bounds = wSparkComp.getBounds();

    wSparkSComp.setContent( wSparkComp );
    wSparkSComp.setExpandHorizontal( true );
    wSparkSComp.setExpandVertical( true );
    wSparkSComp.setMinWidth( bounds.width );
    wSparkSComp.setMinHeight( bounds.height );

    wSparkTab.setControl( wSparkSComp );
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
    wPluginsToStage.setText( Const.NVL(config.getPluginsToStage(), "") );

    // GCP
    /*
     */
    wGcpProjectId.setText( Const.NVL(config.getGcpProjectId(), "") );
    wGcpAppName.setText( Const.NVL(config.getGcpAppName(), "") );
    wGcpStagingLocation.setText( Const.NVL(config.getGcpStagingLocation(), "") );
    String workerCode = config.getGcpWorkerMachineType();
    String workerDescription = "";
    if (StringUtils.isNotEmpty( workerCode )) {
      int index = Const.indexOfString( workerCode, BeamConst.getGcpWorkerMachineTypeCodes() );
      if (index<0) {
        workerDescription = workerCode; // variable, manually entered in general
      } else {
        workerDescription = BeamConst.getGcpWorkerMachineTypeDescriptions()[index];
      }
    }
    wGcpWorkerMachineType.setText(workerDescription);
    wGcpWorkerDiskType.setText(Const.NVL(config.getGcpWorkerDiskType(), ""));
    wGcpDiskSizeGb.setText(Const.NVL(config.getGcpDiskSizeGb(), ""));
    wGcpInitialNumberOfWorkers.setText( Const.NVL(config.getGcpInitialNumberOfWorkers(), "") );
    wGcpMaximumNumberOfWorkers.setText( Const.NVL(config.getGcpMaximumNumberOfWokers(), "") );
    wGcpAutoScalingAlgorithm.setText(Const.NVL(config.getGcpAutoScalingAlgorithm(), "") );
    wGcpStreaming.setSelection( config.isGcpStreaming() );
    String regionCode = config.getGcpRegion();
    String regionDescription = "";
    if (StringUtils.isNotEmpty( regionCode )) {
      int index = Const.indexOfString( regionCode, BeamConst.getGcpRegionCodes() );
      if (index<0) {
        regionDescription = regionCode; // variable, manually entered in general
      } else {
        regionDescription = BeamConst.getGcpRegionDescriptions()[index];
      }
    }
    wGcpRegion.setText(regionDescription);
    wGcpZone.setText(Const.NVL(config.getGcpZone(), ""));

    // Spark
    wSparkMaster.setText(Const.NVL(config.getSparkMaster(), ""));
    wSparkDeployFolder.setText(Const.NVL(config.getSparkDeployFolder(), ""));
    wSparkBatchIntervalMillis.setText(Const.NVL(config.getSparkBatchIntervalMillis(), ""));
    wSparkCheckpointDir.setText(Const.NVL(config.getSparkCheckpointDir(), ""));
    wSparkCheckpointDurationMillis.setText(Const.NVL(config.getSparkCheckpointDurationMillis(), ""));
    wsparkEnableSparkMetricSinks.setSelection(config.isSparkEnableSparkMetricSinks());
    wSparkMaxRecordsPerBatch.setText(Const.NVL(config.getSparkMaxRecordsPerBatch(), ""));
    wSparkMinReadTimeMillis.setText(Const.NVL(config.getSparkMinReadTimeMillis(), ""));
    wSparkReadTimePercentage.setText(Const.NVL(config.getSparkReadTimePercentage(), ""));
    wSparkBundleSize.setText(Const.NVL(config.getSparkBundleSize(), ""));
    wSparkStorageLevel.setText(Const.NVL(config.getSparkStorageLevel(), ""));

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
    cfg.setGcpInitialNumberOfWorkers( wGcpInitialNumberOfWorkers.getText() );
    cfg.setGcpMaximumNumberOfWokers( wGcpMaximumNumberOfWorkers.getText() );
    cfg.setGcpStreaming( wGcpStreaming.getSelection() );
    cfg.setGcpAutoScalingAlgorithm( wGcpAutoScalingAlgorithm.getText() );

    String workerMachineDesciption = wGcpWorkerMachineType.getText();
    String workerMachineCode = null;
    if (StringUtils.isNotEmpty( workerMachineDesciption )) {
      int index = Const.indexOfString( workerMachineDesciption, BeamConst.getGcpWorkerMachineTypeDescriptions() );
      if (index<0) {
        workerMachineCode = workerMachineDesciption; // Variable or manually entered
      } else {
        workerMachineCode = BeamConst.getGcpWorkerMachineTypeCodes()[index];
      }
    }
    cfg.setGcpWorkerMachineType( workerMachineCode );

    cfg.setGcpWorkerDiskType( wGcpWorkerDiskType.getText() );
    cfg.setGcpDiskSizeGb( wGcpDiskSizeGb.getText() );
    cfg.setGcpZone( wGcpZone.getText() );

    String regionDesciption = wGcpRegion.getText();
    String regionCode = null;
    if (StringUtils.isNotEmpty( regionDesciption )) {
      int index = Const.indexOfString( regionDesciption, BeamConst.getGcpRegionDescriptions() );
      if (index<0) {
        regionCode = regionDesciption; // Variable or manually entered
      } else {
        regionCode = BeamConst.getGcpRegionCodes()[index];
      }
    }
    cfg.setGcpRegion( regionCode );

    cfg.setSparkMaster( wSparkMaster.getText() );
    cfg.setSparkDeployFolder( wSparkDeployFolder.getText() );
    cfg.setSparkBatchIntervalMillis( wSparkBatchIntervalMillis.getText() );
    cfg.setSparkCheckpointDir( wSparkCheckpointDir.getText() );
    cfg.setSparkCheckpointDurationMillis( wSparkCheckpointDurationMillis.getText() );
    cfg.setSparkEnableSparkMetricSinks( wsparkEnableSparkMetricSinks.getSelection() );
    cfg.setSparkMaxRecordsPerBatch( wSparkMaxRecordsPerBatch.getText() );
    cfg.setSparkMinReadTimeMillis( wSparkMinReadTimeMillis.getText() );
    cfg.setSparkReadTimePercentage( wSparkReadTimePercentage.getText() );
    cfg.setSparkBundleSize( wSparkBundleSize.getText() );
    cfg.setSparkStorageLevel( wSparkStorageLevel.getText() );

    cfg.getParameters().clear();
    for (int i=0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      cfg.getParameters().add(new JobParameter( item.getText(1), item.getText(2) ));
    }

  }
}
