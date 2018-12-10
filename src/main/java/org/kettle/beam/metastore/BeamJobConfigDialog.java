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

  private ScrolledComposite wGeneralSComp;
  private ScrolledComposite wDataflowSComp;

  private Composite wGeneralComp;
  private Composite wParametersComp;
  private Composite wDataflowComp;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private ComboVar wRunner;
  private TextVar wUserAgent;
  private TextVar wTempLocation;
  private TextVar wPluginsToStage;
  private TextVar wInitialNumberOfWorkers;
  private TextVar wMaximumNumberOfWorkers;
  private Button wStreaming;
  private TextVar wAutoScalingAlgorithm;

  // GCP settings

  private TextVar wGcpProjectId;
  private TextVar wGcpAppName;
  private TextVar wGcpStagingLocation;
  private ComboVar wGcpWorkerMachineType;
  private TextVar wGcpWorkerDiskType;
  private TextVar wGcpDiskSizeGb;
  private TextVar wGcpRegion;
  private ComboVar wGcpZone;

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
    wInitialNumberOfWorkers.addSelectionListener( selAdapter );
    wMaximumNumberOfWorkers.addSelectionListener( selAdapter );
    wStreaming.addSelectionListener( selAdapter );
    wAutoScalingAlgorithm.addSelectionListener( selAdapter );
    wGcpWorkerMachineType.addSelectionListener( selAdapter );
    wGcpWorkerDiskType.addSelectionListener( selAdapter );
    wGcpDiskSizeGb.addSelectionListener( selAdapter );
    wGcpRegion.addSelectionListener( selAdapter );
    wGcpZone.addSelectionListener( selAdapter );


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

    // Initial number of workers
    //
    Label wlInitialNumberOfWorkers = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlInitialNumberOfWorkers );
    wlInitialNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.InitialNumberOfWorkers.Label" ) );
    FormData fdlInitialNumberOfWorkers = new FormData();
    fdlInitialNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlInitialNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlInitialNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlInitialNumberOfWorkers.setLayoutData( fdlInitialNumberOfWorkers );
    wInitialNumberOfWorkers = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInitialNumberOfWorkers );
    FormData fdInitialNumberOfWorkers = new FormData();
    fdInitialNumberOfWorkers.top = new FormAttachment( wlInitialNumberOfWorkers, 0, SWT.CENTER );
    fdInitialNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdInitialNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wInitialNumberOfWorkers.setLayoutData( fdInitialNumberOfWorkers );
    lastControl = wInitialNumberOfWorkers;

    // Maximum number of workers
    //
    Label wlMaximumNumberOfWorkers = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlMaximumNumberOfWorkers );
    wlMaximumNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.MaximumNumberOfWorkers.Label" ) );
    FormData fdlMaximumNumberOfWorkers = new FormData();
    fdlMaximumNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlMaximumNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlMaximumNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlMaximumNumberOfWorkers.setLayoutData( fdlMaximumNumberOfWorkers );
    wMaximumNumberOfWorkers = new TextVar( space, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaximumNumberOfWorkers );
    FormData fdMaximumNumberOfWorkers = new FormData();
    fdMaximumNumberOfWorkers.top = new FormAttachment( wlMaximumNumberOfWorkers, 0, SWT.CENTER );
    fdMaximumNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdMaximumNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wMaximumNumberOfWorkers.setLayoutData( fdMaximumNumberOfWorkers );
    lastControl = wMaximumNumberOfWorkers;

    // Streaming?
    //
    Label wlStreaming = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlStreaming );
    wlStreaming.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Streaming.Label" ) );
    FormData fdlStreaming = new FormData();
    fdlStreaming.top = new FormAttachment( lastControl, margin );
    fdlStreaming.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlStreaming.right = new FormAttachment( middle, -margin );
    wlStreaming.setLayoutData( fdlStreaming );
    wStreaming = new Button( wGeneralComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wStreaming );
    FormData fdStreaming = new FormData();
    fdStreaming.top = new FormAttachment( wlStreaming, 0, SWT.CENTER );
    fdStreaming.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdStreaming.right = new FormAttachment( 95, 0 );
    wStreaming.setLayoutData( fdStreaming );
    lastControl = wStreaming;


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
    wDataflowTab .setText( "GCP Dataflow" );

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

    // Auto Scaling Algorithm
    //
    Label wlAutoScalingAlgorithm = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlAutoScalingAlgorithm );
    wlAutoScalingAlgorithm.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.AutoScalingAlgorithm.Label" ) );
    FormData fdlAutoScalingAlgorithm = new FormData();
    fdlAutoScalingAlgorithm.top = new FormAttachment( lastControl, margin );
    fdlAutoScalingAlgorithm.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlAutoScalingAlgorithm.right = new FormAttachment( middle, -margin );
    wlAutoScalingAlgorithm.setLayoutData( fdlAutoScalingAlgorithm );
    wAutoScalingAlgorithm = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAutoScalingAlgorithm );
    FormData fdAutoScalingAlgorithm = new FormData();
    fdAutoScalingAlgorithm.top = new FormAttachment( wlAutoScalingAlgorithm, 0, SWT.CENTER );
    fdAutoScalingAlgorithm.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdAutoScalingAlgorithm.right = new FormAttachment( 95, 0 );
    wAutoScalingAlgorithm.setLayoutData( fdAutoScalingAlgorithm );
    lastControl = wAutoScalingAlgorithm;


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
    wGcpWorkerMachineType.setItems( BeamConst.getGcpWorkerMachineTypeCodes() );
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
    wGcpRegion = new TextVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpRegion );
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
    wGcpZone = new ComboVar( space, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpZone );
    wGcpZone.setItems(BeamConst.getGcpRegionCodes());
    FormData fdGcpZone = new FormData();
    fdGcpZone.top = new FormAttachment( wlGcpZone, 0, SWT.CENTER );
    fdGcpZone.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpZone.right = new FormAttachment( 95, 0 );
    wGcpZone.setLayoutData( fdGcpZone );
    lastControl = wGcpZone;


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
    wInitialNumberOfWorkers.setText( Const.NVL(config.getInitialNumberOfWorkers(), "") );
    wMaximumNumberOfWorkers.setText( Const.NVL(config.getMaximumNumberOfWokers(), "") );
    wStreaming.setSelection( config.isStreaming() );
    wAutoScalingAlgorithm.setText(Const.NVL(config.getAutoScalingAlgorithm(), "") );

    // GCP
    wGcpProjectId.setText( Const.NVL(config.getGcpProjectId(), "") );
    wGcpAppName.setText( Const.NVL(config.getGcpAppName(), "") );
    wGcpStagingLocation.setText( Const.NVL(config.getGcpStagingLocation(), "") );
    wPluginsToStage.setText( Const.NVL(config.getPluginsToStage(), "") );
    wGcpWorkerMachineType.setText(Const.NVL(config.getGcpWorkerMachineType(), ""));
    wGcpWorkerDiskType.setText(Const.NVL(config.getGcpWorkerDiskType(), ""));
    wGcpDiskSizeGb.setText(Const.NVL(config.getGcpDiskSizeGb(), ""));
    wGcpRegion.setText(Const.NVL(config.getGcpRegion(), ""));
    wGcpZone.setText(Const.NVL(config.getGcpZone(), ""));

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
    cfg.setInitialNumberOfWorkers( wInitialNumberOfWorkers.getText() );
    cfg.setMaximumNumberOfWokers( wMaximumNumberOfWorkers.getText() );
    cfg.setStreaming( wStreaming.getSelection() );
    cfg.setAutoScalingAlgorithm( wAutoScalingAlgorithm.getText() );
    cfg.setGcpWorkerMachineType( wGcpWorkerMachineType.getText() );
    cfg.setGcpWorkerDiskType( wGcpWorkerDiskType.getText() );
    cfg.setGcpDiskSizeGb( wGcpDiskSizeGb.getText() );
    cfg.setGcpZone( wGcpZone.getText() );
    cfg.setGcpRegion( wGcpRegion.getText() );

    cfg.getParameters().clear();
    for (int i=0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      cfg.getParameters().add(new JobParameter( item.getText(1), item.getText(2) ));
    }

  }
}
