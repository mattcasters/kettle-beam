package org.kettle.beam.perspective;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPerspectiveImageProvider;
import org.pentaho.di.ui.spoon.SpoonPerspectiveListener;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTabpanel;
import org.pentaho.ui.xul.containers.XulTabbox;
import org.pentaho.ui.xul.containers.XulTabpanels;
import org.pentaho.ui.xul.containers.XulTabs;
import org.pentaho.ui.xul.containers.XulVbox;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.pentaho.ui.xul.swt.tags.SwtDeck;
import org.pentaho.ui.xul.swt.tags.SwtTab;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class BeamPerspective extends AbstractXulEventHandler implements SpoonPerspective, SpoonPerspectiveImageProvider, XulEventHandler {
  private static Class<?> PKG = BeamPerspective.class;
  private ResourceBundle resourceBundle = new XulSpoonResourceBundle( PKG );

  final String PERSPECTIVE_ID = "031-beam"; //$NON-NLS-1$
  final String PERSPECTIVE_NAME = "beamPerspective"; //$NON-NLS-1$

  private XulDomContainer container;
  private BeamController controller;
  private BeamMenuController gitSpoonMenuController;
  private XulVbox box;

  private LogChannelInterface logger = LogChannel.GENERAL;

  protected XulRunner runner;

  protected Document document;
  protected XulTabs tabs;
  protected XulTabpanels panels;
  protected XulTabbox tabbox;
  protected List<SpoonPerspectiveListener> listeners = new ArrayList<>();

  BeamPerspective() throws XulException {
    KettleXulLoader loader = new KettleXulLoader();
    loader.registerClassLoader( getClass().getClassLoader() );
    container = loader.loadXul( "beam_perspective.xul", resourceBundle );

    try {

      document = container.getDocumentRoot();
      tabs = (XulTabs) document.getElementById( "tabs" );
      panels = (XulTabpanels) document.getElementById( "tabpanels" );
      tabbox = (XulTabbox) tabs.getParent();

      container.addEventHandler( this );

      addAdminTab();

      /*
       * To make compatible with webSpoon
       * Create a temporary parent for the UI and then call layout().
       * A different parent will be assigned to the UI in SpoonPerspectiveManager.PerspectiveManager.performInit().
       */
      SwtDeck deck = (SwtDeck) Spoon.getInstance().getXulDomContainer().getDocumentRoot().getElementById( "canvas-deck" );
      box = deck.createVBoxCard();
      getUI().setParent( (Composite) box.getManagedObject() );
      getUI().layout();

    } catch ( Exception e ) {
      logger.logError( "Error initializing perspective", e );
    }

  }

  @Override public String getName() {
    return "beamPerspective";
  }

  private void addAdminTab() throws Exception {

    final XulTabAndPanel tabAndPanel = createTab();
    tabAndPanel.tab.setLabel( "Admin" );

    PropsUI props = PropsUI.getInstance();

    final Composite comp = (Composite) tabAndPanel.panel.getManagedObject();
    props.setLook( comp );
    comp.setLayout( new FillLayout() );

    ScrolledComposite scrolledComposite = new ScrolledComposite( comp, SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( scrolledComposite );
    scrolledComposite.setLayout( new FillLayout() );

    final Composite parentComposite = new Composite( scrolledComposite, SWT.NONE );
    props.setLook( parentComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = 10;
    formLayout.marginRight = 10;
    formLayout.marginTop = 10;
    formLayout.marginBottom = 10;
    formLayout.spacing = Const.MARGIN;
    parentComposite.setLayout( formLayout );


    parentComposite.layout( true );
    parentComposite.pack();

    // What's the size:
    Rectangle bounds = parentComposite.getBounds();

    scrolledComposite.setContent( parentComposite );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setMinWidth( bounds.width );
    scrolledComposite.setMinHeight( bounds.height );

    comp.layout();
  }


  /**
   * Gets controller
   *
   * @return value of controller
   */
  public BeamController getController() {
    return this.controller;
  }

  @Override public String getPerspectiveIconPath() {
    return null;
  }

  @Override public String getId() {
    return PERSPECTIVE_ID;
  }

  @Override public Composite getUI() {
    return (Composite) container.getDocumentRoot().getRootElement().getFirstChild().getManagedObject();
  }

  @Override public String getDisplayName( Locale locale ) {
    return BaseMessages.getString( PKG, "Neo4j.Perspective.perspectiveName" );
  }

  @Override public InputStream getPerspectiveIcon() {
    return null;
  }

  @Override public void setActive( boolean active ) {
    for ( SpoonPerspectiveListener listener : listeners ) {
      if ( active ) {
        listener.onActivation();
      } else {
        listener.onDeactication();
      }
    }
  }

  public class XulTabAndPanel {
    public XulTab tab;

    public XulTabpanel panel;

    public XulTabAndPanel( XulTab tab, XulTabpanel panel ) {
      this.tab = tab;
      this.panel = panel;
    }

  }

  public XulTabAndPanel createTab() {

    try {
      XulTab tab = (XulTab) document.createElement( "tab" );
      XulTabpanel panel = (XulTabpanel) document.createElement( "tabpanel" );
      panel.setSpacing( 0 );
      panel.setPadding( 0 );

      tabs.addChild( tab );
      panels.addChild( panel );
      tabbox.setSelectedIndex( panels.getChildNodes().indexOf( panel ) );


      tab.addPropertyChangeListener( new PropertyChangeListener() {
        @Override public void propertyChange( PropertyChangeEvent evt ) {
          LogChannel.GENERAL.logBasic( "Property changed: " + evt.getPropertyName() + ", " + evt.toString() );
        }
      } );

      return new XulTabAndPanel( tab, panel );
    } catch ( XulException e ) {
      e.printStackTrace();
    }
    return null;
  }


  private VariableSpace getVariableSpace() {
    VariableSpace space = (VariableSpace) Spoon.getInstance().getActiveMeta();
    if ( space == null ) {
      // Just use system variables
      //
      space = new Variables();
      space.initializeVariablesFrom( null );
    }
    return space;
  }

  public void setNameForTab( XulTab tab, String name ) {
    String tabName = name;
    List<String> usedNames = new ArrayList<String>();
    for ( XulComponent c : tabs.getChildNodes() ) {
      if ( c != tab ) {
        usedNames.add( ( (SwtTab) c ).getLabel() );
      }
    }
    if ( usedNames.contains( name ) ) {
      int num = 2;
      while ( true ) {
        tabName = name + " (" + num + ")";
        if ( usedNames.contains( tabName ) == false ) {
          break;
        }
        num++;
      }
    }

    tab.setLabel( tabName );
  }

  @Override public List<XulOverlay> getOverlays() {
    return null;
  }

  @Override public List<XulEventHandler> getEventHandlers() {
    return null;
  }

  @Override public void addPerspectiveListener( SpoonPerspectiveListener spoonPerspectiveListener ) {

  }

  @Override public EngineMetaInterface getActiveMeta() {
    return null;
  }

  public boolean onTabClose( final int pos ) throws XulException {
    // Never close the admin tab
    //
    if ( pos == 0 ) {
      return false;
    }
    return true;
  }

}
