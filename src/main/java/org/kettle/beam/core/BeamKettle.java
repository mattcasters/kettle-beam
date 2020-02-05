package org.kettle.beam.core;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointPluginType;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginMainClassType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BeamKettle {

  private static final Logger LOG = LoggerFactory.getLogger( BeamKettle.class );

  public static final boolean isInitialized() {
    return KettleEnvironment.isInitialized();
  }

  public static final void init( List<String> stepPluginClasses, List<String> xpPluginClasses ) throws KettleException {
    PluginRegistry registry = PluginRegistry.getInstance();
    synchronized ( registry ) {

      // Load Kettle base plugins
      //
      KettleEnvironment.init();

      XMLHandlerCache.getInstance();

      // LOG.info( "Registering " + stepPluginClasses.size() + " extra step plugins, and " + xpPluginClasses.size() + " XP plugins" );

      // Register extra classes from the plugins...
      // If they're already in the classpath, this should be fast.
      //
      StepPluginType stepPluginType = (StepPluginType) registry.getPluginType( StepPluginType.class );
      for ( String stepPluginClassName : stepPluginClasses ) {
        try {
          // Only register if it doesn't exist yet.  This is not ideal if we want to replace old steps with bug fixed new ones.
          //
          PluginInterface exists = findPlugin( registry, StepPluginType.class, stepPluginClassName );
          if ( exists == null ) {
            // Class should be in the classpath since we put it there
            //
            Class<?> stepPluginClass = Class.forName( stepPluginClassName );
            Step annotation = stepPluginClass.getAnnotation( Step.class );

            // The plugin class is already in the classpath so we simply call Class.forName() on it.
            //
            // LOG.info( "Registering step plugin class: " + stepPluginClass );
            stepPluginType.handlePluginAnnotation( stepPluginClass, annotation, new ArrayList<String>(), true, null );
          } else {
            LOG.debug( "Plugin " + stepPluginClassName + " is already registered" );
          }
        } catch ( Exception e ) {
          LOG.error( "Error registering step plugin class : " + stepPluginClassName, e );
        }
      }

      ExtensionPointPluginType xpPluginType = (ExtensionPointPluginType) registry.getPluginType( ExtensionPointPluginType.class );
      for ( String xpPluginClassName : xpPluginClasses ) {
        try {
          PluginInterface exists = findPlugin( registry, ExtensionPointPluginType.class, xpPluginClassName );
          // Only register if it doesn't exist yet. This is not ideal if we want to replace old steps with bug fixed new ones.
          //
          if ( exists == null ) {
            // Class should be in the classpath since we put it there
            //
            Class<?> xpPluginClass = Class.forName( xpPluginClassName );
            ExtensionPoint annotation = xpPluginClass.getAnnotation( ExtensionPoint.class );

            // The plugin class is already in the classpath so we simply call Class.forName() on it.
            //
            // LOG.info( "Registering step plugin class: " + xpPluginClass );
            xpPluginType.handlePluginAnnotation( xpPluginClass, annotation, new ArrayList<String>(), true, null );
          } else {
            LOG.debug( "Plugin " + xpPluginClassName + " is already registered" );
          }
        } catch ( Exception e ) {
          LOG.error( "Error registering step plugin class : " + xpPluginClassName, e );

        }
      }
    }
  }

  private static PluginInterface findPlugin( PluginRegistry registry, Class<? extends PluginTypeInterface> pluginTypeClass, String pluginClassName ) {
    PluginMainClassType classType = pluginTypeClass.getAnnotation( PluginMainClassType.class );
    // System.out.println("Found class type : "+classType+" as main plugin class");
    List<PluginInterface> plugins = registry.getPlugins( pluginTypeClass );
    // System.out.println("Found "+plugins.size()+" plugins of type "+pluginTypeClass);
    for ( PluginInterface plugin : plugins ) {
      String mainClassName = plugin.getClassMap().get( classType.value() );
      if ( mainClassName != null && pluginClassName.equals( mainClassName ) ) {
        return plugin;
      }
    }
    return null;
  }


  public static PluginInterface getStepPluginForClass( Class<? extends StepMetaInterface> metaClass ) {
    Step stepAnnotation = metaClass.getAnnotation( Step.class );

    return new Plugin(
      new String[] { stepAnnotation.id() },
      StepPluginType.class,
      metaClass,
      stepAnnotation.categoryDescription(),
      stepAnnotation.name(),
      stepAnnotation.description(),
      stepAnnotation.image(),
      stepAnnotation.isSeparateClassLoaderNeeded(),
      false,
      new HashMap<>(),
      new ArrayList<>(),
      stepAnnotation.documentationUrl(),
      null
    );
  }
}
