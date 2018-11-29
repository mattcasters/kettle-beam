package org.kettle.beam.core;

import org.kettle.beam.steps.beaminput.BeamInputMeta;
import org.kettle.beam.steps.beamoutput.BeamOutputMeta;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.ArrayList;
import java.util.HashMap;

public class BeamKettle {
  public static final void init() throws KettleException {
    PluginRegistry registry = PluginRegistry.getInstance();
    synchronized ( registry ) {
      if (!KettleEnvironment.isInitialized()) {

        // Load Kettle base plugins
        //
        KettleEnvironment.init();

        PluginTypeInterface pluginType = registry.getPluginType( StepPluginType.class );
        pluginType.handlePluginAnnotation( BeamInputMeta.class, BeamInputMeta.class.getAnnotation( Step.class ), new ArrayList<>( ), false, null );
        pluginType.handlePluginAnnotation( BeamOutputMeta.class, BeamOutputMeta.class.getAnnotation( Step.class ), new ArrayList<>( ), false, null );
      }

      XMLHandlerCache.getInstance();
    }
  }


  public static PluginInterface getStepPluginForClass( Class<? extends StepMetaInterface> metaClass ) {
    Step stepAnnotation = metaClass.getAnnotation( Step.class );

    return new Plugin(
      new String[] {stepAnnotation.id()},
      StepPluginType.class,
      metaClass,
      stepAnnotation.categoryDescription(),
      stepAnnotation.name(),
      stepAnnotation.description(),
      stepAnnotation.image(),
      stepAnnotation.isSeparateClassLoaderNeeded(),
      false,
      new HashMap<>(  ),
      new ArrayList<>(  ),
      stepAnnotation.documentationUrl(),
      null
    );
  }
}
