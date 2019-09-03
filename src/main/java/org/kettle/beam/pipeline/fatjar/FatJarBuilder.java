package org.kettle.beam.pipeline.fatjar;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.widgets.Event;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.pipeline.TransMetaPipelineConverter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.extension.ExtensionPointPluginType;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class FatJarBuilder {

  private String targetJarFile;
  private List<String> jarFiles;
  private String extraStepPluginClasses;
  private String extraXpPluginClasses;

  private transient Map<String, String> fileContentMap;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
    extraStepPluginClasses = null;
    extraXpPluginClasses = null;
  }

  public FatJarBuilder( String targetJarFile, List<String> jarFiles ) {
    this();
    this.targetJarFile = targetJarFile;
    this.jarFiles = jarFiles;
  }

  public void buildTargetJar() throws KettleException {

    fileContentMap = new HashMap<>();

    try {
      byte[] buffer = new byte[ 1024 ];
      ZipOutputStream zipOutputStream = new ZipOutputStream( new FileOutputStream( targetJarFile ) );

      boolean fileSystemWritten = false;
      for ( String jarFile : jarFiles ) {

        ZipInputStream zipInputStream = new ZipInputStream( new FileInputStream( jarFile ) );
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        while ( zipEntry != null ) {

          boolean skip = false;
          boolean merge = false;

          String entryName = zipEntry.getName();

          if ( entryName.contains( "META-INF/INDEX.LIST" ) ) {
            skip = true;
          }
          if ( entryName.contains( "META-INF/MANIFEST.MF" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".SF" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".DSA" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".RSA" ) ) {
            skip = true;
          }
          if ( entryName.equals(Const.XML_FILE_KETTLE_STEPS)) {
            skip = true;
          }
          if ( entryName.equals(Const.XML_FILE_KETTLE_EXTENSION_POINTS)) {
            skip = true;
          }
          if ( entryName.equals(Const.XML_FILE_KETTLE_JOB_ENTRIES)) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF/services/" ) ) {
            merge = true;
            skip = true;
          }

          if ( !skip ) {
            try {
              zipOutputStream.putNextEntry( new ZipEntry( zipEntry.getName() ) );
            } catch ( ZipException ze ) {
              // Duplicate entry!
              //
              skip = true;
            }
          }

          if ( merge ) {
            String fileContent = IOUtils.toString( zipInputStream, "UTF-8" );
            String previousContent = fileContentMap.get( entryName );
            if ( previousContent == null ) {
              fileContentMap.put( entryName, fileContent );
            } else {
              fileContentMap.put( entryName, previousContent + Const.CR + fileContent );
            }
          } else {
            int len;
            while ( ( len = zipInputStream.read( buffer ) ) > 0 ) {
              if ( !skip ) {
                zipOutputStream.write( buffer, 0, len );
              }
            }
          }

          zipInputStream.closeEntry();

          if ( !skip ) {
            zipOutputStream.closeEntry();
          }

          zipEntry = zipInputStream.getNextEntry();

        }
        zipInputStream.close();
      }

      // Add the META-INF/services files...
      //
      for ( String entryName : fileContentMap.keySet() ) {
        System.out.println( "Entry merged: " + entryName );
        String fileContent = fileContentMap.get( entryName );
        zipOutputStream.putNextEntry( new ZipEntry( entryName ) );
        zipOutputStream.write( fileContent.getBytes( "UTF-8" ) );
        zipOutputStream.closeEntry();
      }

      // Add Steps, job entries and extension point plugins in XML files
      //
      addPluginsXmlFile(zipOutputStream, Const.XML_FILE_KETTLE_STEPS, "steps", "step", StepPluginType.class, StepMetaInterface.class, extraStepPluginClasses);
      addPluginsXmlFile(zipOutputStream, Const.XML_FILE_KETTLE_JOB_ENTRIES, "job-entries", "job-entry", JobEntryPluginType.class, JobEntryInterface.class, null);
      addPluginsXmlFile(zipOutputStream, Const.XML_FILE_KETTLE_EXTENSION_POINTS, "extension-points", "extension-point", ExtensionPointPluginType.class, ExtensionPointInterface.class, extraXpPluginClasses);

      zipOutputStream.close();
    } catch ( Exception e ) {
      throw new KettleException( "Unable to build far jar file '" + targetJarFile + "'", e );
    } finally {
      fileContentMap.clear();
    }

  }

  private void addPluginsXmlFile( ZipOutputStream zipOutputStream, String filename, String mainTag, String pluginTag, Class<? extends PluginTypeInterface> pluginTypeClass, Class<?> mainPluginClass, String extraClasses ) throws Exception {

    // Write all the internal steps plus the selected classes...
    //
    StringBuilder xml = new StringBuilder( );
    xml.append( XMLHandler.openTag( mainTag ) );
    PluginRegistry registry = PluginRegistry.getInstance();
    List<PluginInterface> plugins = registry.getPlugins( pluginTypeClass );
    for (PluginInterface plugin : plugins) {
      if (plugin.isNativePlugin()) {
        addPluginToXml(xml, pluginTag, plugin, mainPluginClass);
      }
    }
    if ( StringUtils.isNotEmpty(extraClasses)) {
      for (String extraPluginClass : extraClasses.split( "," )) {
        PluginInterface plugin = findPluginWithMainClass(extraPluginClass, pluginTypeClass, mainPluginClass);
        if (plugin!=null) {
          addPluginToXml( xml, pluginTag, plugin, mainPluginClass );
        }
      }
    }

    xml.append( XMLHandler.closeTag( mainTag ) );

    zipOutputStream.putNextEntry( new ZipEntry( filename ) );
    zipOutputStream.write( xml.toString().getBytes( "UTF-8" ) );
    zipOutputStream.closeEntry();
  }

  private PluginInterface findPluginWithMainClass( String extraPluginClass, Class<? extends PluginTypeInterface> pluginTypeClass, Class<?> mainClass) {
    List<PluginInterface> plugins = PluginRegistry.getInstance().getPlugins( pluginTypeClass );
    for (PluginInterface plugin : plugins) {
      String check = plugin.getClassMap().get( mainClass );
      if (check!=null && check.equals(extraPluginClass)) {
        return plugin;
      }
    }
    return null;

  }

  private void addPluginToXml( StringBuilder xml, String pluginTag, PluginInterface plugin, Class<?> mainClass ) {
    xml.append("<").append(pluginTag).append(" id=\"");
    xml.append(plugin.getIds()[0]);
    xml.append("\">");
    xml.append(XMLHandler.addTagValue( "description", plugin.getName() ));
    xml.append(XMLHandler.addTagValue( "tooltip", plugin.getDescription() ));
    xml.append(XMLHandler.addTagValue( "classname", plugin.getClassMap().get( mainClass ) ));
    xml.append(XMLHandler.addTagValue( "category", plugin.getCategory() ));
    xml.append(XMLHandler.addTagValue( "iconfile", plugin.getImageFile() ));
    xml.append(XMLHandler.addTagValue( "documentation_url", plugin.getDocumentationUrl() ));
    xml.append(XMLHandler.addTagValue( "cases_url", plugin.getCasesUrl() ));
    xml.append(XMLHandler.addTagValue( "forum_url", plugin.getForumUrl() ));
    xml.append(XMLHandler.closeTag( pluginTag ));
  }

  public static String findPluginClasses( String pluginClassName, String pluginsToInclude) throws KettleException {
    String plugins = pluginsToInclude;

    if ( StringUtils.isEmpty( plugins ) ) {
      plugins = "kettle-beam";
    } else {
      plugins += ",kettle-beam";
    }

    Set<String> classes = new HashSet<>();
    String[] pluginFolders = plugins.split( "," );
    for ( String pluginFolder : pluginFolders ) {
      try {
        List<String> stepClasses = TransMetaPipelineConverter.findAnnotatedClasses( pluginFolder, pluginClassName );
        for ( String stepClass : stepClasses ) {
          classes.add( stepClass );
        }
      } catch ( Exception e ) {
        throw new KettleException( "Error find plugin classes of annotation type '" + pluginClassName + "' in folder '" + pluginFolder, e );
      }
    }

    // OK, we now have all the classes...
    // Let's sort by name and add them in the dialog comma separated...
    //
    List<String> classesList = new ArrayList<>();
    classesList.addAll( classes );
    Collections.sort( classesList );

    StringBuffer all = new StringBuffer();
    for ( String pluginClass : classesList ) {
      if ( all.length() > 0 ) {
        all.append( "," );
      }
      all.append( pluginClass );
    }

    return all.toString();
  }


  /**
   * Gets targetJarFile
   *
   * @return value of targetJarFile
   */
  public String getTargetJarFile() {
    return targetJarFile;
  }

  /**
   * @param targetJarFile The targetJarFile to set
   */
  public void setTargetJarFile( String targetJarFile ) {
    this.targetJarFile = targetJarFile;
  }

  /**
   * Gets jarFiles
   *
   * @return value of jarFiles
   */
  public List<String> getJarFiles() {
    return jarFiles;
  }

  /**
   * @param jarFiles The jarFiles to set
   */
  public void setJarFiles( List<String> jarFiles ) {
    this.jarFiles = jarFiles;
  }

  /**
   * Gets extraStepPluginClasses
   *
   * @return value of extraStepPluginClasses
   */
  public String getExtraStepPluginClasses() {
    return extraStepPluginClasses;
  }

  /**
   * @param extraStepPluginClasses The extraStepPluginClasses to set
   */
  public void setExtraStepPluginClasses( String extraStepPluginClasses ) {
    this.extraStepPluginClasses = extraStepPluginClasses;
  }

  /**
   * Gets extraXpPluginClasses
   *
   * @return value of extraXpPluginClasses
   */
  public String getExtraXpPluginClasses() {
    return extraXpPluginClasses;
  }

  /**
   * @param extraXpPluginClasses The extraXpPluginClasses to set
   */
  public void setExtraXpPluginClasses( String extraXpPluginClasses ) {
    this.extraXpPluginClasses = extraXpPluginClasses;
  }

  /**
   * Gets fileContentMap
   *
   * @return value of fileContentMap
   */
  public Map<String, String> getFileContentMap() {
    return fileContentMap;
  }

  /**
   * @param fileContentMap The fileContentMap to set
   */
  public void setFileContentMap( Map<String, String> fileContentMap ) {
    this.fileContentMap = fileContentMap;
  }
}
