package org.kettle.beam.pipeline.fatjar;

import org.apache.commons.io.IOUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class FatJarBuilder {

  private String targetJarFile;
  private List<String> jarFiles;

  private transient Map<String, String> fileContentMap;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
  }

  public FatJarBuilder( String targetJarFile, List<String> jarFiles ) {
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

          if (merge) {
            String fileContent = IOUtils.toString( zipInputStream, "UTF-8" );
            String previousContent = fileContentMap.get( entryName );
            if (previousContent==null) {
              fileContentMap.put(entryName, fileContent);
            } else {
              fileContentMap.put(entryName, previousContent+ Const.CR+fileContent);
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
      for (String entryName : fileContentMap.keySet()) {
        System.out.println("Entry merged: "+entryName);
        String fileContent = fileContentMap.get(entryName);
        zipOutputStream.putNextEntry( new ZipEntry( entryName ) );
        zipOutputStream.write( fileContent.getBytes( "UTF-8" ) );
        zipOutputStream.closeEntry();
      }

      zipOutputStream.close();
    } catch ( Exception e ) {
      throw new KettleException( "Unable to build far jar file '" + targetJarFile + "'", e );
    } finally {
      fileContentMap.clear();
    }

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
}
