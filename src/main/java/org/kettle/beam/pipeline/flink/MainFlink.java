  package org.kettle.beam.pipeline.flink;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.pipeline.KettleBeamPipelineExecutor;
import org.kettle.beam.pipeline.main.MainBeam;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainFlink {
  public static void main( String[] args ) {
    MainBeam.mainMethod(args, "Apache Flink");
  }
}
