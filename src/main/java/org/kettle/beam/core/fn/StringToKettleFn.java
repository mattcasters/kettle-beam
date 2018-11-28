package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringToKettleFn extends DoFn<String, KettleRow> {

    private FileDefinition fileDefinition;

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger( StringToKettleFn.class );
    private final Counter numParseErrors = Metrics.counter( "main", "ParseErrors" );

    public StringToKettleFn( FileDefinition fileDefinition ) {
      this.fileDefinition = fileDefinition;
    }

    @ProcessElement
    public void processElement( ProcessContext processContext ) {
      String inputString = processContext.element();
      String[] components = inputString.split( fileDefinition.getSeparator(), -1 );

      // TODO: implement enclosure in FileDefinition
      //
      try {

        RowMetaInterface rowMeta = fileDefinition.getRowMeta();
        Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
        int index = 0;
        while ( index < rowMeta.size() && index < components.length ) {
          String sourceString = components[ index ];
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
          ValueMetaInterface stringMeta = new ValueMetaString( "SourceString" );
          row[ index ] = valueMeta.convertDataFromString( sourceString, stringMeta, null, null, ValueMetaInterface.TRIM_TYPE_NONE );

          index++;
        }

        System.out.println("Read row from file : "+rowMeta.getString(row));

        // Pass the row to the process context
        //
        processContext.output( new KettleRow( row ) );

      } catch ( Exception e ) {
        numParseErrors.inc();
        LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
      }
    }

    /**
     * Gets fileDefinition
     *
     * @return value of fileDefinition
     */
    public FileDefinition getFileDefinition() {
      return fileDefinition;
    }

    /**
     * @param fileDefinition The fileDefinition to set
     */
    public void setFileDefinition( FileDefinition fileDefinition ) {
      this.fileDefinition = fileDefinition;
    }

}
