package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcessRowInTransFn extends DoFn<KettleRow, KettleRow> {

    private RowMetaInterface rowMeta;
    private RowProducer rowProducer;
    private SingleThreadedTransExecutor executor;
    private List<Object[]> outputRows;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( ProcessRowInTransFn.class );
  private static final Counter numErrors = Metrics.counter( "main", "ParseErrors" );


  public ProcessRowInTransFn( RowMetaInterface rowMeta, RowProducer rowProducer, SingleThreadedTransExecutor executor, List<Object[]> outputRows ) {
      this.rowMeta = rowMeta;
      this.rowProducer = rowProducer;
      this.executor = executor;
      this.outputRows = outputRows;
    }

    @ProcessElement
    public void processElement(@Element KettleRow inputRow, OutputReceiver<KettleRow> receiver) {

      try {
        // Clear the output first
        //
        outputRows.clear();

        // Pass a row to the input buffer
        //
        rowProducer.putRow( rowMeta, inputRow.getRow() );

        // Force the row through all steps
        //
        executor.oneIteration();

        // Pass all the rows in the output
        //
        for (Object[] outputRow : outputRows) {
          receiver.output(new KettleRow( outputRow ));
        }

        // All done

      } catch(Exception e) {
        numErrors.inc();
        try {
          LOG.error( "Error processing data for row '" + rowMeta.getString( inputRow.getRow() ) + "'", e );
        } catch(Exception e2) {
          LOG.error( "Error processing data", e );
        }
      }

    }
  }