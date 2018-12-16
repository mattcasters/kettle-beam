package org.kettle.beam.pipeline;

import org.apache.beam.sdk.PipelineResult;

public interface BeamMetricsUpdatedListener {

  void beamMetricsUpdated( PipelineResult pipelineResult );
}
