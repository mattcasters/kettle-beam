package org.kettle.beam.core.shared;

import org.pentaho.di.core.exception.KettleException;

public enum AggregationType {
  SUM, AVERAGE, COUNT_ALL, MIN, MAX, FIRST_INCL_NULL, LAST_INCL_NULL, FIRST, LAST,
  ;

  public static final AggregationType getTypeFromName( String name) throws KettleException {
    for ( AggregationType type : values()) {
      if (name.equals( type.name() )) {
        return type;
      }
    }
    throw new KettleException( "Aggregation type '"+name+"' is not recognized or supported" );
  }
}
