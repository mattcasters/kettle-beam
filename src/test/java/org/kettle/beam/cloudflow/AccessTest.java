package org.kettle.beam.cloudflow;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import junit.framework.TestCase;
import org.junit.Test;

public class AccessTest extends TestCase {

  @Test
  public void testAccess() throws Exception {

    System.out.println("GOOGLE_APPLICATION_CREDENTIALS="+System.getenv( "GOOGLE_APPLICATION_CREDENTIALS" ));

    // If you don't specify credentials when constructing the client, the client library will
    // look for credentials via the environment variable GOOGLE_APPLICATION_CREDENTIALS.
    //
    Storage storage = StorageOptions.getDefaultInstance().getService();

    System.out.println("Buckets:");
    Page<Bucket> buckets = storage.list();
    for (Bucket bucket : buckets.iterateAll()) {
      System.out.println(bucket.toString());
    }
  }
}
