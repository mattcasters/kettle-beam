package org.kettle.beam.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.util.common.ReflectHelpers;

import java.io.File;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

public class BeamConst {

  private static List<String[]> gcpWorkerCodeDescriptions = Arrays.asList(
    new String[] { "n1-standard-1", "Standard machine type with 1 vCPU and 3.75 GB of memory." },
    new String[] { "n1-standard-2", "Standard machine type with 2 vCPUs and 7.5 GB of memory." },
    new String[] { "n1-standard-4", "Standard machine type with 4 vCPUs and 15 GB of memory." },
    new String[] { "n1-standard-8", "Standard machine type with 8 vCPUs and 30 GB of memory." },
    new String[] { "n1-standard-16", "Standard machine type with 16 vCPUs and 60 GB of memory." },
    new String[] { "n1-standard-32", "Standard machine type with 32 vCPUs and 120 GB of memory." },
    new String[] { "n1-standard-64", "Standard machine type with 64 vCPUs and 240 GB of memory." },
    new String[] { "n1-standard-96", "Standard machine type with 96 vCPUs and 360 GB of memory." },
    new String[] { "n1-highmem-2", "High memory machine type with 2 vCPUs and 13 GB of memory." },
    new String[] { "n1-highmem-4", "High memory machine type with 4 vCPUs, and 26 GB of memory." },
    new String[] { "n1-highmem-8", "High memory machine type with 8 vCPUs and 52 GB of memory." },
    new String[] { "n1-highmem-16", "High memory machine type with 16 vCPUs and 104 GB of memory." },
    new String[] { "n1-highmem-32", "High memory machine type with 32 vCPUs and 208 GB of memory." },
    new String[] { "n1-highmem-64", "High memory machine type with 64 vCPUs and 416 GB of memory." },
    new String[] { "n1-highmem-96", "High memory machine type with 96 vCPUs and 624 GB of memory." },
    new String[] { "n1-highcpu-2", "High-CPU machine type with 2 vCPUs and 1.80 GB of memory." },
    new String[] { "n1-highcpu-4", "High-CPU machine type with 4 vCPUs and 3.60 GB of memory." },
    new String[] { "n1-highcpu-8", "High-CPU machine type with 8 vCPUs and 7.20 GB of memory." },
    new String[] { "n1-highcpu-16", "High-CPU machine type with 16 vCPUs and 14.4 GB of memory." },
    new String[] { "n1-highcpu-32", "High-CPU machine type with 32 vCPUs and 28.8 GB of memory." },
    new String[] { "n1-highcpu-64", "High-CPU machine type with 64 vCPUs and 57.6 GB of memory." },
    new String[] { "n1-highcpu-96", "High-CPU machine type with 96 vCPUs and 86.4 GB of memory." },
    new String[] { "f1-micro", "Micro machine type with 0.2 vCPU, 0.60 GB of memory, backed by a shared physical core." },
    new String[] { "g1-small", "Shared-core machine type with 0.5 vCPU, 1.70 GB of memory, backed by a shared physical core." },
    new String[] { "n1-ultramem-40", "Memory-optimized machine type with 40 vCPUs and 961GB of memory." },
    new String[] { "n1-ultramem-80", "Memory-optimized machine type with 80 vCPUs and 1.87 TB of memory." },
    new String[] { "n1-megamem-96", "Memory-optimized machine type with 96 vCPUs and 1.4 TB of memory." },
    new String[] { "n1-ultramem-160", "Memory-optimized machine type with 160 vCPUs and 3.75 TB of memory." }
  );

  public static final String[] getGcpWorkerMachineTypeCodes() {
    String[] codes = new String[gcpWorkerCodeDescriptions.size()];
    for (int i=0;i<codes.length;i++) {
      codes[i] = gcpWorkerCodeDescriptions.get(i)[0];
    }
    return codes;
  }

  public static final String[] getGcpWorkerMachineTypeDescriptions() {
    String[] descriptions = new String[gcpWorkerCodeDescriptions.size()];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = gcpWorkerCodeDescriptions.get(i)[1];
    }
    return descriptions;
  }

  public static final String[] getGcpRegionCodes() {
    return new String[] {
      "us-central1",
      "us-east1",
      "us-west1",
      "europe-west1",
      "asia-east1",
      "asia-northeast1",
    };
  }

  public static void testFilesystems() {

    URI file = new File( "file:///tmp/customers/input/state-data.txt" ).toURI();
    String scheme = file.getScheme();

    FileSystem fileSystem = FileSystems.getFileSystem( file );

    System.out.println("//////////////// local file-system URI scheme : "+scheme);
    System.out.println("//////////////// local file-system FS  scheme : "+fileSystem.provider().getScheme());

    Set<FileSystemRegistrar> registrars = Sets.newTreeSet( ReflectHelpers.ObjectsClassComparator.INSTANCE);

    registrars.addAll( Lists.newArrayList( ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader())));


  }
}
