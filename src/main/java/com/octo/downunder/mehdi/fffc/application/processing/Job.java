package com.octo.downunder.mehdi.fffc.application.processing;

import com.octo.downunder.mehdi.fffc.application.processing.pipeline.FixedFileProcessingPipeline;
import com.octo.downunder.mehdi.fffc.application.processing.pipeline.MetaFileProcessingPipeline;
import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * A Job is a encapsulation of a set of {@link com.octo.downunder.mehdi.fffc.application.processing.pipeline.Pipeline}s
 * A Job is a singleton providing a guarantee to have a single {@link SparkSession} across the application
 */
public class Job {

   private static Job INSTANCE;

   private static SparkSession SPARK;

   /**
    * @return Construct and/or return a singleton instance of Job after building a single {@link SparkSession}
    */
   public static Job getInstance() {
      if (INSTANCE == null){
         INSTANCE = new Job();
         SPARK = SparkSession
               .builder()
               .master("local[*]")
               .appName("Fixed File Format converter")
               .getOrCreate();
      }
      return INSTANCE;
   }

   /**
    * Run a set of {@link com.octo.downunder.mehdi.fffc.application.processing.pipeline.Pipeline}s given the different file paths passed as arguments
    *
    * @param metaFilePath
    * @param fixedFilePath
    * @param csvOutputDirectoryPath
    */
   public void run(final String metaFilePath, final String fixedFilePath, final String csvOutputDirectoryPath) {

      List<EnrichedMetaColumn> enrichedMetaColumns = new MetaFileProcessingPipeline(SPARK).execute(metaFilePath);

      new FixedFileProcessingPipeline(SPARK, enrichedMetaColumns, csvOutputDirectoryPath).execute(fixedFilePath);

   }

}
