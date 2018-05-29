package com.octo.downunder.mehdi.fffc.application.processing.stage;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link Stage} that given a fixedFilePath will return the corresponding {@link Dataset} of the lines of the fixedFile
 */
@AllArgsConstructor
public class FixedFilePathToFixedLinesDatasetStage implements Stage<String, Dataset<String>> {

   private SparkSession spark;

   @Override
   public Dataset<String> execute(final String fixedFilePath) {

      return spark.read().textFile(fixedFilePath);

   }
}
