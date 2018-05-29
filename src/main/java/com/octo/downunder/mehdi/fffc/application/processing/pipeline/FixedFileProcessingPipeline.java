package com.octo.downunder.mehdi.fffc.application.processing.pipeline;

import com.octo.downunder.mehdi.fffc.application.processing.stage.FixedFilePathToFixedLinesDatasetStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.FixedLinesDatasetToRowsDatasetStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.RowsDatasetToCsvFileStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.Stage;
import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * A {@link Pipeline} that given, the fixedFilePath will generate the corresponding CSV file(s) under the output directory
 */
@AllArgsConstructor
public class FixedFileProcessingPipeline implements Pipeline<String, Void> {

   private SparkSession spark;

   private List<EnrichedMetaColumn> enrichedMetaColumns;

   private String csvOutpuDirectoryPath;

   @Override
   public Void execute(final String fixedFilePath) {

      final Stage<String, Dataset<String>> fixedFilePathToFixedLinesDatasetStage = new FixedFilePathToFixedLinesDatasetStage(spark);

      final Stage<Dataset<String>, Dataset<Row>> fixedLinesDatasetToRowsDatasetStage = new FixedLinesDatasetToRowsDatasetStage(enrichedMetaColumns);

      final Stage<Dataset<Row>, Void> rowsDatasetToCsvFileStage = new RowsDatasetToCsvFileStage(csvOutpuDirectoryPath);

      //the chaining of the stages of the pipeline
      return fixedFilePathToFixedLinesDatasetStage.andThen(fixedLinesDatasetToRowsDatasetStage).andThen(rowsDatasetToCsvFileStage).execute(fixedFilePath);
   }
}
