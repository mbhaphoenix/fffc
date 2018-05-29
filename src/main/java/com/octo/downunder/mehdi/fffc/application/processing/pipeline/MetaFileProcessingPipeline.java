package com.octo.downunder.mehdi.fffc.application.processing.pipeline;

import com.octo.downunder.mehdi.fffc.application.processing.stage.MetaColumnsToEnrichedMetaColumnsStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.MetaFilePathToMetaColumsStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.Stage;
import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import com.octo.downunder.mehdi.fffc.domain.MetaColumn;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * A {@link Pipeline} that given, the metafilePath will return an enriched meta columns list
 */
@AllArgsConstructor
public class MetaFileProcessingPipeline implements Pipeline<String, List<EnrichedMetaColumn>> {

   private SparkSession spark;

   @Override
   public List<EnrichedMetaColumn> execute(final String metaFilePath) {

      final Stage<String, List<MetaColumn>> metaFilePathToMetaColumsStage = new MetaFilePathToMetaColumsStage(spark);

      // I created the following stage to make its code more unit testable : without relying on Spark session
      final Stage<List<MetaColumn>, List<EnrichedMetaColumn>> metaColumnsToEnrichedMetaColumnsStage = new MetaColumnsToEnrichedMetaColumnsStage();

      //the chaining of the stages of the pipeline
      return metaFilePathToMetaColumsStage.andThen(metaColumnsToEnrichedMetaColumnsStage).execute(metaFilePath);
   }
}
