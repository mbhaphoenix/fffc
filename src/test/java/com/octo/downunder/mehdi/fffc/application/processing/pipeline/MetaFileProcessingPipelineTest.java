package com.octo.downunder.mehdi.fffc.application.processing.pipeline;

import com.octo.downunder.mehdi.fffc.application.processing.stage.MetaColumnsToEnrichedMetaColumnsStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.MetaFilePathToMetaColumsStage;
import com.octo.downunder.mehdi.fffc.application.processing.stage.Stage;
import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import com.octo.downunder.mehdi.fffc.domain.MetaColumn;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.*;

public class MetaFileProcessingPipelineTest {

   private SparkSession spark;

   private Stage<List<MetaColumn>, List<EnrichedMetaColumn>> stage;

   private Pipeline<String, List<EnrichedMetaColumn>> pipeline;

   @Before
   public void setUp() throws Exception {
      spark = SparkSession.builder().master("local[1]").getOrCreate();
      pipeline = new MetaFileProcessingPipeline(spark);
   }

   @Test
   public void testExecuteWithValidMataFile() throws Exception {
      List<EnrichedMetaColumn> enrichedMetaColumns = pipeline.execute("src/test/resources/meta-files/valid-metadata.csv");
      assertThat(enrichedMetaColumns.size()).isEqualTo(3);
      EnrichedMetaColumn secondEnrichedMetaColumn = enrichedMetaColumns.get(1);
      assertThat(secondEnrichedMetaColumn.getName()).isEqualTo("First name");
      assertThat(secondEnrichedMetaColumn.getType()).isEqualTo("string");
      assertThat(secondEnrichedMetaColumn.getStart()).isEqualTo(10);
      assertThat(secondEnrichedMetaColumn.getEnd()).isEqualTo(25);
   }

   @Test
   public void testExecuteWithInvalidMetaFile() throws Exception {
      assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> pipeline.execute("src/test/resources/meta-files/invalid-type-metadata.csv"))
            .withMessageContaining("Column type must be one of [string, date, numeric]. Found: not-string-nor-date-nor-numeric");

   }

   @After
   public void tearDown() throws Exception {
      spark.close();
   }


}