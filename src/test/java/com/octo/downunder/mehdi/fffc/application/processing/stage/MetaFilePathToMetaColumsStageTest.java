package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.domain.MetaColumn;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MetaFilePathToMetaColumsStageTest {


   private SparkSession spark;

   private Stage<String, List<MetaColumn>> stage;

   @Before
   public void setUp() throws Exception {
      spark = SparkSession.builder().master("local[1]").getOrCreate();
      stage = new MetaFilePathToMetaColumsStage(spark);
   }

   @Test
   public void testExecuteWithVaidMataFile() throws Exception {
      List<MetaColumn> metaColumns = stage.execute("src/test/resources/meta-files/valid-metadata.csv");
      assertThat(metaColumns.size()).isEqualTo(3);
      MetaColumn fisrtMetaColumn = metaColumns.get(0);
      assertThat(fisrtMetaColumn.getName()).isEqualTo("Birth date");
      assertThat(fisrtMetaColumn.getLength()).isEqualTo(10);
      assertThat(fisrtMetaColumn.getType()).isEqualTo("date");
   }

   @Test
   public void testExecuteWithInvalidMetaFile() throws Exception {
      assertThatExceptionOfType(SparkException.class).isThrownBy(() -> stage.execute("src/test/resources/meta-files/invalid-metadata.csv"))
            .withMessageContaining("Malformed records are detected in record parsing.").withRootCauseExactlyInstanceOf(NumberFormatException.class)
            .withStackTraceContaining("For input string: \"date\"");

   }

   @After
   public void tearDown() throws Exception {
      spark.close();
   }
}