package com.octo.downunder.mehdi.fffc.application.processing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.text.ParseException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class JobIT {

   private static final String CSV_OUTPUT_DIRECTORY_PATH = "tmp/output";

   //given-* files are valid files

   @Test
   public void testRunWithValidFiles() throws Exception {
      //Given
      final String metaFilePath = "src/test/resources/meta-files/given-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/given-fff.txt";

      Job job = Job.getInstance();

      //When
      job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH);

      //then
      File csvFile = getGenratedCsvFile();

      List<String> csvLines = FileUtils.readLines(csvFile);
      assertThat(csvLines.get(0)).isEqualTo("Birth date,First name,Last name,Weight");
      assertThat(csvLines.get(1)).isEqualTo("01/01/1970,John,Smith,81.5");
      assertThat(csvLines.get(2)).isEqualTo("31/01/1975,Jane,Doe,61.1");
      assertThat(csvLines.get(3)).isEqualTo("28/11/1988,Bob,Big,102.4");

   }

   @Test
   public void testRunWithEscape() throws Exception {
      //Given
      final String metaFilePath = "src/test/resources/meta-files/given-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/escaped-fff.txt";

      Job job = Job.getInstance();

      //When
      job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH);

      //then
      File csvFile = getGenratedCsvFile();

      List<String> csvLines = FileUtils.readLines(csvFile);
      assertThat(csvLines.get(0)).isEqualTo("Birth date,First name,Last name,Weight");
      assertThat(csvLines.get(1)).isEqualTo("01/01/1970,\"Jo,hn\",Smith,81.5");
      assertThat(csvLines.get(2)).isEqualTo("31/01/1975,Jane,\"Do\\\"e\",61.1");
      assertThat(csvLines.get(3)).isEqualTo("28/11/1988,Bob,Big,102.4");

   }

   @Test
   public void testRunWithInvalidFixedFileDate() throws Exception {
      final String metaFilePath = "src/test/resources/meta-files/given-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/invalid-fff.txt";

      Job job = Job.getInstance();

      assertThatExceptionOfType(SparkException.class).isThrownBy(() -> job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH))
            .withCauseInstanceOf(SparkException.class)
            .withRootCauseExactlyInstanceOf(ParseException.class)
            .withStackTraceContaining("Error while parsing fixedLine '1970-01-01John           Smith          8b1.5' because the trimmed token 8b1.5 is not a valid numeric");
   }

   @Test
   public void testRunWithInvalidMetaFile() throws Exception {
      final String metaFilePath = "src/test/resources/meta-files/job-invalid-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/given-fff.txt";

      Job job = Job.getInstance();

      assertThatExceptionOfType(SparkException.class).isThrownBy(() -> job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH))
            .withCauseInstanceOf(SparkException.class)
            .withRootCauseExactlyInstanceOf(RuntimeException.class)
            .withStackTraceContaining("Malformed CSV record");
   }

   @Test
   public void testRunWithNonexistentMetaFile() throws Exception {
      final String metaFilePath = "src/test/resources/meta-files/nonexistent-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/nonexistent-fff.txt";

      Job job = Job.getInstance();

      assertThatExceptionOfType(AnalysisException.class).isThrownBy(() -> job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH))
            .withMessageStartingWith("Path does not exist: file:")
            .withMessageEndingWith("/fffc/src/test/resources/meta-files/nonexistent-metadata.csv;");
   }

   @Test
   public void testRunWithNonexistentFixedFile() throws Exception {
      final String metaFilePath = "src/test/resources/meta-files/given-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/nonexistent-fff.txt";

      Job job = Job.getInstance();

      assertThatExceptionOfType(AnalysisException.class).isThrownBy(() -> job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH))
            .withMessageStartingWith("Path does not exist: file:")
            .withMessageEndingWith("/fffc/src/test/resources/fixed-files/nonexistent-fff.txt;");
   }

   @Test
   public void testRunWithAnAlreadyExistingOutputPath() throws Exception {
      new File(CSV_OUTPUT_DIRECTORY_PATH).mkdirs();
      final String metaFilePath = "src/test/resources/meta-files/given-metadata.csv";
      final String fixedFilePath = "src/test/resources/fixed-files/given-fff.txt";

      Job job = Job.getInstance();

      assertThatExceptionOfType(AnalysisException.class).isThrownBy(() -> job.run(metaFilePath, fixedFilePath, CSV_OUTPUT_DIRECTORY_PATH))
            .withMessageStartingWith("path file")
            .withMessageEndingWith("output already exists.;");
   }


   private static File getGenratedCsvFile() {
      File csvOutputDirectory = new File(CSV_OUTPUT_DIRECTORY_PATH);
      FileFilter fileFilter = new WildcardFileFilter("part-*.csv");
      return csvOutputDirectory.listFiles(fileFilter)[0];
   }

   @After
   public void tearDown() throws Exception {
      FileUtils.deleteQuietly(new File(CSV_OUTPUT_DIRECTORY_PATH));
   }
}