package com.octo.downunder.mehdi.fffc.application.processing.stage;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link Stage} that given a {@link Dataset} of valid  {@link Row}s generates the corresponding CSV file(s) under csvOutpuDirectory
 */
@AllArgsConstructor
public class RowsDatasetToCsvFileStage implements Stage<Dataset<Row>, Void> {

   private String csvOutpuDirectoryPath;

   @Override
   public Void execute(final Dataset<Row> rowsDataset) {
      rowsDataset.write()
            //the option to add the header for each CSV file
            .option("header", "true")
            //is the default option to use , as CSV separator
            .option("delimiter", ",")
            //the option to format date attibutes of a row with the given format value, here dd/MM/yyyy
            .option("dateFormat", "dd/MM/yyyy")
            //the option to set " as the quote char
            .option("quote", "\"")
            //the option to quote tokens containing the quote or the separator chars (here is " and ,)
            .option("quoteMode", "MINIMAL")
            .csv(csvOutpuDirectoryPath);
      return null;
   }
}
