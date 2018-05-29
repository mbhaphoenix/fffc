package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.application.util.MetaColumnsUtils;
import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

import static com.octo.downunder.mehdi.fffc.application.util.TokenProcessingUtils.getProcessedToken;
import static com.octo.downunder.mehdi.fffc.application.util.MetaColumnsUtils.getOutputRowEncoder;

/**
 * A {@link Stage} that given a {@link Dataset} of the lines of the fixedFile will return the corresponding {@link Dataset} of {@link Row}s after parsing and verifying the input lines
 */
@AllArgsConstructor
public class FixedLinesDatasetToRowsDatasetStage implements Stage<Dataset<String>, Dataset<Row>> {

   private static final Logger LOGGER = LoggerFactory.getLogger(FixedLinesDatasetToRowsDatasetStage.class);

   private List<EnrichedMetaColumn> enrichedMetaColumns;

   @Override
   public Dataset<Row> execute(final Dataset<String> textFileDataset) {
      return textFileDataset.map((MapFunction<String, Row>) fixedLine ->
            buildRowFromFixedLine(fixedLine, enrichedMetaColumns), getOutputRowEncoder(enrichedMetaColumns));
   }

   /**
    * Convert a fixedLine to a {@link Row} after processing, parsing, validating it otherwise throws a {@link ParseException} with proper error message
    * @param fixedLine a line in the fixedFile
    * @param enrichedMetaColumns the list of meta columns enriched with start and end indexes of the different tokens of the fixedLine
    * @return a {@link Row} representing a valid processed fixedLine
    * @throws ParseException whenever a token in not conform to the requirements
    */
   static Row buildRowFromFixedLine(String fixedLine, List<EnrichedMetaColumn> enrichedMetaColumns) throws ParseException {
      List<Object> rowAttributes = new LinkedList<>();
      for (EnrichedMetaColumn enrichedMetaColumn : enrichedMetaColumns) {
         String token = fixedLine.substring(enrichedMetaColumn.getStart(), enrichedMetaColumn.getEnd()).trim();
         Object processedToken = null;
         try {
            processedToken = getProcessedToken(enrichedMetaColumn.getType(), token);
         } catch (ParseException pe) {
            //refine the error message
            String errorMessage = "Error while parsing fixedLine '" + fixedLine + "' because the trimmed token " + pe.getMessage();
            LOGGER.error(errorMessage);
            throw new ParseException(errorMessage, 0);
         }
         rowAttributes.add(processedToken);
      }
      return RowFactory.create(rowAttributes.toArray());
   }


}
