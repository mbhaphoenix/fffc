package com.octo.downunder.mehdi.fffc.application.util;

import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class MetaColumnsUtils {

   private static final Logger LOGGER = LoggerFactory.getLogger(MetaColumnsUtils.class);

   private static final List<String> ACCEPTABLE_TYPES =Arrays.asList("string", "date", "numeric");

   /**
    * Check whether the given token is a one of the valid ones : string | date | numeric otherwise throws an IllegalArgumentException
    * @param metaColumnType
    */
   public static void checkMetaColumnTypeValidity(String metaColumnType) {
      if(!ACCEPTABLE_TYPES.contains(metaColumnType)){
         String errorMessage = "Column type must be one of " + ACCEPTABLE_TYPES + ". Found: " + metaColumnType;
         LOGGER.error(errorMessage);
         throw new IllegalArgumentException(errorMessage);
      }
   }

   /**
    *
    * @param enrichedMetaColumns
    * @return an encoder built from the enrichedMetaColumns list for the {@link Row}s of the corresponding fixed lines
    */
   public static ExpressionEncoder<Row> getOutputRowEncoder(List<EnrichedMetaColumn> enrichedMetaColumns) {
      List<StructField> outputStructFields = new LinkedList<>();
      for (EnrichedMetaColumn enrichedMetaColumn : enrichedMetaColumns) {
         DataType dataType = null;
         switch (enrichedMetaColumn.getType()) {
            case "string":
               dataType = DataTypes.StringType;
               break;
            case "date":
               dataType = DataTypes.DateType;
               break;
            case "numeric":
               dataType = DataTypes.StringType;
               break;
         }
         outputStructFields.add(DataTypes.createStructField(enrichedMetaColumn.getName(), dataType, false));
      }
      return RowEncoder.apply(DataTypes.createStructType(outputStructFields));
   }

}
