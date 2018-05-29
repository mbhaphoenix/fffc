package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.domain.MetaColumn;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link Stage} that given a metaFilePath will return the list of {@link MetaColumn} corresponding to the lines of the metadata file
 */
@AllArgsConstructor
public class MetaFilePathToMetaColumsStage implements Stage<String, List<MetaColumn>> {

   private SparkSession spark;

   @Override
   public List<MetaColumn> execute(final String metaFilePath) {

      Dataset<MetaColumn> metaColumnDataset = spark.read().option("mode", "FAILFAST").schema(getMetaColumnsSchema()).csv(metaFilePath).as(Encoders.bean(MetaColumn.class));

      return metaColumnDataset.collectAsList();
   }

   //name, length, type
   StructType getMetaColumnsSchema() {
      List<StructField> metadataFields = new LinkedList<>();
      metadataFields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
      metadataFields.add(DataTypes.createStructField("length", DataTypes.IntegerType, false));
      metadataFields.add(DataTypes.createStructField("type", DataTypes.StringType, false));
      return DataTypes.createStructType(metadataFields);
   }
}
