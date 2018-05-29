package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import com.octo.downunder.mehdi.fffc.domain.MetaColumn;

import java.util.ArrayList;
import java.util.List;

import static com.octo.downunder.mehdi.fffc.application.util.MetaColumnsUtils.checkMetaColumnTypeValidity;

/**
 * A {@link Stage} that given a list of {@link MetaColumn} will return the corresponding list of {@link EnrichedMetaColumn}
 * An {@link EnrichedMetaColumn} will contain extra and necessary information to extract fields from the fixed lines
 */
public class MetaColumnsToEnrichedMetaColumnsStage implements Stage<List<MetaColumn>, List<EnrichedMetaColumn>> {

   private int currentIndex = 0;

   @Override
   public List<EnrichedMetaColumn> execute(final List<MetaColumn> metaColumns) {
      List<EnrichedMetaColumn> enrichedMetaColumnsList = new ArrayList<>(metaColumns.size());
      for (final MetaColumn metaColumn : metaColumns) {
         String metaColumnType = metaColumn.getType();

         checkMetaColumnTypeValidity(metaColumnType);

         EnrichedMetaColumn enrichedMetaColumn = new EnrichedMetaColumn();
         enrichedMetaColumn.setName(metaColumn.getName());
         enrichedMetaColumn.setType(metaColumnType);
         enrichedMetaColumn.setStart(currentIndex);
         enrichedMetaColumn.setEnd(currentIndex + metaColumn.getLength());

         currentIndex = enrichedMetaColumn.getEnd();

         enrichedMetaColumnsList.add(enrichedMetaColumn);
      }
      return enrichedMetaColumnsList;
   }



}
