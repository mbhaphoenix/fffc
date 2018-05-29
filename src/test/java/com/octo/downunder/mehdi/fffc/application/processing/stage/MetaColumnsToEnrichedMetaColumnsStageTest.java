package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import com.octo.downunder.mehdi.fffc.domain.MetaColumn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MetaColumnsToEnrichedMetaColumnsStageTest {

   @Test
   public void testExecute() throws Exception {

      MetaColumn dateColumn = new MetaColumn();
      dateColumn.setName("birth date");
      dateColumn.setType("date");
      dateColumn.setLength(10);

      MetaColumn stringColumn = new MetaColumn();
      stringColumn.setName("name");
      stringColumn.setType("string");
      stringColumn.setLength(20);

      MetaColumn numericColumn = new MetaColumn();
      numericColumn.setName("height");
      numericColumn.setType("numeric");
      numericColumn.setLength(4);

      List<MetaColumn> metaColumns = new ArrayList<>(3);
      metaColumns.add(dateColumn);
      metaColumns.add(stringColumn);
      metaColumns.add(numericColumn);


      List<EnrichedMetaColumn> enrichedMetaColumns = new MetaColumnsToEnrichedMetaColumnsStage().execute(metaColumns);

      assertThat(enrichedMetaColumns.size()).isEqualTo(3);
      assertThat(enrichedMetaColumns.get(1).getName()).isEqualTo("name");
      assertThat(enrichedMetaColumns.get(1).getType()).isEqualTo("string");
      assertThat(enrichedMetaColumns.get(1).getStart()).isEqualTo(10);
      assertThat(enrichedMetaColumns.get(1).getEnd()).isEqualTo(30);
   }
}