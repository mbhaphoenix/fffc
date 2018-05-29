package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.domain.EnrichedMetaColumn;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FixedLinesDatasetToRowsDatasetStageTest {

   @Test
   public void testExecuteWithInvalidFixedLine() {

      List<EnrichedMetaColumn> enrichedMetaColumns = new ArrayList<>();
      EnrichedMetaColumn numericEnrichedMetaColumn = new EnrichedMetaColumn();
      numericEnrichedMetaColumn.setName("firstname");
      numericEnrichedMetaColumn.setType("numeric");
      numericEnrichedMetaColumn.setStart(0);
      numericEnrichedMetaColumn.setEnd(5);
      enrichedMetaColumns.add(numericEnrichedMetaColumn);

      EnrichedMetaColumn dateEnrichedMetaColumn = new EnrichedMetaColumn();
      dateEnrichedMetaColumn.setName("birthdate");
      dateEnrichedMetaColumn.setType("date");
      dateEnrichedMetaColumn.setStart(6);
      dateEnrichedMetaColumn.setEnd(16);
      enrichedMetaColumns.add(dateEnrichedMetaColumn);


      assertThatExceptionOfType(ParseException.class)
            .isThrownBy(() -> FixedLinesDatasetToRowsDatasetStage.buildRowFromFixedLine(" 3.b5 2018-05-25", enrichedMetaColumns))
            .withMessage("Error while parsing fixedLine ' 3.b5 2018-05-25' because the trimmed token 3.b5 is not a valid numeric");
   }
}