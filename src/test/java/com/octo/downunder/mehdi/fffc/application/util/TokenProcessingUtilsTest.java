package com.octo.downunder.mehdi.fffc.application.util;

import org.junit.Test;

import java.text.ParseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import static com.octo.downunder.mehdi.fffc.application.util.TokenProcessingUtils.*;

public class TokenProcessingUtilsTest {

   @Test
   public void testGetProcessedTokenWithValidType() throws Exception {
      assertThat(getProcessedToken("numeric", "2.333")).isEqualTo("2.333");
   }

   @Test
   public void testGetProcessedTokenWithInvalidType() throws Exception {
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> getProcessedToken("invalidTYpe","1.25"))
            .withMessage("Fatal error while processing a token. Unacceptable type invalidTYpe");
   }

   @Test
   public void testGetProcessedNumericTokenWithValidToken() throws Exception {
      assertThat(getProcessedNumericToken("12.2500")).isEqualTo("12.2500");
   }

   @Test
   public void testGetProcessedNumericTokenWithInvalidToken() throws Exception {
      assertThatExceptionOfType(ParseException.class).isThrownBy(() -> getProcessedNumericToken("1b.25"))
            .withMessage("1b.25 is not a valid numeric");
   }

   @Test
   public void getProcessedDateTokenWithValidDateFormat() throws Exception {
      assertThat(getProcessedDateToken("1988-11-28"))
            .isEqualTo(TokenProcessingUtils.DATE_FORMAT.parse("1988-11-28"));
   }

   @Test
   public void getProcessedDateTokenWithInvalidDateFormat() throws Exception {
      assertThatExceptionOfType(ParseException.class).isThrownBy(() -> getProcessedDateToken("2018/05/25"))
            .withMessage("2018/05/25 is not a valid date");
   }

   @Test
   public void getProcessedStringTokenWithValidToken() throws Exception {
      assertThat(getProcessedStringToken("jlfdlksjd-!§è(.2018-05-25"))
            .isEqualTo("jlfdlksjd-!§è(.2018-05-25");
   }

   @Test
   public void getProcessedStringTokenWithInvalidToken() throws Exception {
      assertThatExceptionOfType(ParseException.class).isThrownBy(() -> getProcessedStringToken("string1" + System.lineSeparator() + "123"))
            .withMessage("string1" + System.lineSeparator() + "123 should not contain CR nor LF");

      String theOtherLineSep = (System.lineSeparator().equals("\r")) ? "\n" : "\r";
      assertThatExceptionOfType(ParseException.class).isThrownBy(() -> getProcessedStringToken("string2" + theOtherLineSep + "123"))
            .withMessage("string2" + theOtherLineSep + "123 should not contain CR nor LF");

   }
}