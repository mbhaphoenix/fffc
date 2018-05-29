package com.octo.downunder.mehdi.fffc.application.util;

import com.octo.downunder.mehdi.fffc.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class TokenProcessingUtils {

   private static final Logger LOGGER = LoggerFactory.getLogger(TokenProcessingUtils.class);

   private static final Pattern PATTERN_WITH_CR_LF = Pattern.compile(".*[\n\r].*", Pattern.DOTALL);

   static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

   /**
    *
    * @param metaColumnType numeric | string | date
    * @param token a trimmed token to process and validate
    * @return valid processed token
    * @throws ParseException whenever an invalid token in encountered
    * @throws IllegalStateException should never be thrown
    */
   public static Object getProcessedToken(String metaColumnType, String token) throws ParseException {
      switch (metaColumnType) {
         case "string":
            return getProcessedStringToken(token);
         case "date":
            return getProcessedDateToken(token);
         case "numeric":
            return getProcessedNumericToken(token);
         //No need to handle the default case because it has been handled while constructing the EnrichedMetaColumn
         //Otherwise an IllegalStateException will be thrown
      }
      String errorMessage = "Fatal error while processing a token. Unacceptable type " + metaColumnType;
      LOGGER.error(errorMessage);
      throw new IllegalStateException(errorMessage);
   }

   //Verify that the token is a valid numeric type
   static String getProcessedNumericToken(String token) throws ParseException {
      //Use of BigDecimal because it preserves precision while converting to String
      try {
         return new BigDecimal(token).toString();
      } catch (NumberFormatException nfe) {
         throw new ParseException(token + " is not a valid numeric", 0);
      }

   }

   //Verify that the token is valid date with "yyyy-MM-dd" format
   static java.sql.Date getProcessedDateToken(String token) throws ParseException {
      try {
         Date parsedDate;
         synchronized (DATE_FORMAT) {
            parsedDate = DATE_FORMAT.parse(token);
         }
         return new java.sql.Date(parsedDate.getTime());
      } catch (Exception p) {
         throw new ParseException(token + " is not a valid date", 0);
      }
   }

   //verifying the token does not contain CR (\r carriage-return) nor LF (\n	newline line feed)
   static String getProcessedStringToken(String token) throws ParseException {
      if (PATTERN_WITH_CR_LF.matcher(token).find()) throw new ParseException(token + " should not contain CR nor LF", 0);
      return token;
   }
}
