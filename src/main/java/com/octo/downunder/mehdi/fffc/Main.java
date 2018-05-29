package com.octo.downunder.mehdi.fffc;

import com.octo.downunder.mehdi.fffc.application.processing.Job;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * The application entrypoint and the last catcher of Exceptions thrown by the driver (Spark driver)
 */
public class Main {

   private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

   public static void main(String... args) {

      //If the list of args is not correct, no need to go further. Let's stop before creating an expensive Spark Context
      if(args.length != 3) {
         final String errorMEssage = "You should provide exactly 3 args: the metadata file path, the fixed file path and the output directory path";
         LOGGER.error(errorMEssage);
         throw new IllegalArgumentException(errorMEssage);
      }

      try {
         Job.getInstance().run(args[0], args[1], args[2]);
      } catch (Exception e) {
         //Catching any excpetion and formatting a comprehensible error message to log
         Throwable rootCause = ExceptionUtils.getRootCause(e);
         if(rootCause != null) {
            LOGGER.error("The application has been interrupted because of an exception with cause [{}] and error message [{}]. " +
                        "The root cause is [{}] and has error message [{}]",
                  e.getClass(), e.getMessage(), rootCause.getClass(), rootCause.getMessage());
         } else {
            LOGGER.error("The application has been interrupted because of an exception with cause [{}] and error message [{}].",
                  e.getClass(), e.getMessage());
         }
         deleteEmptyGeneratedOutputDirectory(args[2]);

         //So that if you ran the application from Command line you have an explicit message on stderr about the failure
         System.err.println("The application has been interrupted because of an exception. Check log files for more details.");
         System.exit(1);
      }
      LOGGER.info("Successful termination. CSV file(s) are generated under {}", args[2]);

      //So that if you ran the application from Command line you have an explicit message on stdout about the success
      System.out.println("Successful termination. CSV file(s) are generated under " + args[2]);
      System.exit(0);

   }

   //The output directory can be created even with application failure. So we delete on failure, if it is empty to allow the user to use the same output directory
   private static void deleteEmptyGeneratedOutputDirectory(final String outputDirectoryPath) {
      File outputDirectory = new File(outputDirectoryPath);
      if(outputDirectory.isDirectory() && outputDirectory.list().length ==0) {
         FileUtils.deleteQuietly(outputDirectory);
      }
   }
}
