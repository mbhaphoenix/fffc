package com.octo.downunder.mehdi.fffc;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MainTest {

   @Test
   public void testMainThrowsIllegalArgumentException() throws Exception {

      assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(()-> Main.main())
            .withMessage("You should provide exactly 3 args: the metadata file path, the fixed file path and the output directory path")
            .withNoCause();
      assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(()-> Main.main("one"));
      assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(()-> Main.main("one", "two"));
      assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(()-> Main.main("one", "two", "three", "four"));


   }

}