package com.octo.downunder.mehdi.fffc.application.processing.stage;

import com.octo.downunder.mehdi.fffc.application.processing.pipeline.Pipeline;

import java.util.Objects;

/**
 * A Stage is a logical unit of work of a {@link Pipeline}
 * A Stage can be chained to another one through the {@link #andThen(Stage)}. This method is the difference with a {@link Pipeline}
 * @param <I> type of the input parameter of {@link Pipeline#execute(Object)}
 * @param <O> type of the return type of {@link Pipeline#execute(Object)}
 */
@FunctionalInterface
public interface Stage<I,O> extends Pipeline<I, O> {

   default <V> Stage<I, V> andThen(Stage<? super O, ? extends V> after) {
      Objects.requireNonNull(after);
      return (I i) -> after.execute(execute(i));
   }
}
