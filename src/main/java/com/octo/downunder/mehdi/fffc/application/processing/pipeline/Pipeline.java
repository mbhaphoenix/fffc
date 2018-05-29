package com.octo.downunder.mehdi.fffc.application.processing.pipeline;

import com.octo.downunder.mehdi.fffc.application.processing.stage.Stage;
import java.io.Serializable;

/**
 * A Pipeline is a succession of {@link Stage}s
 * @param <I> type of the input parameter of {@link #execute(Object)}
 * @param <O> type of the return type of {@link #execute(Object)}
 */
@FunctionalInterface
public interface Pipeline<I,O> extends Serializable {

   O execute(I i);

}
