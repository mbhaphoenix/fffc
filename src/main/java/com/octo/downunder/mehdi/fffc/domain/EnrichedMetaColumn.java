package com.octo.downunder.mehdi.fffc.domain;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
public class EnrichedMetaColumn implements Serializable {

   private String name;

   private String type;

   private int start;

   private int end;
}
