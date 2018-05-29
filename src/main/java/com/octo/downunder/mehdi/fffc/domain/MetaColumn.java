package com.octo.downunder.mehdi.fffc.domain;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class MetaColumn implements Serializable {

   private String name;

   private Integer length;

   private String type;

}
