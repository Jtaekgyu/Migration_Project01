package com.example.migration.controller.dto.request;

import lombok.Getter;

@Getter
public class ObjectCheckReqDto {
    private Boolean view;
    private Boolean procedure;
    private Boolean function;
    private Boolean sysnonym;
    private Boolean sequence;
    private Boolean mview;
    private Boolean trigger;
}
