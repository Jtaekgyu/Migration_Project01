package com.example.migration.controller.dto.request;

import lombok.Getter;

import java.util.List;

@Getter
public class MigrationReqDto {
    private OracleInfoReqDto oraReqDto;
    private List<String> oracleTableList;
    private List<ObjectCheckReqDto> objChkReqDtoList;
}
