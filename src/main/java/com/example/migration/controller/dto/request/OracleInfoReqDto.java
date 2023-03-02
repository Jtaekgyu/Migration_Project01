package com.example.migration.controller.dto.request;

import lombok.Getter;

@Getter
public class OracleInfoReqDto {
    private String host;
    private String port;
    private String sid;
    private String username;
    private String password;
}
