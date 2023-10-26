package com.example.demo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserData {
    private String name;
    private Integer amount;
    private String time;
}
