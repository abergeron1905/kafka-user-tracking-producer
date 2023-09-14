package com.kafka.producer.model;

import com.kafka.producer.enums.UserId;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class User {

    private UserId userId;

    private String userName;

    private Date dateOfBirth;

}
