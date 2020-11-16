package com.asuraflink.project.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString
public class UserEventContainer {
    private String userId;
    private List<UserEvent> userEvents=new ArrayList<>();
}
