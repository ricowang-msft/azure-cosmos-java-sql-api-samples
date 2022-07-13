package com.azure.cosmos.examples.common;

public class Food {

  private String id;
  private String description;
  private int version;
  private String foodGroup;

  public Food() {
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getFoodGroup() {
    return foodGroup;
  }

  public void setFoodGroup(String foodGroup) {
    this.foodGroup = foodGroup;
  }

}
