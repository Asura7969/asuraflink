package com.asuraflink.project.hotItem.pojo;

public class UserBehavior {
    public long userId;
    public long itemId;
    //商品类目ID
    public int categoryId;
    //用户行为,包括（"pv", "buy", "cart", "fav"）
    public String behavior;
    //单位秒
    public long timestamp;

    public UserBehavior() {
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
