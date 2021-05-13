package com.unicrawl;

public class Output {
    int num;
    String title;
    String url;
    float score;

    public Output() {
        
    }

    public Output(int num, String title, String url, float score) {
        this.num = num;
        this.title = title;
        this.url = url;
        this.score = score;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }


}
