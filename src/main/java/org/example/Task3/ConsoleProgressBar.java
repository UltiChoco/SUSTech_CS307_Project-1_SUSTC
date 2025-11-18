package org.example.Task3;

public class ConsoleProgressBar {
    private int totalSteps;
    private int barLength;
    private char[] animationChars;
    private String prefix;
    private String filledChar;
    private String emptyChar;
    private String colorStart;
    private String colorReset;

    public ConsoleProgressBar(int totalSteps) {
        this(totalSteps, 50);
    }

    public ConsoleProgressBar(int totalSteps, int barLength) {
        this(totalSteps, barLength, new char[]{'|', '/', '-', '\\'});
    }

    public ConsoleProgressBar(int totalSteps, int barLength, char[] animationChars) {
        this.totalSteps = totalSteps;
        this.barLength = barLength;
        this.animationChars = animationChars;
        this.prefix = "";
        this.filledChar = "█";
        this.emptyChar = " ";
        this.colorStart = "";
        this.colorReset = "";
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setFilledChar(String filledChar) {
        this.filledChar = filledChar;
    }

    public void setEmptyChar(String emptyChar) {
        this.emptyChar = emptyChar;
    }

    public void setColor(String colorCode) {
        this.colorStart = colorCode;
        this.colorReset = "\033[0m";
    }

    public void update(int progress) {
        if (progress < 0) progress = 0;
        if (progress > totalSteps) progress = totalSteps;

        double ratio = (double) progress / totalSteps;
        int filled = (int) (ratio * barLength);
        StringBuilder bar = new StringBuilder();
        bar.append('[');

        for (int i = 0; i < barLength; i++) {
            if (i < filled) {
                bar.append(colorStart).append(filledChar).append(colorReset);
            } else {
                bar.append(emptyChar);
            }
        }
        bar.append(']');

        char anim = animationChars[progress % animationChars.length];
        int percent = (int) (ratio * 100);

        System.out.printf("\r%s%s %d%% %c", prefix, bar, percent, anim);

        if (progress == totalSteps) {
            System.out.println();
        }
    }
}

