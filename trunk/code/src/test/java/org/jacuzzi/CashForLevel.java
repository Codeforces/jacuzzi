package org.jacuzzi;

import org.jacuzzi.mapping.Id;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class CashForLevel {
    @Id
    private int level;
    private long cash;
    private long premiumCash;

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public long getCash() {
        return cash;
    }

    public void setCash(long cash) {
        this.cash = cash;
    }

    public long getPremiumCash() {
        return premiumCash;
    }

    public void setPremiumCash(long premiumCash) {
        this.premiumCash = premiumCash;
    }
}
