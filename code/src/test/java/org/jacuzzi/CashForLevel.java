package org.jacuzzi;

import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.Transient;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class CashForLevel {
    @Id
    private int level;
    private long cash;
    private long premiumCash;
    private long a;

    @Transient
    private long transientField;

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

    public long getA() {
        return a;
    }

    public void setA(long a) {
        this.a = a;
    }

    public long getTransientField() {
        return transientField;
    }

    public void setTransientField(long transientField) {
        this.transientField = transientField;
    }

    public long get() {
        return transientField;
    }

    public void set(long transientField) {
        this.transientField = transientField;
    }
}
