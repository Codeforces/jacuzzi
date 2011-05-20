package org.jacuzzi;

import org.jacuzzi.mapping.Id;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class ConfigurationObject {
    @Id
    private int id;
    private long experience;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getExperience() {
        return experience;
    }

    public void setExperience(long experience) {
        this.experience = experience;
    }
}
