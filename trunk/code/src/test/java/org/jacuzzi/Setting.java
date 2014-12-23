package org.jacuzzi;

import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.OperationControl;

import java.util.Date;

/**
 * @author Maxim Shipko (sladethe@gmail.com)
 *         Date: 05.12.14
 */
public class Setting {
    @Id
    private long id;
    private String name;
    private String value;

    @OperationControl(ignoreUpdate = true)
    private Date updateTime;

    @OperationControl(ignoreSelect = true, ignoreInsert = true, ignoreUpdate = true)
    private Long pseudoTransientValue;

    @OperationControl(ignoreSelect = true)
    private Integer ignoreSelectValue;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getUpdateTime() {
        return updateTime == null ? null : new Date(updateTime.getTime());
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime == null ? null : new Date(updateTime.getTime());
    }

    public Long getPseudoTransientValue() {
        return pseudoTransientValue;
    }

    public void setPseudoTransientValue(Long pseudoTransientValue) {
        this.pseudoTransientValue = pseudoTransientValue;
    }

    public Integer getIgnoreSelectValue() {
        return ignoreSelectValue;
    }

    public void setIgnoreSelectValue(Integer ignoreSelectValue) {
        this.ignoreSelectValue = ignoreSelectValue;
    }
}
