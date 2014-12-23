package org.jacuzzi;

/**
 * @author Maxim Shipko (sladethe@gmail.com)
 *         Date: 23.12.14
 */
public interface SettingDao {
    Setting find(Long id);

    void insert(Setting setting);

    void update(Setting setting);
}
