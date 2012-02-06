package org.jacuzzi;

import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.MappedTo;
import org.jacuzzi.mapping.Transient;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
@MappedTo("PhantomFieldObject")
public class PhantomFieldObject {
    @Id
    private int id;

    @SuppressWarnings("UnusedDeclaration")
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Transient
    public int getNonExistingField() {
        return 42;
    }

    @Transient
    public void setNonExistingField(@SuppressWarnings("UnusedParameters") int value) {
        // No operations.
    }
}
