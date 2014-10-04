/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep;

import java.beans.BeanDescriptor;
import java.beans.PropertyDescriptor;

import com.sleepycat.util.ConfigBeanInfoBase;

/**
 * @hidden
 * Getter/Setters for JavaBean based tools.
 */
public class ReplicationMutableConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(ReplicationMutableConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(ReplicationMutableConfig.class);
    }
}
