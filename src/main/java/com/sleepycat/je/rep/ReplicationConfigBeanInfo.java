/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep;

import java.beans.BeanDescriptor;
import java.beans.PropertyDescriptor;

/**
 * @hidden
 * Getter/Setters for JavaBean based tools.
 */
public class ReplicationConfigBeanInfo 
    extends ReplicationMutableConfigBeanInfo {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(ReplicationConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(ReplicationConfig.class);
    }
}
