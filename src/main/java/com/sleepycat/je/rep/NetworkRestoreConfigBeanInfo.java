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
public class NetworkRestoreConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(NetworkRestoreConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(NetworkRestoreConfig.class);
    }
}
