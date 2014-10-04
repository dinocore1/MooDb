/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import com.sleepycat.util.ConfigBeanInfoBase;

import java.beans.BeanDescriptor;
import java.beans.PropertyDescriptor;

/**
 * @hidden
 * Getter/Setters for JavaBean based tools.
 */
public class TransactionConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(TransactionConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(TransactionConfig.class);
    }
}
