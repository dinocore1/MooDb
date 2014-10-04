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
public class PreloadConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(PreloadConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(PreloadConfig.class);
    }
}
