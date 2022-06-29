/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: RowInfo.java
 *
 * Created On: 2022-06-28
 */

package org.example;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class RowInfo implements Serializable
{

private static final long serialVersionUID = 5701998331022616753L;

private final FieldInfo[] m_aFieldInfo;

@SuppressWarnings("unused")
public RowInfo(FieldInfo fieldInfo)
{
    this(new FieldInfo[]{fieldInfo}); // Convert into an array with single element
}

public RowInfo(FieldInfo[] aFieldInfo)
{
    m_aFieldInfo = aFieldInfo;
}

public RowInfo(RowInfo rowInfoIn)
{
    m_aFieldInfo = new FieldInfo[rowInfoIn.getFieldCount()];
    for (int i = 0; i < m_aFieldInfo.length; i++)
    {
        m_aFieldInfo[i] = new FieldInfo(rowInfoIn.getFieldInfo(i));
    }
}

public FieldInfo[] getAllFieldInfo()
{
    return m_aFieldInfo;
}

public int getFieldCount()
{
    return m_aFieldInfo.length;
}

public FieldInfo getFieldInfo(int nFieldIdx)
{
    return m_aFieldInfo[nFieldIdx];
}

public void setFieldInfo(int nFieldIdx, FieldInfo fieldInfo)
{
    m_aFieldInfo[nFieldIdx] = fieldInfo;
}

public FieldInfo getFieldInfo(String sFieldName)
{
    FieldInfo fieldInfoOut = null;

    int nFieldInfoIdx = getFieldInfoIdx(sFieldName);
    if (nFieldInfoIdx >= 0)
    {
        fieldInfoOut = m_aFieldInfo[nFieldInfoIdx];
    }

    return fieldInfoOut;
}

public int getFieldInfoIdx(String sFieldName)
{
    int nFieldCount = getFieldCount();
    for (int i = 0; i < nFieldCount; i++)
    {
        if (sFieldName.compareToIgnoreCase(m_aFieldInfo[i].getFieldName()) == 0
            || sFieldName.compareToIgnoreCase(m_aFieldInfo[i].getFieldAlias()) == 0)
        {
            return i;
        }
    }

    return -1;
}

public void write(DataOutput dataOutput) throws IOException
{
    for (FieldInfo fieldInfo : m_aFieldInfo)
    {
        fieldInfo.write(dataOutput);
    }
}

public void readFields(DataInput dataInput) throws IOException
{
    for (FieldInfo fieldInfo : m_aFieldInfo)
    {
        fieldInfo.readFields(dataInput);
    }
}

public static RowInfo concat(RowInfo[] aRowInfo){

    ArrayList<FieldInfo> listFieldInfo = new ArrayList<>();
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < aRowInfo.length; i++)
    {
        //This is for non projected information
        if (aRowInfo[i] != null) {
            Collections.addAll(listFieldInfo, aRowInfo[i].getAllFieldInfo());
        }
    }

    return new RowInfo(listFieldInfo.toArray(FieldInfo.EMPTY_FIELD_INFO_ARRAY));
}

@Override
public String toString()
{
    return getClass().getSimpleName() + '{' + "fieldCount=" + getFieldCount() + " " + Arrays.toString(
        m_aFieldInfo) + '}';
}

@Override
public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RowInfo)) return false;
    RowInfo rowInfo = (RowInfo) o;
    return Arrays.equals(m_aFieldInfo, rowInfo.m_aFieldInfo);
}

@Override
public int hashCode() {
    return Arrays.hashCode(m_aFieldInfo);
}
} ///////// End of class ///////// End of class
