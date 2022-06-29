/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: RowData.java
 *
 * Created On: 2022-06-28
 */

package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * RowData contains an array of Field and is used as a holder of values of all the columns in a data row.
 */
public class RowData implements Cloneable, Comparable<RowData>, Serializable, Writable
{

private static final long serialVersionUID = 249884948442224533L;

private FieldData[] m_aFieldData;


@SuppressWarnings("unused")
public RowData(FieldData fieldData)
{
    this(new FieldData[]{fieldData}); // Convert into an array with single element
}

public RowData(FieldData[] aFieldData)
{
    m_aFieldData = aFieldData;
}

public RowData(RowData rowDataToCopyFrom)
{
    m_aFieldData = FieldData.createCopyOfFieldDataArray(rowDataToCopyFrom.m_aFieldData);
}

public RowData(RowInfo rowInfo)
{
    FieldInfo[] aFieldInfo = rowInfo.getAllFieldInfo();

    m_aFieldData = new FieldData[aFieldInfo.length];
    for (int i = 0; i < m_aFieldData.length; i++)
    {
        m_aFieldData[i] = aFieldInfo[i].getFieldType().allocateFieldData();
    }
}

@SuppressWarnings("unused")
public void copyAllFieldDataVal(RowData rowDataIn)
{
    if (rowDataIn == this)
    {
        return;
    }

    for (int i = 0; i < m_aFieldData.length; i++)
    {
        m_aFieldData[i].setValue(rowDataIn.m_aFieldData[i].getValue());
    }
}

public int getFieldCount()
{
    return m_aFieldData.length;
}

public FieldData getFieldData(int nFieldIdx)
{
    return m_aFieldData[nFieldIdx];
}

public void setFieldData(int nFieldIdx, FieldData fieldDataIn)
{
    m_aFieldData[nFieldIdx] = fieldDataIn;
}

public FieldData[] getFieldDataArray()
{
    return m_aFieldData;
}

public void setRowData(RowData rowDataIn)
{
    m_aFieldData = rowDataIn.m_aFieldData;
}

@SuppressWarnings("MethodDoesntCallSuperMethod")
@Override
public RowData clone()
{
    return new RowData(this);
}

@Override
public int compareTo(RowData rowDataToCompareWith)
{
    int nFieldCountThis = getFieldCount();
    int nFieldCountToCompareWith = rowDataToCompareWith.getFieldCount();

    if (nFieldCountThis > nFieldCountToCompareWith)
    {
        return 1;
    }

    if (nFieldCountThis < nFieldCountToCompareWith)
    {
        return -1;
    }

    int nCompResult;

    for (int i = 0; i < m_aFieldData.length; i++)
    {
        nCompResult = m_aFieldData[i].compareTo(rowDataToCompareWith.m_aFieldData[i]);
        if (nCompResult != 0)
        {
            return nCompResult;
        }
    }

    return 0;
}

@Override
public boolean equals(Object obj)
{
    if (obj == this)
    {
        return true;
    }

    if (!(obj instanceof RowData))
    {
        return false;
    }

    return (compareTo((RowData) obj) == 0);
}

@Override
public int hashCode()
{
    return Arrays.deepHashCode(m_aFieldData);
}

@Override
public void readFields(DataInput dataInput) throws IOException
{
    for (FieldData fieldData : m_aFieldData)
    {
        fieldData.readFields(dataInput);
    }
}

@Override
public void write(DataOutput dataOutput) throws IOException
{
    for (FieldData fieldData : m_aFieldData)
    {
        fieldData.write(dataOutput);
    }
}

@Override
public String toString()
{
    StringBuilder sb = new StringBuilder();
    int i;

    for (i = 0; i < getFieldCount(); i++)
    {
        sb.append("|");
        sb.append(getFieldData(i).getStringValue());
        sb.append("\t");
    }

    return sb.toString();
}

public static RowData concat(RowData[] aRowData)
{
    ArrayList<FieldData> listFieldData = new ArrayList<>();
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < aRowData.length; i++)
    {
        //This is for non projected information
        if (aRowData[i] != null) {
            Collections.addAll(listFieldData, aRowData[i].getFieldDataArray());
        }
    }

    return new RowData(listFieldData.toArray(FieldData.EMPTY_FIELD_DATA_ARRAY));
}

} ///////// End of class///////// End of class
