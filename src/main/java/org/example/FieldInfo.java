/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: FieldInfo.java
 *
 * Created On: 2022-06-28
 */

package org.example;

import org.apache.parquet.schema.PrimitiveType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * FieldInfo contains FieldIdx, the index of the field in the database schema, FieldType and the FieldName.
 */
public class FieldInfo implements Serializable, Comparable<FieldInfo>
{

public static final FieldInfo[] EMPTY_FIELD_INFO_ARRAY = new FieldInfo[0];

private static final long serialVersionUID = -5639028053782761876L;

private String m_sFieldName;
private FieldType m_fieldType;
private final PrimitiveType m_primitiveType;
private String m_sFieldAlias;

private final int m_nTableIdx;
private final int m_nFieldIdx;

public FieldInfo(String sFieldName, FieldType fieldType)
{
    this(sFieldName, fieldType, null, sFieldName, -1, -1);
}

public FieldInfo(String sFieldName, FieldType fieldType, PrimitiveType primitiveType)
{
    this(sFieldName, fieldType, primitiveType, sFieldName, -1, -1);
}

public FieldInfo(FieldInfo fieldInfoIn)
{
    this(fieldInfoIn, -1, -1);
}

public FieldInfo(FieldInfo fieldInfoIn, int nTableIdx, int nFieldIdx)
{
    this(fieldInfoIn.m_sFieldName, fieldInfoIn.m_fieldType, fieldInfoIn.m_primitiveType, fieldInfoIn.m_sFieldAlias,
        nTableIdx, nFieldIdx);
}

public FieldInfo(String sFieldName, FieldType fieldType, String sFieldAlias, int nTableIdx, int nFieldIdx)
{
    this(sFieldName, fieldType, null, sFieldAlias, nTableIdx, nFieldIdx);
}

public FieldInfo(String sFieldName, FieldType fieldType, PrimitiveType primitiveType, String sFieldAlias, int nTableIdx,
    int nFieldIdx)
{
    m_fieldType = fieldType;
    m_sFieldName = sFieldName;
    m_sFieldAlias = sFieldAlias;
    m_nTableIdx = nTableIdx;
    m_nFieldIdx = nFieldIdx;
    m_primitiveType = primitiveType;
}

public int getTableIdx()
{
    return m_nTableIdx;
}

public int getFieldIdx()
{
    return m_nFieldIdx;
}

public String getFieldName()
{
    return m_sFieldName;
}

public FieldType getFieldType()
{
    return m_fieldType;
}

public PrimitiveType getPrimitiveType(){
    return m_primitiveType;
}

public void setFieldType(FieldType fieldType)
{
    m_fieldType = fieldType;
}

public String getFieldAlias()
{
    return m_sFieldAlias;
}

public void setFieldAlias(String sFieldAlias)
{
    m_sFieldAlias = sFieldAlias;
}

public FieldInfo createCopyWithTableIdxAndKeyIdx(int nTableIdx, int nFieldIdx)
{
    return new FieldInfo(this, nTableIdx, nFieldIdx);
}

@Override
public String toString()
{
    return "FieldInfo{" + "m_sFieldName='" + m_sFieldName + '\'' + ", m_fieldType=" + m_fieldType + ", m_sFieldAlias='"
        + m_sFieldAlias + '\'' + ", m_nTableIdx=" + m_nTableIdx + ", m_nFieldIdx=" + m_nFieldIdx + '}';
}

@Override
public boolean equals(Object obj)
{
    if (obj == this)
    {
        return true;
    }

    if (!(obj instanceof FieldInfo))
    {
        return false;
    }

    return (compareTo((FieldInfo) obj) == 0);
}

@Override
public int compareTo(FieldInfo that)
{
    if (this.getFieldAlias().equals(that.getFieldAlias()))
    {
        if (this.getFieldType().equals(that.getFieldType()))
        {
            return 0;
        }
    }

    return -1;
}

public boolean equalsByFieldName(FieldInfo that)
{
    return this.getFieldName().equals(that.getFieldName());
}

public void write(DataOutput dataOutput)
{
    try
    {
        dataOutput.writeUTF(m_sFieldName);
        dataOutput.writeUTF(m_fieldType.name());
    } catch (Exception e) {
        throw new RuntimeException("Unable to write row info to stream");
    }
}

public void readFields(DataInput dataInput)
{
    try
    {
        m_sFieldName = dataInput.readUTF();
        m_fieldType = FieldType.valueOf(dataInput.readUTF());
    } catch (Exception e) {
        throw new RuntimeException("Unable to write row info to stream");
    }
}

public static Comparator<FieldInfo> getFieldInfoComparator() {
    return FieldInfo::compareTo;
}

@Override
public int hashCode() {
    return Objects.hash(m_sFieldName, getFieldType(), getPrimitiveType(), m_sFieldAlias, m_nTableIdx, m_nFieldIdx);
}
} ///////// End of class ///////// End of class
