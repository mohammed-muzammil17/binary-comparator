/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: FieldData.java
 *
 * Created On: 2022-06-28
 */

package org.example;

import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Binary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Field class contains FieldType and FieldValue. Please refer to the enum FieldType for different types of field types
 * that are supported.
 */
public abstract class FieldData implements Cloneable, Comparable<FieldData>, Serializable, Writable
{

public static final FieldData[] EMPTY_FIELD_DATA_ARRAY = new FieldData[0];

private static final long serialVersionUID = 5241628374634023725L;

public abstract FieldType getFieldType();

public abstract Object getValue();

public abstract Object getStringValue();

public abstract void setValue(Object value);

protected abstract void readFieldsX(DataInput dataInput) throws IOException;

protected abstract void writeX(DataOutput dataOutput) throws IOException;

@SuppressWarnings("unused")
public static FieldData[] createCopyOfFieldDataArray(FieldData[] aFieldDataToCopyFrom)
{
    int nFieldDataCount = aFieldDataToCopyFrom.length;

    FieldData[] aFieldDataOut = new FieldData[nFieldDataCount];

    for (int i = 0; i < nFieldDataCount; i++)
    {
        aFieldDataOut[i] = aFieldDataToCopyFrom[i].clone();
    }

    return aFieldDataOut;
}

@SuppressWarnings("MethodDoesntCallSuperMethod")
@Override
public FieldData clone()
{
    FieldData fieldDataOut = getFieldType().allocateFieldData();
    fieldDataOut.setValue(getValue());
    return fieldDataOut;
}

@Override
public int compareTo(FieldData fieldDataToCompareWith)
{
    Object valThis = getValue();
    Object valToCompareWith = fieldDataToCompareWith.getValue();

    if (valThis == null && valToCompareWith == null)
    {
        return 0;
    }
    if (valThis == null)
    {
        return 1;
    }
    if (valToCompareWith == null)
    {
        return -1;
    }

    return getFieldType().compareX(valThis, valToCompareWith);
}

@Override
public boolean equals(Object obj)
{
    if (obj == this)
    {
        return true;
    }

    if (!(obj instanceof FieldData))
    {
        return false;
    }

    return (getFieldType().equalsX(this.getValue(), ((FieldData) obj).getValue()));
}

@Override
public int hashCode()
{
    return Objects.hash(getFieldType(), getValue());
}

@Override
public void readFields(DataInput dataInput) throws IOException
{
    if (dataInput.readByte() != 0)
    {
        readFieldsX(dataInput);
    }
    else
    {
        setValue(null);
    }
}

@Override
public void write(DataOutput dataOutput) throws IOException
{
    if (getValue() != null)
    {
        dataOutput.writeByte(1);
        writeX(dataOutput);
    }
    else
    {
        dataOutput.writeByte(0);
    }
}

@Override
public String toString()
{
    return getClass().getSimpleName() + '{' + "fieldType=" + getFieldType() + " value=" + getValue() + '}';
}

@SuppressWarnings("unused")
public static abstract class TypedFieldData<T> extends FieldData
{

    private static final long serialVersionUID = -1624537915557260783L;

}

public static class BinaryFieldData extends TypedFieldData<Binary>
{

    private static final long serialVersionUID = 4044930044176241607L;

    private Binary m_value;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.BINARY;
    }

    @Override
    public Binary getValue()
    {
        return m_value;
    }

    @Override
    public Object getStringValue()
    {
        return m_value;
    }

    @Override
    public void setValue(Object value)
    {
        m_value = (Binary) value;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = Binary.fromString(dataInput.readUTF());
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(m_value.toStringUsingUTF8());
    }

}

public static class DateFieldData extends TypedFieldData<LocalDate>
{

    private static final long serialVersionUID = 3730225904999531532L;

    private static final Integer NULL_VALUE = Integer.MIN_VALUE;

    private Integer m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.DATE;
    }

    @Override
    public Integer getValue()
    {
        return m_value;
    }

    @Override
    public LocalDate getStringValue()
    {
        return LocalDate.ofEpochDay(m_value);
    }

    @Override
    public void setValue(Object value)
    {
        m_value = value != null ? ((Number) value).intValue() : NULL_VALUE;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readInt();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(m_value);
    }

}

public static class BooleanFieldData extends TypedFieldData<Boolean>
{

    private static final long serialVersionUID = 4528228253253833639L;

    private static final Byte NULL_VALUE = Byte.MIN_VALUE;

    private byte m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.BOOLEAN;
    }

    @Override
    public Boolean getValue()
    {
        if (m_value == 1)
        {
            return Boolean.TRUE;
        }

        if (m_value == 0)
        {
            return Boolean.FALSE;
        }

        return null;
    }

    @Override
    public Boolean getStringValue()
    {
        if (m_value == 1)
        {
            return Boolean.TRUE;
        }

        if (m_value == 0)
        {
            return Boolean.FALSE;
        }

        return null;
    }

    @Override
    public void setValue(Object value)
    {
        if (value != null)
        {
            if ((Boolean) value)
            {
                m_value = 1;
            }
            else
            {
                m_value = 0;
            }
        }
        else
        {
            m_value = NULL_VALUE;
        }
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readByte();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeByte(m_value);
    }

}

public static class DoubleFieldData extends TypedFieldData<Double>
{

    private static final long serialVersionUID = -4678014356705055770L;

    private static final double NULL_VALUE = Double.MAX_VALUE;

    private double m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.DOUBLE;
    }

    @Override
    public Double getValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public Object getStringValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public void setValue(Object value)
    {
        m_value = value != null ? ((Number) value).doubleValue() : NULL_VALUE;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readDouble();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(m_value);
    }

}

public static class FloatFieldData extends TypedFieldData<Float>
{

    private static final long serialVersionUID = 3668561371898227085L;

    private static final float NULL_VALUE = Float.MAX_VALUE;

    private float m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.FLOAT;
    }

    @Override
    public Float getValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public Object getStringValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public void setValue(Object value)
    {
        m_value = value != null ? ((Number) value).floatValue() : NULL_VALUE;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readFloat();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeFloat(m_value);
    }

}

public static class Int96FieldData extends TypedFieldData<Binary>
{

    private static final long serialVersionUID = -4324914254336331407L;

    private Binary m_value;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.INT96;
    }

    @Override
    public Binary getValue()
    {
        return m_value;
    }

    @Override
    public Object getStringValue()
    {
        return m_value;
    }

    @Override
    public void setValue(Object value)
    {

        if (value instanceof Number)
        {
            m_value = Binary.fromString(value.toString());
        }
        else
        {
            m_value = (Binary) value;
        }
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = Binary.fromString(dataInput.readUTF());
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(m_value.toStringUsingUTF8());
    }

}

public static class IntegerFieldData extends TypedFieldData<Integer>
{

    private static final long serialVersionUID = 3730225004999531532L;

    private static final int NULL_VALUE = Integer.MAX_VALUE;

    private int m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.INTEGER;
    }

    @Override
    public Integer getValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public Object getStringValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public void setValue(Object value)
    {
        m_value = value != null ? ((Number) value).intValue() : NULL_VALUE;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readInt();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(m_value);
    }

}

public static class LongFieldData extends TypedFieldData<Long>
{

    private static final long serialVersionUID = -6504140158970786820L;

    private static final long NULL_VALUE = Long.MAX_VALUE;

    private long m_value = NULL_VALUE;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.LONG;
    }

    @Override
    public Long getValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public Object getStringValue()
    {
        return m_value != NULL_VALUE ? m_value : null;
    }

    @Override
    public void setValue(Object value)
    {
        m_value = value != null ? ((Number) value).longValue() : NULL_VALUE;
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readLong();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(m_value);
    }

}

public static class StringFieldData extends TypedFieldData<String>
{

    private static final long serialVersionUID = 1578922341863124931L;

    private String m_value;

    @Override
    public FieldType getFieldType()
    {
        return FieldType.STRING;
    }

    @Override
    public String getValue()
    {
        return m_value;
    }

    @Override
    public Object getStringValue()
    {
        return m_value;
    }

    @Override
    public void setValue(Object value)
    {
        // The parquet internally maintains string as binary. We need to use that for the conversion
        if (value instanceof Binary)
        {
            m_value = ((Binary) value).toStringUsingUTF8();
        }
        else
        {
            m_value = (String) value;
        }
    }

    @Override
    protected void readFieldsX(DataInput dataInput) throws IOException
    {
        m_value = dataInput.readUTF();
    }

    @Override
    protected void writeX(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(m_value);
    }

}

} ///////// End of class ///////// End of class
