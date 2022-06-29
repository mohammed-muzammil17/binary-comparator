/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: FieldType.java
 *
 * Created On: 2022-06-28
 */

package org.example;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import java.time.LocalDate;

/**
 * FieldType supports all the types supported by a Parquet File and makes them available as field types supported by
 * Java. The fields INT96 and BINARY are stored as Binary Bytes.
 */
public enum FieldType
{
    BINARY
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.BinaryFieldData();
            }

            @SuppressWarnings("deprecation")
            @Override
            public int compareX(Object o1, Object o2)
            {
                return ((Binary) o1).compareTo((Binary) o2);
            }

            @Override
            public Binary getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getBinary(nFieldIdx, nRowIdx);
            }
        },
    BOOLEAN
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.BooleanFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return Boolean.compare((Boolean) o1, (Boolean) o2);
            }

            @Override
            public Boolean getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getBoolean(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Byte.BYTES;
            }
        },
    DATE
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.DateFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return ((LocalDate) o1).compareTo((LocalDate)o2);
            }


            @Override
            public Object getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return null;
            }

        },
    DOUBLE
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.DoubleFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return Double.compare((Double) o1, (Double) o2);
            }

            @Override
            public Double getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getDouble(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Double.BYTES;
            }
        },
    FLOAT
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.FloatFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return Float.compare((Float) o1, (Float) o2);
            }

            @Override
            public Float getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getFloat(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Float.BYTES;
            }
        },
    INT96
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.Int96FieldData();
            }

            @SuppressWarnings("deprecation")
            @Override
            public int compareX(Object o1, Object o2)
            {
                return ((Binary) o1).compareTo((Binary) o2);
            }

            @Override
            public Binary getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getInt96(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Long.BYTES + Integer.BYTES;
            }
        },
    INTEGER
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.IntegerFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return Integer.compare((Integer) o1, (Integer) o2);
            }

            @Override
            public Integer getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getInteger(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Integer.BYTES;
            }
        },
    LONG
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.LongFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return Long.compare((Long) o1, (Long) o2);
            }

            @Override
            public Long getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getLong(nFieldIdx, nRowIdx);
            }

            @Override
            public int getFieldSizeInBytes()
            {
                return Long.BYTES;
            }
        },
    STRING
        {
            @Override
            public FieldData allocateFieldData()
            {
                return new FieldData.StringFieldData();
            }

            @Override
            public int compareX(Object o1, Object o2)
            {
                return ((String) o1).compareTo((String) o2);
            }

            @Override
            public String getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx)
            {
                return group.getString(nFieldIdx, nRowIdx);
            }
        };

public abstract FieldData allocateFieldData();

public abstract int compareX(Object o1, Object o2);

public boolean equalsX(Object o1, Object o2)
{
    return o1.equals(o2);
}

public abstract Object getFieldDataFromParquetGroupInternal(Group group, int nFieldIdx, int nRowIdx);

@SuppressWarnings("unused")
public int getFieldSizeInBytes()
{
    throw new UnsupportedOperationException("Not a fixed length field. FieldType: " + name());
}

@SuppressWarnings("unused")
public Object getFieldDataFromParquetGroup(Group group, int nFieldIdx)
{
    return getFieldDataFromParquetGroup(group, nFieldIdx, 0);
}

public Object getFieldDataFromParquetGroup(Group group, int nFieldIdx, int nRowIdx)
{
    int nFieldRepetitionCount = group.getFieldRepetitionCount(nFieldIdx);

    if (nRowIdx < nFieldRepetitionCount)
    {
        return this.getFieldDataFromParquetGroupInternal(group, nFieldIdx, nRowIdx);
    }

    return null;
}
} ///////// End of class
