/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: ByteArrayConverter.java
 *
 * Created On: 2022-06-29
 */

package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class ByteArrayComparator implements IByteArrayComparator
{

private int m_largestSize = 0;
private int m_rowByteSize = 0;
private List<byte[]> m_listOfAllRows = new ArrayList<>();

public static int compare(RowData r1, RowData r2)
{
    for (int i = 0; i < r1.getFieldCount(); i++)
    {
        int res = r1.getFieldData(i).compareTo(r2.getFieldData(i));
        if (res != 0)
        {
            return res;
        }
    }
    return 0;
}

public static int byteCompare(byte[] b1, byte[] b2)
{
    int compareResult = 0;
    for (int i = 0; i < b1.length; ++i)
    {
        compareResult = Byte.compare(b1[i], b2[i]);
        if( compareResult != 0 ){
            return compareResult;
        }
    }
    return compareResult;
}

@Override
public byte[] getDoubleBytesForNeg(Double num)
{
    byte[] doubleBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
    for (int i = 0; i < 8; i++)
    {
        doubleBytes[i] = (byte) ~doubleBytes[i];
    }
    return doubleBytes;
}

@Override
public void calculateByteArraySize(RowData row)
{
    int count = 0;
    FieldType type;
    for (int i = 0; i < row.getFieldCount(); i++)
    {
        type = row.getFieldData(i).getFieldType();
        if (type == FieldType.INTEGER || type == FieldType.FLOAT)
        {
            count += 4;
        }
        else if (type == FieldType.LONG || type == FieldType.DOUBLE)
        {
            count += 8;
        }
        else if (type == FieldType.BOOLEAN)
        {
            count += 1;
        }
    }
    m_rowByteSize = count;
}

public void convertRowDataToBytes(RowData row)
{
    FieldData fieldData;
    FieldType type;
    List<byte[]> listOfAllByteArrays = new ArrayList<>();
    for (int i = 0; i < row.getFieldCount(); i++)
    {
        fieldData = row.getFieldData(i);
        type = fieldData.getFieldType();
        if (type == FieldType.INTEGER)
        {
            listOfAllByteArrays.add(getIntegerBytes((Integer) fieldData.getValue()));
        }
        else if (type == FieldType.LONG)
        {
            listOfAllByteArrays.add(getLongBytes((Long) fieldData.getValue()));
        }
        else if (type == FieldType.FLOAT && (Float) fieldData.getValue() < 0)
        {
            listOfAllByteArrays.add(getFloatBytesForNeg((Float) fieldData.getValue()));
        }
        else if (type == FieldType.FLOAT)
        {
            listOfAllByteArrays.add(getFloatBytesForPos((Float) fieldData.getValue()));
        }
        else if (type == FieldType.DOUBLE && (Double) fieldData.getValue() < 0)
        {
            listOfAllByteArrays.add(getDoubleBytesForNeg((Double) fieldData.getValue()));
        }
        else if (type == FieldType.DOUBLE)
        {
            listOfAllByteArrays.add(getDoubleBytesForPos((Double) fieldData.getValue()));
        }
        else if (type == FieldType.BOOLEAN)
        {
            listOfAllByteArrays.add(getBooleanBytes((Boolean) fieldData.getValue()));
        }
    }
    m_listOfAllRows.add(mergeAllByteArrays(listOfAllByteArrays));
}

@Override
public byte[] mergeAllByteArrays(List<byte[]> list)
{
    int index = 0;
    byte[] byteList = new byte[m_rowByteSize];
    for (int i = 0; i < list.size(); i++)
    {
        for (int j = 0; j < list.get(i).length; j++)
        {
            byteList[index] = list.get(i)[j];
            index++;
        }
    }
    return byteList;
}

@Override
public byte[] getBooleanBytes(boolean val)
{
    byte[] boolBytes = new byte[1];
    if(val){
        boolBytes[0] = 2;
    }
    else{
        boolBytes[0] = 1;
    }
    return boolBytes;
}

@Override
public byte[] getDoubleBytesForPos(Double num)
{
    byte[] floatBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
    floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
    return floatBytes;
}

@Override
public byte[] getLongBytes(long num)
{
    byte[] longBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(num).array();
    if (num < 0)
    {
        longBytes[0] = (byte) (longBytes[0] & ~(1 << 7));
    }
    else
    {
        longBytes[0] = (byte) ((longBytes[0] | (1 << 7)));
    }
    return longBytes;
}

@Override
public byte[] getFloatBytesForNeg(float num)
{
    byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
    for (int i = 0; i < 4; i++)
    {
        floatBytes[i] = (byte) ~floatBytes[i];
    }
    return floatBytes;
}

@Override
public byte[] getFloatBytesForPos(float num)
{
    byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
    floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
    return floatBytes;
}

@Override
public byte[] getIntegerBytes(int num)
{
    byte[] intBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(num).array();
    if (num < 0)
    {
        intBytes[0] = (byte) (intBytes[0] & ~(1 << 7));
    }
    else
    {
        intBytes[0] = (byte) ((intBytes[0] | (1 << 7)));
    }
    return intBytes;
}

@Override
public List<byte[]> getListOfAllRows()
{
    return m_listOfAllRows;
}

@Override
public byte[] getPaddedString(String str, int size)
{
    int strLen = str.length();
    byte[] bytesOut = new byte[size]; // assuming string is ASCII encoded.
    int i;
    for (i = 0; i < strLen; i++)
    {
        bytesOut[i] = (byte) (str.charAt(i) & 0xFF);
    }
    for (; i < size; i++)
    {
        bytesOut[i] = (byte) 0;
    }

    return bytesOut;
}
} ///////// End of class
