package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MainApp
{

private static int largestSize = 0;
private static int rowByteSize = 0;
private static List<byte[]> listOfAllRows = new ArrayList<>();

public static void main(String... args)
{
    FieldInfo[] fieldInfos = new FieldInfo[3];
    fieldInfos[0] = new FieldInfo("field1", FieldType.INTEGER);
    fieldInfos[1] = new FieldInfo("field2", FieldType.LONG);
    fieldInfos[2] = new FieldInfo("field2", FieldType.LONG);
    RowDataGenerator generator = new RowDataGenerator(fieldInfos, 10_000_000);

    long lStartTime = System.nanoTime();
    List<RowData> result = generator.generateRowDataList();

    System.out.println("Data generated in: " + (System.nanoTime() - lStartTime));


    lStartTime = System.nanoTime();
    result.sort(MainApp::compare);
    System.out.println("Time took to sort row data: " + (System.nanoTime() - lStartTime));
//    System.out.println(compare(result.get(0), result.get(1)));


    lStartTime = System.nanoTime();
    rowByteSize = calculateByteArraySize(result.get(0));
    for (RowData row : result)
    {
        convertRowDataToBytes(row);
    }
    System.out.println("Time took to sort row data: " + (System.nanoTime() - lStartTime));

    lStartTime = System.nanoTime();
    listOfAllRows.sort(MainApp::memcmp);
    System.out.println("Time took to sort row data byte array: " + (System.nanoTime() - lStartTime));
}

static void convertRowDataToBytes(RowData row)
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
    }
    listOfAllRows.add(mergeAllByteArrays(listOfAllByteArrays));
}

static byte[] mergeAllByteArrays(List<byte[]> list)
{
    int index = 0;
    byte[] byteList = new byte[rowByteSize];
    for (int i = 0; i < list.size(); i++)
    {
        for (int j = 0; j < list.get(i).length; j++)
        {
            byteList[index] = list.get(i)[j];
        }
    }
    return byteList;
}

static int calculateByteArraySize(RowData row)
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
    }
    return count;
}

static int compare(RowData r1, RowData r2)
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

static byte[] getDoubleBytesForNeg(Double num)
{
    byte[] floatBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
    for (int i = 0; i < 8; i++)
    {
        floatBytes[i] = (byte) ~floatBytes[i];
    }
    return floatBytes;
}

static byte[] getDoubleBytesForPos(Double num)
{
    byte[] floatBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
    floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
    return floatBytes;
}

static byte[] getLongBytes(long num)
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

static byte[] getFloatBytesForNeg(float num)
{
    byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
    for (int i = 0; i < 4; i++)
    {
        floatBytes[i] = (byte) ~floatBytes[i];
    }
    return floatBytes;
}

static byte[] getFloatBytesForPos(float num)
{
    byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
    floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
    return floatBytes;
}

static byte[] getIntegerBytes(int num)
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

static byte[] getPaddedString(String str, int size)
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

static int memcmp(byte[] b1, byte[] b2)
{
    for (int i = 0; i < b1.length; ++i)
    {
        int b1i = (b1[i] & 0xFF);
        int b2i = (b2[i] & 0xFF);

        return Integer.compare(b1i, b2i);
    }

    return 0;
}

}

