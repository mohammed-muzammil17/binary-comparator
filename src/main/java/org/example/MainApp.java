package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MainApp
{
public static void main(String... args)
{
    ByteArrayComparator comparator = new ByteArrayComparator();
    FieldInfo[] fieldInfos = new FieldInfo[4];
    fieldInfos[0] = new FieldInfo("field1", FieldType.INTEGER);
    fieldInfos[1] = new FieldInfo("field2", FieldType.INTEGER);
    fieldInfos[2] = new FieldInfo("field3", FieldType.INTEGER);
    fieldInfos[3] = new FieldInfo("field4", FieldType.DOUBLE);
    RowDataGenerator generator = new RowDataGenerator(fieldInfos, 3_000_000);

    long lStartTime = System.nanoTime();
    List<RowData> result = generator.generateRowDataList();
    List<RowData> anotherResult = new ArrayList<>(result);
    System.out.println("Data generated in: " + (System.nanoTime() - lStartTime));

    lStartTime = System.nanoTime();
    result.sort(ByteArrayComparator::compare);
    System.out.println("Time took to sort row data: " + (System.nanoTime() - lStartTime));


    lStartTime = System.nanoTime();
    comparator.calculateByteArraySize(anotherResult.get(0));
    for (RowData row : anotherResult)
    {
        comparator.convertRowDataToBytes(row);
    }
    System.out.println("Time took to convert row data: " + (System.nanoTime() - lStartTime));

    lStartTime = System.nanoTime();
    List<byte[]> listOfAllRows = comparator.getListOfAllRows();
    listOfAllRows.sort(ByteArrayComparator::byteCompare);
    System.out.println("Time took to sort row data byte array: " + (System.nanoTime() - lStartTime));
}
}

