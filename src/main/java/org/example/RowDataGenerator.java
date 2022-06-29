/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: RowDataGenerator.java
 *
 * Created On: 2022-06-28
 */

package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RowDataGenerator
{
long iterationsSize;
FieldInfo[] fieldInfos;
RowInfo rowInfo;
RowData m_rowData;
Random m_random = new Random();
List<RowData> m_rowDataList = new ArrayList<>();

public RowDataGenerator(FieldInfo[] fieldInfos, long iterationsSize)
{
    this.fieldInfos = fieldInfos;
    this.iterationsSize = iterationsSize;
    rowInfo = new RowInfo(fieldInfos);
    m_rowData = new RowData(rowInfo);
}



public List<RowData> generateRowDataList()
{
    int i = 0;
    while (i < iterationsSize)
    {
        RowData randomRowData = generateRandomKeyData(m_rowData);
        m_rowDataList.add(randomRowData);
        i++;
    }
    return m_rowDataList;
}

private RowData generateRandomKeyData(RowData rowData)
{
    for (int i = 0; i < rowData.getFieldCount();i++){
        if(rowData.getFieldData(i).getFieldType() == FieldType.INTEGER){
            rowData.getFieldData(i).setValue(m_random.nextInt((int) (iterationsSize * 0.01)));
        }
        else if(rowData.getFieldData(i).getFieldType() == FieldType.LONG){
            rowData.getFieldData(i).setValue(m_random.nextLong());
        }
        else if(rowData.getFieldData(i).getFieldType() == FieldType.FLOAT){
            rowData.getFieldData(i).setValue(m_random.nextFloat());
        }
        else if(rowData.getFieldData(i).getFieldType() == FieldType.DOUBLE){
            rowData.getFieldData(i).setValue(m_random.nextDouble());
        }
    }
    return rowData.clone();
}


} ///////// End of class
