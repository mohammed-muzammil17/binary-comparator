/*
 * Copyright (c) 2022 Uniphi Inc
 * All rights reserved.
 *
 * File Name: IByteArrayConverter.java
 *
 * Created On: 2022-06-29
 */

package org.example;

import java.util.List;

public interface IByteArrayComparator
{
byte[] getDoubleBytesForNeg(Double num);

void calculateByteArraySize(RowData row);
byte[] mergeAllByteArrays(List<byte[]> list);

byte[] getBooleanBytes(boolean val);

byte[] getDoubleBytesForPos(Double num);

byte[] getLongBytes(long num);

byte[] getFloatBytesForNeg(float num);

byte[] getFloatBytesForPos(float num);

byte[] getIntegerBytes(int num);

List<byte[]> getListOfAllRows();

void convertRowDataToBytes(RowData row);

byte[] getPaddedString(String str, int size);

} ///////// End of interface
