package org.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MainApp {
private static int largestSize = 0;

    public static void main(String... args){

    }

    static byte[] getDoubleBytesForNeg(Double num){
        byte[] floatBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
        for(int i = 0; i < 8; i++){
            floatBytes[i] = (byte) ~floatBytes[i];
        }
        return floatBytes;
    }

    static byte[] getDoubleBytesForPos(Double num){
        byte[] floatBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(num).array();
        floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
        return floatBytes;
    }

    static  byte[] getLongBytes(long num){
        byte[] longBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(num).array();
        if(num < 0){
            longBytes[0] = (byte) (longBytes[0] & ~(1 << 7));
        }
        else{
            longBytes[0] = (byte) ((longBytes[0] | (1 << 7)));
        }
        return longBytes;
    }
    static byte[] getFloatBytesForNeg(float num){
        byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
        for(int i = 0; i < 4; i++){
            floatBytes[i] = (byte) ~floatBytes[i];
        }
        return floatBytes;
    }

    static byte[] getFloatBytesForPos(float num){
        byte[] floatBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(num).array();
        floatBytes[0] = (byte) ((floatBytes[0] | (1 << 7)));
        return floatBytes;
    }

    static byte[] getIntegerBytes(int num){
        byte[] intBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(num).array();
        if(num < 0){
            intBytes[0] = (byte) (intBytes[0] & ~(1 << 7));
        }
        else{
            intBytes[0] = (byte) ((intBytes[0]  | (1 << 7)));
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
        for(int i = 0; i < b1.length; ++i)
        {
            int b1i = (b1[i] & 0xFF);
            int b2i = (b2[i] & 0xFF);

            return Integer.compare(b1i, b2i);
        }

        return 0;
    }
}

