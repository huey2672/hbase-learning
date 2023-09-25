package com.huey.learning.hbase.sample.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author huey
 */
public class FamilyPrefixFilter extends FilterBase {

    private final byte[] prefix;

    public FamilyPrefixFilter(byte[] prefix) {
        this.prefix = prefix;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        return filterCell(cell);
    }

    @Override
    public ReturnCode filterCell(Cell cell) throws IOException {
        byte[] family = CellUtil.cloneFamily(cell);
        if (Bytes.startsWith(family, prefix)) {
            return ReturnCode.INCLUDE;
        }
        else {
            return ReturnCode.SKIP;
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        // 将过滤器的参数转换为字节数组进行序列化
        return prefix;
    }

    public static FamilyPrefixFilter parseFrom(byte[] pbBytes) throws DeserializationException {
        // 从字节数组中反序列化出过滤器的参数
        return new FamilyPrefixFilter(pbBytes);
    }

}
