/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickHouseTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================
    private static final String CLICKHOUSE_UNKNOWN = "UNKNOWN";

    // -------------------------integer types----------------------------
    private static final String CLICKHOUSE_UINT8 = "UInt8";
    private static final String CLICKHOUSE_UINT16 = "UInt16";
    private static final String CLICKHOUSE_UINT32 = "UInt32";
    private static final String CLICKHOUSE_UINT64 = "UInt64";
    private static final String CLICKHOUSE_UINT128 = "UInt128";
    private static final String CLICKHOUSE_UINT256 = "UInt256";
    private static final String CLICKHOUSE_INT8 = "Int8";
    private static final String CLICKHOUSE_INT16 = "Int16";
    private static final String CLICKHOUSE_INT32 = "Int32";
    private static final String CLICKHOUSE_INT64 = "Int64";
    private static final String CLICKHOUSE_INT128 = "Int128";
    private static final String CLICKHOUSE_INT256 = "Int256";

    // -------------------------floating-point numbers----------------------------
    private static final String CLICKHOUSE_FLOAT32 = "Float32";
    private static final String CLICKHOUSE_FLOAT64 = "Float64";
    private static final String CLICKHOUSE_DECIMAL = "Decimal";

    // -------------------------boolean----------------------------
    private static final String CLICKHOUSE_BOOLEAN = "Boolean";

    // -------------------------strings----------------------------
    private static final String CLICKHOUSE_STRING = "String";
    private static final String CLICKHOUSE_FIXEDSTRING = "FixedString";

    // -------------------------dates----------------------------
    private static final String CLICKHOUSE_DATE = "Date";
    private static final String CLICKHOUSE_DATE32 = "Date32";
    private static final String CLICKHOUSE_DATETIME = "DateTime";
    private static final String CLICKHOUSE_DATETIME64 = "DateTime64";

    // -------------------------json----------------------------
    private static final String CLICKHOUSE_JSON = "JSON";

    // -------------------------uuid----------------------------
    private static final String CLICKHOUSE_UUID = "UUID";

    // -------------------------low cardinality types----------------------------
    private static final String CLICKHOUSE_ENUM = "Enum";
    private static final String CLICKHOUSE_LOW_CARDINALITY = "LowCardinality";

    // -------------------------arrays----------------------------
    private static final String CLICKHOUSE_ARRAY = "Array";

    // -------------------------other types----------------------------
    private static final String CLICKHOUSE_MAP = "Map";
    private static final String CLICKHOUSE_SIMPLE_AGGREGATE_FUNCTION = "SimpleAggregateFunction";
    private static final String CLICKHOUSE_AGGREGATE_FUNCTION = "AggregateFunction";
    private static final String CLICKHOUSE_NESTED = "Nested";
    private static final String CLICKHOUSE_TUPLE = "Tuple";
    private static final String CLICKHOUSE_NULLABLE = "Nullable";
    private static final String CLICKHOUSE_NULL = "NULL";
    private static final String CLICKHOUSE_IPV4 = "IPv4";
    private static final String CLICKHOUSE_IPV6 = "IPv6";
    private static final String CLICKHOUSE_POINT = "Point";
    private static final String CLICKHOUSE_RING = "Ring";
    private static final String CLICKHOUSE_POLYGON = "Polygon";
    private static final String CLICKHOUSE_MULTIPOLYGON = "MultiPolygon";
    private static final String CLICKHOUSE_EXPRESSION = "Expression";
    private static final String CLICKHOUSE_SET = "Set";
    private static final String CLICKHOUSE_NOTHING = "Nothing";
    private static final String CLICKHOUSE_INTERVAL = "Interval";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String clickhouseType = metadata.getColumnTypeName(colIndex);
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (clickhouseType) {
            case CLICKHOUSE_BOOLEAN:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case CLICKHOUSE_INT8:
                return BasicType.BYTE_TYPE;
            case CLICKHOUSE_UINT8:
            case CLICKHOUSE_INT16:
                return BasicType.SHORT_TYPE;
            case CLICKHOUSE_UINT16:
            case CLICKHOUSE_INT32:
                return BasicType.INT_TYPE;
            case CLICKHOUSE_UINT32:
            case CLICKHOUSE_INT64:
            case CLICKHOUSE_UINT64:
                return BasicType.LONG_TYPE;
            case CLICKHOUSE_STRING:
            case CLICKHOUSE_FIXEDSTRING:
            case CLICKHOUSE_INT128:
            case CLICKHOUSE_UINT128:
            case CLICKHOUSE_INT256:
            case CLICKHOUSE_UINT256:
            case CLICKHOUSE_JSON:
            case CLICKHOUSE_UUID:
            case CLICKHOUSE_IPV4:
            case CLICKHOUSE_IPV6:
            case CLICKHOUSE_POINT:
            case CLICKHOUSE_RING:
            case CLICKHOUSE_POLYGON:
            case CLICKHOUSE_MULTIPOLYGON:
                return BasicType.STRING_TYPE;
            case CLICKHOUSE_FLOAT32:
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_FLOAT64:
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case CLICKHOUSE_DATE:
            case CLICKHOUSE_DATE32:
            case CLICKHOUSE_DATETIME:
            case CLICKHOUSE_DATETIME64:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case CLICKHOUSE_ARRAY:
                return PrimitiveByteArrayType.INSTANCE;

            case CLICKHOUSE_ENUM:
            case CLICKHOUSE_LOW_CARDINALITY:
            case CLICKHOUSE_MAP:
            case CLICKHOUSE_SIMPLE_AGGREGATE_FUNCTION:
            case CLICKHOUSE_AGGREGATE_FUNCTION:
            case CLICKHOUSE_NESTED:
            case CLICKHOUSE_TUPLE:
            case CLICKHOUSE_NULLABLE:
            case CLICKHOUSE_NULL:
            case CLICKHOUSE_EXPRESSION:
            case CLICKHOUSE_SET:
            case CLICKHOUSE_NOTHING:
            case CLICKHOUSE_INTERVAL:
            case CLICKHOUSE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support ClickHouse type '%s' on column '%s'  yet.",
                                clickhouseType, jdbcColumnName));
        }
    }
}
