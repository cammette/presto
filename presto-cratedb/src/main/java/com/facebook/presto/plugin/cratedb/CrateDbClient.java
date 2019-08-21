/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.cratedb;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.crate.client.jdbc.CrateDriver;

import javax.inject.Inject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.sql.ResultSetMetaData.columnNullable;

public class CrateDbClient
        extends BaseJdbcClient
{
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    @Inject
    public CrateDbClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new CrateDriver(), config));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = this.toDataType(resultSet);
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private JdbcTypeHandle toDataType(ResultSet resultSet)
            throws SQLException
    {
        int dataType = 0;
        int columnSize = 0;
        int decimalDigits = 0;
        String typeName = resultSet.getString("TYPE_NAME");
        if (typeName.equalsIgnoreCase("text") || typeName.equalsIgnoreCase("ip")
                || typeName.equalsIgnoreCase("ip") || typeName.equalsIgnoreCase("char")) {
            dataType = Types.LONGNVARCHAR;
            columnSize = Integer.MAX_VALUE;
        }
        else if (typeName.equalsIgnoreCase("double precision")) {
            dataType = Types.NUMERIC;
            decimalDigits = 8;
            columnSize = 15;
        }
        else if (typeName.equalsIgnoreCase("real")) {
            dataType = Types.REAL;
        }
        else if (typeName.equalsIgnoreCase("bigint")) {
            dataType = Types.BIGINT;
        }
        else if (typeName.equalsIgnoreCase("integer")) {
            dataType = Types.INTEGER;
        }
        else if (typeName.equalsIgnoreCase("smallint")) {
            dataType = Types.SMALLINT;
        }
        else if (typeName.equalsIgnoreCase("timestamp with time zone")) {
            dataType = Types.TIMESTAMP;
        }
        else if (typeName.equalsIgnoreCase("timestamp without time zone")) {
            dataType = Types.TIMESTAMP;
        }
        else if (typeName.equalsIgnoreCase("boolean")) {
            dataType = Types.BOOLEAN;
        }else{
            dataType = Types.JAVA_OBJECT;
        }
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                dataType,
                columnSize,
                decimalDigits);
        return typeHandle;
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
              "",
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                layoutHandle.getTupleDomain(),
                Optional.empty());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (VARBINARY.equals(type)) {
            return "bytea";
        }

        return super.toSqlType(type);
    }

    @Override
    public void createTable(ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, null, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        try (Connection connection = connectionFactory.openConnection()) {
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }
}
