using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Dapper;
using Microsoft.Data.SqlClient;

namespace DatabaseCloner
{
    class Program
    {
        static void Main(string[] args)
        {
            string databaseName = "";
            string sourceConnectionString = $"";
            string targetServer = "";
            string targetDatabase = databaseName;
            string targetUser = "";
            string targetPassword = "";
            string targetConnectionString = $"Server={targetServer};Database={targetDatabase};User Id={targetUser};Password={targetPassword};TrustServerCertificate=True;";

            CreateDatabaseIfNotExists(targetServer, targetDatabase, targetUser, targetPassword);
            CloneDatabase(sourceConnectionString, targetConnectionString);
        }

        static void CreateDatabaseIfNotExists(string server, string database, string user, string password)
        {
            string masterConnectionString = $"Server={server};Database=master;User Id={user};Password={password};TrustServerCertificate=True;";
            using (var connection = new SqlConnection(masterConnectionString))
            {
                connection.Open();
                Console.WriteLine("Verificando se o banco de dados já existe...");
                var checkDatabaseExists = connection.QueryFirstOrDefault<int>($"SELECT COUNT(*) FROM sys.databases WHERE name = @database", new { database });
                if (checkDatabaseExists == 0)
                {
                    var createDatabaseQuery = $"CREATE DATABASE [{database}]";
                    connection.Execute(createDatabaseQuery);
                    Console.WriteLine($"Banco de dados '{database}' criado com sucesso.");
                }
                else
                {
                    Console.WriteLine($"Banco de dados '{database}' já existe.");
                }
            }
        }

        static void CloneDatabase(string sourceConnectionString, string targetConnectionString)
        {
            using (var sourceConnection = new SqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();
                using (var targetConnection = new SqlConnection(targetConnectionString))
                {
                    targetConnection.Open();

                    Console.WriteLine("Clonando schemas do banco de dados...");
                    var createSchemasScript = GenerateCreateSchemasScript(sourceConnection);
                    ExecuteScript(targetConnection, createSchemasScript, nameof(GenerateCreateSchemasScript));
                    Console.WriteLine("Schemas clonados com sucesso.");

                    Console.WriteLine("Clonando esquema do banco de dados...");
                    var createSchemaScript = GenerateCreateSchemaScript(sourceConnection);
                    ExecuteScript(targetConnection, createSchemaScript, nameof(GenerateCreateSchemaScript));
                    Console.WriteLine("Esquema do banco de dados clonado com sucesso.");

                    Console.WriteLine("Clonando dados das tabelas...");
                    var tables = GetTables(sourceConnection);
                    foreach (var table in tables)
                    {
                        if (table.TableName.Contains("AuditRegistros") || table.TableName.Contains("Laudo") || table.Schema == "AgendamentoShared" || table.Schema == "AgendamentoBkp")
                        {
                            Console.WriteLine($"Ignorando cópia de dados para a tabela {table.Schema}.{table.TableName}...");
                            continue;
                        }

                        Console.WriteLine($"Copiando dados da tabela {table.Schema}.{table.TableName}...");
                        CloneTableData(sourceConnection, targetConnection, table);
                        Console.WriteLine($"Dados da tabela {table.Schema}.{table.TableName} copiados com sucesso.");
                    }

                    Console.WriteLine("Clonando chaves primárias e únicas...");
                    var createPrimaryKeyAndUniqueConstraintsScript = GeneratePrimaryKeyAndUniqueConstraintsScript(sourceConnection);
                    ExecuteScript(targetConnection, createPrimaryKeyAndUniqueConstraintsScript, nameof(GeneratePrimaryKeyAndUniqueConstraintsScript));
                    Console.WriteLine("Chaves primárias e únicas clonadas com sucesso.");

                    Console.WriteLine("Clonando chaves estrangeiras...");
                    var createForeignKeyConstraintsScript = GenerateForeignKeyConstraintsScript(sourceConnection);
                    ExecuteScript(targetConnection, createForeignKeyConstraintsScript, nameof(GenerateForeignKeyConstraintsScript));
                    Console.WriteLine("Chaves estrangeiras clonadas com sucesso.");

                    Console.WriteLine("Clonando índices...");
                    var createIndexesScript = GenerateIndexesScript(sourceConnection);
                    ExecuteScript(targetConnection, createIndexesScript, nameof(GenerateIndexesScript));
                    Console.WriteLine("Índices clonados com sucesso.");

                    Console.WriteLine("Clonando índices full-text...");
                    var createFullTextIndexesScript = GenerateFullTextIndexesScript(sourceConnection);
                    ExecuteScript(targetConnection, createFullTextIndexesScript, nameof(GenerateFullTextIndexesScript));
                    Console.WriteLine("Índices full-text clonados com sucesso.");
                }
            }
        }

        static string GenerateCreateSchemasScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var schemas = connection.Query<string>("SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('dbo', 'sys', 'AgendamentoShared', 'AgendamentoBkp')");

            foreach (var schema in schemas)
            {
                script.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]');");
            }

            return script.ToString();
        }

        static string GenerateCreateSchemaScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var tables = GetTables(connection);

            foreach (var table in tables)
            {
                if (table.Schema == "AgendamentoShared" || table.Schema == "AgendamentoBkp")
                {
                    continue;
                }

                var columns = connection.Query($@"
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @TableName",
                    new { table.Schema, table.TableName });

                script.AppendLine($"CREATE TABLE [{table.Schema}].[{table.TableName}] (");

                foreach (var column in columns)
                {
                    var dataType = column.DATA_TYPE;
                    var maxLength = column.CHARACTER_MAXIMUM_LENGTH;

                    if (dataType == "nvarchar" && maxLength == -1)
                    {
                        dataType += "(MAX)";
                    }
                    else if (dataType == "varchar" && maxLength == -1)
                    {
                        dataType += "(MAX)";
                    }
                    else if (dataType == "ntext" || dataType == "text" || dataType == "image")
                    {
                        // Esses tipos de dados não precisam de tamanho especificado
                    }
                    else if (maxLength != null && maxLength != -1)
                    {
                        dataType += $"({maxLength})";
                    }


                    var columnDefinition = $"[{column.COLUMN_NAME}] {dataType}" +
                                           $"{(column.IS_NULLABLE == "NO" ? " NOT NULL" : " NULL")}" +
                                           $"{(column.COLUMN_DEFAULT != null ? $" DEFAULT {column.COLUMN_DEFAULT}" : "")}";
                    script.AppendLine($"{columnDefinition},");
                }

                script.Length -= 3; // Remove the last comma and newline characters
                script.AppendLine("\n);");
            }

            return script.ToString();
        }

        static IEnumerable<(string TableName, string Schema)> GetTables(SqlConnection connection)
        {
            return connection.Query<(string TableName, string Schema)>("SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                .Where(t => t.Schema != "AgendamentoShared" && t.Schema != "AgendamentoBkp");
        }

        static void CloneTableData(SqlConnection sourceConnection, SqlConnection targetConnection, (string TableName, string Schema) table)
        {
            var columnsInfo = GetColumnsInfo(targetConnection, table);
            var data = sourceConnection.Query($"SELECT * FROM [{table.Schema}].[{table.TableName}]").ToList();

            int batchSize = 3000;
            int totalRows = data.Count;
            int numberOfBatches = (int)Math.Ceiling(totalRows / (double)batchSize);

            for (int batch = 0; batch < numberOfBatches; batch++)
            {
                using (var bulkCopy = new SqlBulkCopy(targetConnection))
                {
                    bulkCopy.BulkCopyTimeout = 600; // Ajuste de tempo limite para 10 minutos
                    bulkCopy.DestinationTableName = $"[{table.Schema}].[{table.TableName}]";
                    var dataTable = new DataTable();

                    foreach (var column in columnsInfo)
                    {
                        dataTable.Columns.Add(new DataColumn(column.Key, GetColumnType(column.Key, sourceConnection, table)));
                    }

                    foreach (var row in data.Skip(batch * batchSize).Take(batchSize))
                    {
                        var rowDictionary = AdjustRow(columnsInfo, (IDictionary<string, object>)row);
                        var dataRow = dataTable.NewRow();
                        foreach (var column in columnsInfo.Keys)
                        {
                            dataRow[column] = rowDictionary[column] ?? DBNull.Value;
                        }
                        dataTable.Rows.Add(dataRow);
                    }

                    bulkCopy.WriteToServer(dataTable);
                    Console.WriteLine($"Lote {batch + 1} de {numberOfBatches} inserido na tabela {table.Schema}.{table.TableName}");
                }
            }
        }

        static Type GetColumnType(string columnName, SqlConnection connection, (string TableName, string Schema) table)
        {
            var columnType = connection.QuerySingle<string>($@"
                SELECT DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @TableName AND COLUMN_NAME = @ColumnName",
                new { table.Schema, table.TableName, ColumnName = columnName });

            return columnType switch
            {
                "int" => typeof(int),
                "bigint" => typeof(long),
                "smallint" => typeof(short),
                "tinyint" => typeof(byte),
                "bit" => typeof(bool),
                "decimal" => typeof(decimal),
                "numeric" => typeof(decimal),
                "money" => typeof(decimal),
                "smallmoney" => typeof(decimal),
                "float" => typeof(double),
                "real" => typeof(float),
                "datetime" => typeof(DateTime),
                "smalldatetime" => typeof(DateTime),
                "char" => typeof(string),
                "varchar" => typeof(string),
                "text" => typeof(string),
                "nchar" => typeof(string),
                "nvarchar" => typeof(string),
                "ntext" => typeof(string),
                "binary" => typeof(byte[]),
                "varbinary" => typeof(byte[]),
                "image" => typeof(byte[]),
                "uniqueidentifier" => typeof(Guid),
                _ => typeof(string)
            };
        }

        static IDictionary<string, object> AdjustRow(Dictionary<string, int?> columnsInfo, IDictionary<string, object> row)
        {
            foreach (var key in row.Keys.ToList())
            {
                if (row[key] is DateTime dateTime)
                {
                    if (dateTime < new DateTime(1753, 1, 1))
                    {
                        row[key] = new DateTime(1753, 1, 1);
                    }
                    else if (dateTime > new DateTime(9999, 12, 31))
                    {
                        row[key] = new DateTime(9999, 12, 31);
                    }
                }
                else if (row[key] is string str)
                {
                    if (columnsInfo.TryGetValue(key, out var maxLength) && maxLength.HasValue && maxLength.Value != -1 && str.Length > maxLength.Value)
                    {
                        row[key] = str.Substring(0, maxLength.Value);
                    }
                }
            }
            return row;
        }

        static Dictionary<string, int?> GetColumnsInfo(SqlConnection connection, (string TableName, string Schema) table)
        {
            var columnsInfo = new Dictionary<string, int?>();
            var columns = connection.Query($@"
                SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @TableName",
                new { table.Schema, table.TableName });

            foreach (var column in columns)
            {
                columnsInfo[column.COLUMN_NAME] = column.CHARACTER_MAXIMUM_LENGTH;
            }

            return columnsInfo;
        }

        static string GeneratePrimaryKeyAndUniqueConstraintsScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var constraintsAndIndexes = connection.Query<string>(@"
        SELECT 
            'IF NOT EXISTS (SELECT * FROM sys.key_constraints WHERE name = ''' + LEFT(k.name, 128) COLLATE DATABASE_DEFAULT + ''')
            BEGIN
                ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(k.parent_object_id)) COLLATE DATABASE_DEFAULT + '.' + QUOTENAME(OBJECT_NAME(k.parent_object_id)) COLLATE DATABASE_DEFAULT + 
                ' ADD CONSTRAINT ' + LEFT(k.name, 128) COLLATE DATABASE_DEFAULT + ' ' + 
                CASE k.type 
                    WHEN 'PK' THEN 'PRIMARY KEY (' + 
                        STUFF((SELECT ', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                               FROM sys.index_columns ic 
                               JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id 
                               WHERE ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id
                               FOR XML PATH('')), 1, 2, '') + ')'
                    WHEN 'UQ' THEN 'UNIQUE (' + 
                        STUFF((SELECT ', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                               FROM sys.index_columns ic 
                               JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id 
                               WHERE ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id
                               FOR XML PATH('')), 1, 2, '') + ')'
                END + '
            END'
        FROM sys.key_constraints k
        WHERE k.type IN ('PK', 'UQ')
        ORDER BY k.parent_object_id, k.name;
    ");

            foreach (var item in constraintsAndIndexes)
            {
                script.AppendLine(item);
            }

            return script.ToString();
        }


        static string GenerateForeignKeyConstraintsScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var foreignKeys = connection.Query<ForeignKeyInfo>(@"
        SELECT 
            fk.name AS ForeignKeyName,
            OBJECT_SCHEMA_NAME(fkc.parent_object_id) AS ParentSchemaName,
            OBJECT_NAME(fkc.parent_object_id) AS ParentTableName,
            OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS ReferencedSchemaName,
            OBJECT_NAME(fkc.referenced_object_id) AS ReferencedTableName,
            STRING_AGG(QUOTENAME(pc.name), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ParentColumns,
            STRING_AGG(QUOTENAME(rc.name), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ReferencedColumns
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
        JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
        GROUP BY fk.name, fkc.parent_object_id, fkc.referenced_object_id
        ORDER BY fkc.parent_object_id, fk.name;
    ");

            foreach (var fk in foreignKeys)
            {
                string foreignKeyName = fk.ForeignKeyName.Length > 128 ? fk.ForeignKeyName.Substring(0, 128) : fk.ForeignKeyName;
                string foreignKeySafeName = foreignKeyName.Replace('~', 'a');

                script.AppendLine($@"
            IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = '{foreignKeySafeName}')
            BEGIN
                ALTER TABLE [{fk.ParentSchemaName}].[{fk.ParentTableName}] ADD CONSTRAINT {foreignKeySafeName} FOREIGN KEY ({fk.ParentColumns}) REFERENCES [{fk.ReferencedSchemaName}].[{fk.ReferencedTableName}] ({fk.ReferencedColumns})
            END");
            }

            return script.ToString();
        }



        static string GenerateIndexesScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var indexes = connection.Query<string>(@"
        SELECT 
            'CREATE ' + 
            CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END + 
            i.type_desc COLLATE DATABASE_DEFAULT + ' INDEX ' + QUOTENAME(i.name) COLLATE DATABASE_DEFAULT + ' ON ' + 
            QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) COLLATE DATABASE_DEFAULT + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) COLLATE DATABASE_DEFAULT + ' (' +
            STUFF((SELECT ', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                   FROM sys.index_columns ic
                   JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                   WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                   AND ic.is_included_column = 0
                   FOR XML PATH('')), 1, 2, '') + ') ' +
            ISNULL('INCLUDE (' + STUFF((SELECT ', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                   FROM sys.index_columns ic
                   JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                   WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
                   FOR XML PATH('')), 1, 2, '') + ') ', '') +
            'WHERE ' + 
            STUFF((SELECT ' AND ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT + ' IS NOT NULL'
                   FROM sys.index_columns ic
                   JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                   WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                   AND ic.is_included_column = 0
                   FOR XML PATH('')), 1, 5, '')
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0
        ORDER BY i.object_id, i.index_id;
    ");
            foreach (var item in indexes)
            {
                script.AppendLine(item);
            }
            return script.ToString();
        }


        static string GenerateFullTextIndexesScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            // Verificar e criar catálogos de texto completo existentes na base de origem
            var fullTextCatalogs = connection.Query<string>(@"
        SELECT 
            'IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE name = ''' + ftc.name + ''')
            BEGIN
                CREATE FULLTEXT CATALOG ' + ftc.name + ';
            END'
        FROM sys.fulltext_catalogs ftc
    ");

            foreach (var catalog in fullTextCatalogs)
            {
                script.AppendLine(catalog);
            }

            // Verificação para catálogo de texto completo padrão e criação se não existir
            script.AppendLine(@"
    IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE is_default = 1)
    BEGIN
        CREATE FULLTEXT CATALOG DefaultFullTextCatalog AS DEFAULT;
    END
    ");

            var fullTextIndexes = connection.Query<string>(@"
    SELECT 
        'CREATE FULLTEXT INDEX ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) + ' (' + 
        STUFF((
            SELECT ', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
            FROM sys.fulltext_index_columns ic 
            JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
            WHERE ic.object_id = i.object_id
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + 
        ') KEY INDEX ' + QUOTENAME(idx.name) COLLATE DATABASE_DEFAULT + 
        ' ON ' + QUOTENAME(
            ISNULL(ftc.name, (SELECT TOP 1 name FROM sys.fulltext_catalogs WHERE is_default = 1))) COLLATE DATABASE_DEFAULT
    FROM sys.fulltext_indexes i
    JOIN sys.tables t ON i.object_id = t.object_id
    JOIN sys.indexes idx ON i.unique_index_id = idx.index_id AND i.object_id = idx.object_id
    LEFT JOIN sys.fulltext_catalogs ftc ON i.fulltext_catalog_id = ftc.fulltext_catalog_id");

            foreach (var item in fullTextIndexes)
            {
                script.AppendLine(item);
            }

            return script.ToString();
        }



        static void ExecuteScript(SqlConnection connection, string script, string metodoName)
        {
            var commands = script.Split(new[] { "GO" }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var commandText in commands)
            {
                if (!string.IsNullOrWhiteSpace(commandText))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = commandText.Trim();
                        command.CommandType = CommandType.Text;
                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{metodoName} - Erro ao executar comando: {commandText}");
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Comando SQL vazio encontrado, ignorando...");
                }
            }
        }
    }

    public class ForeignKeyInfo
    {
        public string ForeignKeyName { get; set; }
        public string ParentSchemaName { get; set; }
        public string ParentTableName { get; set; }
        public string ReferencedSchemaName { get; set; }
        public string ReferencedTableName { get; set; }
        public string ParentColumns { get; set; }
        public string ReferencedColumns { get; set; }
    }
}
