using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text;

namespace DatabaseCloner
{
    class Program
    {
        static void Main(string[] args)
        {
            string databaseName = "";
            string sourceConnectionString = $"Server=;Database=;User Id=;Password=;TrustServerCertificate=True;";
            string targetServer = "localhost";
            string targetDatabase = databaseName;
            string targetUser = "";
            string targetPassword = "";

            string targetConnectionString =
                $"Server={targetServer};Database={targetDatabase};User Id={targetUser};Password={targetPassword};TrustServerCertificate=True;";

            CreateDatabaseIfNotExists(targetServer, targetDatabase, targetUser, targetPassword);

            // Faz o clone completo
            CloneDatabase(sourceConnectionString, targetConnectionString);

            Console.WriteLine("Clone finalizado.");
        }

        static void CreateDatabaseIfNotExists(string server, string database, string user, string password)
        {
            string masterConnectionString =
                $"Server={server};Database=master;User Id={user};Password={password};TrustServerCertificate=True;";

            using (var connection = new SqlConnection(masterConnectionString))
            {
                connection.Open();
                Console.WriteLine("Verificando se o banco de dados já existe...");

                var checkDatabaseExists = connection.QueryFirstOrDefault<int>(
                    $"SELECT COUNT(*) FROM sys.databases WHERE name = @database", new { database });

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
            using (var targetConnection = new SqlConnection(targetConnectionString))
            {
                sourceConnection.Open();
                targetConnection.Open();

                Console.WriteLine("Clonando schemas do banco de dados...");
                var createSchemasScript = GenerateCreateSchemasScript(sourceConnection);
                ExecuteScript(targetConnection, createSchemasScript, nameof(GenerateCreateSchemasScript));
                Console.WriteLine("Schemas clonados com sucesso.");

                Console.WriteLine("Clonando tabelas (estrutura com IDENTITY, colunas computadas, etc.)...");
                var createTablesScript = GenerateCreateTableScript(sourceConnection);
                ExecuteScript(targetConnection, createTablesScript, nameof(GenerateCreateTableScript));
                Console.WriteLine("Tabelas clonadas com sucesso.");

                Console.WriteLine("Clonando dados das tabelas (respeitando IDENTITY)...");
                var tables = GetTables(sourceConnection);
                foreach (var table in tables)
                {
                    CloneTableData(sourceConnection, targetConnection, table);
                }
                Console.WriteLine("Dados clonados com sucesso.");

                Console.WriteLine("Clonando constraints (PRIMARY KEY, UNIQUE)...");
                var createPKUQScript = GeneratePrimaryKeyAndUniqueConstraintsScript(sourceConnection);
                ExecuteScript(targetConnection, createPKUQScript, nameof(GeneratePrimaryKeyAndUniqueConstraintsScript));
                Console.WriteLine("Constraints PK e Unique clonadas com sucesso.");

                Console.WriteLine("Clonando CHECK constraints...");
                var createCheckScript = GenerateCheckConstraintsScript(sourceConnection);
                ExecuteScript(targetConnection, createCheckScript, nameof(GenerateCheckConstraintsScript));
                Console.WriteLine("Check constraints clonadas com sucesso.");

                Console.WriteLine("Clonando chaves estrangeiras...");
                var createForeignKeysScript = GenerateForeignKeyConstraintsScript(sourceConnection);
                ExecuteScript(targetConnection, createForeignKeysScript, nameof(GenerateForeignKeyConstraintsScript));
                Console.WriteLine("Foreign keys clonadas com sucesso.");

                Console.WriteLine("Clonando índices...");
                var createIndexesScript = GenerateIndexesScript(sourceConnection);
                ExecuteScript(targetConnection, createIndexesScript, nameof(GenerateIndexesScript));
                Console.WriteLine("Índices clonados com sucesso.");

                Console.WriteLine("Clonando índices full-text...");
                var createFullTextScript = GenerateFullTextIndexesScript(sourceConnection);
                ExecuteScript(targetConnection, createFullTextScript, nameof(GenerateFullTextIndexesScript));
                Console.WriteLine("Índices full-text clonados com sucesso.");

                Console.WriteLine("Clonando triggers...");
                var createTriggersScript = GenerateTriggersScript(sourceConnection);
                ExecuteScript(targetConnection, createTriggersScript, nameof(GenerateTriggersScript));
                Console.WriteLine("Triggers clonadas com sucesso.");
            }
        }

        #region Geração de Scripts

        /// <summary>
        /// Gera script de criação de todos os schemas presentes no banco de origem (exceto alguns ignorados).
        /// </summary>
        static string GenerateCreateSchemasScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            // Ajuste se quiser ignorar ou não certos schemas
            var schemas = connection.Query<string>(
                @"SELECT DISTINCT s.name
                  FROM sys.schemas s
                  INNER JOIN sys.tables t ON t.schema_id = s.schema_id
                  WHERE s.name NOT IN ('dbo','sys')"
            );

            foreach (var schema in schemas)
            {
                script.AppendLine($@"
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
BEGIN
    EXEC('CREATE SCHEMA [{schema}]');
END
GO
");
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar cada tabela (colunas, incluindo IDENTITY, computadas, default constraints).
        /// </summary>
        static string GenerateCreateTableScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var tables = GetTables(connection);

            foreach (var table in tables)
            {
                // Pula schemas que você não quer clonar
                if (table.Schema == "" || table.Schema == "")
                    continue;

                // Obtem colunas, inclusive identity e computadas
                var columns = connection.Query<ColumnInfo>(@"
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.precision AS [Precision],
    c.scale AS [Scale],
    c.is_nullable AS IsNullable,
    ISNULL(ic.is_identity, 0) AS IsIdentity,
    ic.seed_value AS IdentitySeed,
    ic.increment_value AS IdentityIncrement,
    cc.is_computed AS IsComputed,
    cc.definition AS ComputedDefinition,
    dc.definition AS DefaultDefinition
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.identity_columns ic 
       ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.computed_columns cc
       ON c.object_id = cc.object_id AND c.column_id = cc.column_id
LEFT JOIN sys.default_constraints dc
       ON c.object_id = dc.parent_object_id
       AND c.column_id = dc.parent_column_id
WHERE c.object_id = OBJECT_ID(@fullName)
ORDER BY c.column_id;
",
                    new { fullName = $"[{table.Schema}].[{table.TableName}]" });

                script.AppendLine($@"IF OBJECT_ID('{table.Schema}.{table.TableName}', 'U') IS NULL
BEGIN
    CREATE TABLE [{table.Schema}].[{table.TableName}]
    (");

                var columnDefs = new List<string>();

                foreach (var col in columns)
                {
                    // Se for coluna computada
                    if (col.IsComputed == true)
                    {
                        // Ex: [ColX] AS (<expressão>) [PERSISTED]
                        var computed = $"[{col.ColumnName}] AS ({col.ComputedDefinition})";
                        columnDefs.Add(computed);
                    }
                    else
                    {
                        // Coluna normal
                        var dataType = GetSqlDataType(col);

                        // Ex: [ColX] datatype [IDENTITY(a,b)] [NOT NULL|NULL] [DEFAULT (foo)]
                        string identityPart = col.IsIdentity ? $" IDENTITY({col.IdentitySeed},{col.IdentityIncrement})" : "";
                        string nullablePart = col.IsNullable == true ? "NULL" : "NOT NULL";
                        string defaultPart = "";
                        if (!string.IsNullOrEmpty(col.DefaultDefinition))
                        {
                            defaultPart = $" DEFAULT {col.DefaultDefinition}";
                        }

                        string colDef = $"[{col.ColumnName}] {dataType}{identityPart} {nullablePart}{defaultPart}";
                        columnDefs.Add(colDef);
                    }
                }

                script.AppendLine("    " + string.Join(",\n    ", columnDefs));
                script.AppendLine(@");
END
GO
");
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar PRIMARY KEY e UNIQUE constraints.
        /// (Já adaptado para forçar COLLATE em colunas geradas.)
        /// </summary>
        static string GeneratePrimaryKeyAndUniqueConstraintsScript(SqlConnection connection)
        {
            var script = new StringBuilder();
            var constraintsAndIndexes = connection.Query<string>(@"
SELECT 
    'IF NOT EXISTS (SELECT * FROM sys.key_constraints WHERE name = ''' 
    + k.name COLLATE DATABASE_DEFAULT 
    + ''')
    BEGIN
        ALTER TABLE ' 
    + QUOTENAME(OBJECT_SCHEMA_NAME(k.parent_object_id) COLLATE DATABASE_DEFAULT) 
    + '.' 
    + QUOTENAME(OBJECT_NAME(k.parent_object_id) COLLATE DATABASE_DEFAULT) 
    + ' ADD CONSTRAINT ' 
    + k.name COLLATE DATABASE_DEFAULT 
    + ' ' 
    + CASE k.type 
        WHEN 'PK' THEN 'PRIMARY KEY (' 
            + STUFF
            (
                (
                   SELECT ', ' + QUOTENAME(c.name COLLATE DATABASE_DEFAULT)
                   FROM sys.index_columns ic 
                   JOIN sys.columns c 
                       ON ic.column_id = c.column_id 
                      AND ic.object_id = c.object_id 
                   WHERE ic.object_id = k.parent_object_id 
                     AND ic.index_id = k.unique_index_id
                     AND ic.is_included_column = 0
                   ORDER BY ic.key_ordinal
                   FOR XML PATH('')
                ), 
                1, 
                2, 
                ''
            ) 
            + ')'
        WHEN 'UQ' THEN 'UNIQUE (' 
            + STUFF
            (
                (
                   SELECT ', ' + QUOTENAME(c.name COLLATE DATABASE_DEFAULT)
                   FROM sys.index_columns ic 
                   JOIN sys.columns c 
                       ON ic.column_id = c.column_id 
                      AND ic.object_id = c.object_id 
                   WHERE ic.object_id = k.parent_object_id 
                     AND ic.index_id = k.unique_index_id
                     AND ic.is_included_column = 0
                   ORDER BY ic.key_ordinal
                   FOR XML PATH('')
                ),
                1,
                2,
                ''
            ) 
            + ')'
      END 
    + '
    END
    GO'
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

        /// <summary>
        /// Gera script para criar CHECK constraints.
        /// (Forçar COLLATE em cc.name se der conflito.)
        /// </summary>
        static string GenerateCheckConstraintsScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            var checks = connection.Query<string>(@"
SELECT 
  'IF NOT EXISTS (SELECT * FROM sys.check_constraints WHERE name = ''' 
   + cc.name COLLATE DATABASE_DEFAULT + ''')
   BEGIN
       ALTER TABLE ' 
       + QUOTENAME(s.name COLLATE DATABASE_DEFAULT) + '.' 
       + QUOTENAME(t.name COLLATE DATABASE_DEFAULT) + ' 
       ADD CONSTRAINT ' + cc.name COLLATE DATABASE_DEFAULT + ' CHECK ' + cc.definition + '
   END
   GO'
FROM sys.check_constraints cc
JOIN sys.tables t ON cc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
ORDER BY cc.name;
");

            foreach (var item in checks)
            {
                script.AppendLine(item);
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar FOREIGN KEYS.
        /// (Forçar COLLATE em fk.name e demais partes, se necessário).
        /// </summary>
        static string GenerateForeignKeyConstraintsScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            // Note: se surgir erro de collation, force aqui também: fk.name COLLATE DATABASE_DEFAULT as ForeignKeyName
            var foreignKeys = connection.Query<ForeignKeyInfo>(@"
SELECT 
    fk.name COLLATE DATABASE_DEFAULT AS ForeignKeyName,
    OBJECT_SCHEMA_NAME(fkc.parent_object_id) COLLATE DATABASE_DEFAULT AS ParentSchemaName,
    OBJECT_NAME(fkc.parent_object_id) COLLATE DATABASE_DEFAULT AS ParentTableName,
    OBJECT_SCHEMA_NAME(fkc.referenced_object_id) COLLATE DATABASE_DEFAULT AS ReferencedSchemaName,
    OBJECT_NAME(fkc.referenced_object_id) COLLATE DATABASE_DEFAULT AS ReferencedTableName,
    STUFF((
        SELECT ', ' + QUOTENAME(cparent.name COLLATE DATABASE_DEFAULT)
        FROM sys.foreign_key_columns fkc2
        JOIN sys.columns cparent ON fkc2.parent_object_id = cparent.object_id AND fkc2.parent_column_id = cparent.column_id
        WHERE fkc2.constraint_object_id = fk.object_id
        ORDER BY fkc2.constraint_column_id
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    ,1,2,'') AS ParentColumns,
    STUFF((
        SELECT ', ' + QUOTENAME(cref.name COLLATE DATABASE_DEFAULT)
        FROM sys.foreign_key_columns fkc3
        JOIN sys.columns cref ON fkc3.referenced_object_id = cref.object_id AND fkc3.referenced_column_id = cref.column_id
        WHERE fkc3.constraint_object_id = fk.object_id
        ORDER BY fkc3.constraint_column_id
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    ,1,2,'') AS ReferencedColumns
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
GROUP BY fk.name, fkc.parent_object_id, fkc.referenced_object_id, fk.object_id
ORDER BY fk.name;
");

            foreach (var fk in foreignKeys)
            {
                string fkNameSafe = fk.ForeignKeyName;
                // Se quiser truncar nomes grandes
                if (fkNameSafe.Length > 128) fkNameSafe = fkNameSafe.Substring(0, 128);

                script.AppendLine($@"
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = '{fkNameSafe}')
BEGIN
    ALTER TABLE [{fk.ParentSchemaName}].[{fk.ParentTableName}]
    ADD CONSTRAINT [{fkNameSafe}]
    FOREIGN KEY ({fk.ParentColumns})
    REFERENCES [{fk.ReferencedSchemaName}].[{fk.ReferencedTableName}] ({fk.ReferencedColumns});
END
GO
");
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar índices (excluindo PK e UQ).
        /// Inclui o tratamento de índices filtrados (WHERE...).
        /// (Forçar COLLATE em i.name, c.name, etc. se der conflito).
        /// </summary>
        static string GenerateIndexesScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            var indexes = connection.Query<string>(@"
SELECT
    'IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ''' 
    + i.name COLLATE DATABASE_DEFAULT 
    + ''' AND object_id = ' + CAST(i.object_id AS VARCHAR(10)) + ')
     BEGIN
         CREATE ' +
         CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END +
         i.type_desc + ' INDEX [' + i.name COLLATE DATABASE_DEFAULT + '] ON ' +
         QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id) COLLATE DATABASE_DEFAULT) + '.' 
         + QUOTENAME(OBJECT_NAME(i.object_id) COLLATE DATABASE_DEFAULT) +
         ' (' + STUFF((
                SELECT ', ' + QUOTENAME(c.name COLLATE DATABASE_DEFAULT)
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id
                  AND ic.index_id = i.index_id
                  AND ic.is_included_column = 0
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')' +
         CASE 
             WHEN (SELECT COUNT(*) 
                   FROM sys.index_columns ic2
                   WHERE ic2.object_id = i.object_id 
                     AND ic2.index_id = i.index_id 
                     AND ic2.is_included_column = 1) > 0
             THEN ' INCLUDE (' + STUFF((
                SELECT ', ' + QUOTENAME(c2.name COLLATE DATABASE_DEFAULT)
                FROM sys.index_columns ic2
                JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id
                  AND ic2.index_id = i.index_id
                  AND ic2.is_included_column = 1
                ORDER BY ic2.key_ordinal
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
             ELSE ''
         END +
         CASE WHEN i.filter_definition IS NOT NULL
              THEN ' WHERE ' + i.filter_definition
              ELSE ''
         END + ';
     END
     GO'
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.is_primary_key = 0 
  AND i.is_unique_constraint = 0
  AND i.name IS NOT NULL
ORDER BY t.name, i.name;
");

            foreach (var item in indexes)
            {
                script.AppendLine(item);
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar catálogos e índices full-text.
        /// </summary>
        static string GenerateFullTextIndexesScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            // Criar catálogos
            var catalogs = connection.Query<string>(@"
SELECT 
    'IF NOT EXISTS(SELECT * FROM sys.fulltext_catalogs WHERE name = ''' 
    + ftc.name COLLATE DATABASE_DEFAULT 
    + ''')
     BEGIN
         CREATE FULLTEXT CATALOG [' + ftc.name COLLATE DATABASE_DEFAULT + ']
     END
     GO'
FROM sys.fulltext_catalogs ftc
");

            foreach (var cat in catalogs)
            {
                script.AppendLine(cat);
            }

            // Verificar se há algum default. Se não existir, cria um default
            script.AppendLine(@"
IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE is_default = 1)
BEGIN
    CREATE FULLTEXT CATALOG DefaultFullTextCatalog AS DEFAULT;
END
GO
");

            // Criar full-text indexes
            var ftIndexes = connection.Query<string>(@"
SELECT
    'IF NOT EXISTS(SELECT * FROM sys.fulltext_indexes WHERE object_id = ' 
    + CAST(i.object_id AS VARCHAR(10)) 
    + ')
     BEGIN
         CREATE FULLTEXT INDEX ON ' 
         + QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id) COLLATE DATABASE_DEFAULT) + '.' 
         + QUOTENAME(OBJECT_NAME(i.object_id) COLLATE DATABASE_DEFAULT) +
         ' (' + STUFF((
            SELECT ', ' + QUOTENAME(c.name COLLATE DATABASE_DEFAULT)
            FROM sys.fulltext_index_columns fic
            JOIN sys.columns c ON fic.object_id = c.object_id AND fic.column_id = c.column_id
            WHERE fic.object_id = i.object_id
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') +
         ') KEY INDEX ' + QUOTENAME(idx.name COLLATE DATABASE_DEFAULT) +
         ' ON ' + QUOTENAME(ISNULL(ftc.name COLLATE DATABASE_DEFAULT, 'DefaultFullTextCatalog')) + ';
     END
     GO'
FROM sys.fulltext_indexes i
JOIN sys.indexes idx ON i.object_id = idx.object_id AND i.unique_index_id = idx.index_id
LEFT JOIN sys.fulltext_catalogs ftc ON i.fulltext_catalog_id = ftc.fulltext_catalog_id;
");

            foreach (var item in ftIndexes)
            {
                script.AppendLine(item);
            }

            return script.ToString();
        }

        /// <summary>
        /// Gera script para criar triggers de tabela (INSERT/UPDATE/DELETE).
        /// </summary>
        static string GenerateTriggersScript(SqlConnection connection)
        {
            var script = new StringBuilder();

            // Forçar collation nos campos de metadados que podem gerar conflito
            var triggers = connection.Query<TriggerInfo>(@"
SELECT 
    tr.name COLLATE DATABASE_DEFAULT AS TriggerName,
    s.name COLLATE DATABASE_DEFAULT AS SchemaName,
    t.name COLLATE DATABASE_DEFAULT AS TableName,
    m.definition AS TriggerDefinition,
    tr.is_disabled AS IsDisabled
FROM sys.triggers tr
JOIN sys.tables t ON tr.parent_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.sql_modules m ON tr.object_id = m.object_id
WHERE tr.is_ms_shipped = 0
ORDER BY tr.name;
");

            foreach (var trg in triggers)
            {
                // Aqui forçamos IF NOT EXISTS ... e usamos o TriggerName que já está com COLLATE forçado
                script.AppendLine($@"
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = '{trg.TriggerName}')
BEGIN
    {trg.TriggerDefinition}
    {(trg.IsDisabled ? $"ALTER TRIGGER [{trg.TriggerName}] DISABLE" : "")}
END
GO
");
            }

            return script.ToString();
        }

        #endregion

        #region Cópia de Dados

        /// <summary>
        /// Retorna lista de tabelas base (ignorando alguns schemas).
        /// </summary>
        static IEnumerable<(string TableName, string Schema)> GetTables(SqlConnection connection)
        {
            return connection.Query<(string TableName, string Schema)>(@"
SELECT t.name, s.name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name NOT IN ('sys')
ORDER BY s.name, t.name;
");
        }

        /// <summary>
        /// Copia dados de uma tabela para a outra, respeitando colunas IDENTITY.
        /// </summary>
        static void CloneTableData(SqlConnection sourceConnection, SqlConnection targetConnection, (string TableName, string Schema) table)
        {
            Console.WriteLine($"Copiando dados da tabela {table.Schema}.{table.TableName}...");

            // Descobrir se há coluna identity. Se houver, vamos precisar de SET IDENTITY_INSERT ON
            bool hasIdentity = targetConnection.QuerySingleOrDefault<int>(@"
SELECT COUNT(*)
FROM sys.columns c
JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE c.object_id = OBJECT_ID(@fullName)",
                new { fullName = $"[{table.Schema}].[{table.TableName}]" }) > 0;

            // Carrega todos os dados da tabela origem
            var data = sourceConnection.Query($"SELECT * FROM [{table.Schema}].[{table.TableName}]").ToList();
            if (data.Count == 0)
            {
                Console.WriteLine("   -> Tabela vazia, nenhum registro para copiar.");
                return;
            }

            // Obter mapeamento de colunas
            var targetCols = GetColumnsInfo(targetConnection, table);

            // Se a tabela tiver identity, faremos INSERT manual com SET IDENTITY_INSERT
            // senão, podemos usar SqlBulkCopy diretamente
            if (hasIdentity)
            {
                using (var cmd = targetConnection.CreateCommand())
                {
                    cmd.CommandText = $"SET IDENTITY_INSERT [{table.Schema}].[{table.TableName}] ON";
                    cmd.ExecuteNonQuery();
                }

                const int batchSize = 1000;
                int totalRows = data.Count;
                int numberOfBatches = (int)Math.Ceiling(totalRows / (double)batchSize);

                for (int b = 0; b < numberOfBatches; b++)
                {
                    var slice = data.Skip(b * batchSize).Take(batchSize);
                    var sbInsert = new StringBuilder();

                    foreach (var rowObj in slice)
                    {
                        var rowDict = (IDictionary<string, object>)rowObj;
                        var colNames = new List<string>();
                        var colValues = new List<string>();

                        foreach (var colName in targetCols.Keys)
                        {
                            colNames.Add($"[{colName}]");
                            var val = rowDict.ContainsKey(colName) ? rowDict[colName] : null;
                            colValues.Add(ToSqlLiteral(val));
                        }

                        sbInsert.AppendLine($@"
INSERT INTO [{table.Schema}].[{table.TableName}] 
({string.Join(",", colNames)}) 
VALUES ({string.Join(",", colValues)});
");
                    }

                    using (var cmd = targetConnection.CreateCommand())
                    {
                        cmd.CommandTimeout = 600;
                        cmd.CommandText = sbInsert.ToString();
                        cmd.ExecuteNonQuery();
                    }

                    Console.WriteLine($"   -> Lote {b + 1}/{numberOfBatches} inserido.");
                }

                using (var cmd = targetConnection.CreateCommand())
                {
                    cmd.CommandText = $"SET IDENTITY_INSERT [{table.Schema}].[{table.TableName}] OFF";
                    cmd.ExecuteNonQuery();
                }
            }
            else
            {
                // Pode usar SqlBulkCopy
                SqlBulkCopyWithData(sourceConnection, targetConnection, table, targetCols);
            }

            Console.WriteLine($"   -> {data.Count} registros copiados.");
        }

        /// <summary>
        /// Usa SqlBulkCopy para inserir dados (sem identidade).
        /// </summary>
        static void SqlBulkCopyWithData(SqlConnection sourceConnection, SqlConnection targetConnection,
                                        (string TableName, string Schema) table,
                                        Dictionary<string, ColumnMeta> columnsMeta)
        {
            var data = sourceConnection.Query($"SELECT * FROM [{table.Schema}].[{table.TableName}]").ToList();
            if (data.Count == 0) return;

            int batchSize = 3000;
            int totalRows = data.Count;
            int numberOfBatches = (int)Math.Ceiling(totalRows / (double)batchSize);

            for (int batch = 0; batch < numberOfBatches; batch++)
            {
                using (var bulkCopy = new SqlBulkCopy(targetConnection))
                {
                    bulkCopy.BulkCopyTimeout = 600; // 10 minutos
                    bulkCopy.DestinationTableName = $"[{table.Schema}].[{table.TableName}]";

                    var dt = new DataTable();
                    foreach (var col in columnsMeta)
                    {
                        dt.Columns.Add(new DataColumn(col.Key, col.Value.ClrType));
                    }

                    // Copia as linhas
                    foreach (var row in data.Skip(batch * batchSize).Take(batchSize))
                    {
                        var rowDict = (IDictionary<string, object>)row;
                        var dataRow = dt.NewRow();
                        foreach (var colName in columnsMeta.Keys)
                        {
                            if (rowDict.ContainsKey(colName))
                            {
                                object val = rowDict[colName];
                                // Ajuste de range de datas
                                if (val is DateTime dtVal)
                                {
                                    if (dtVal < new DateTime(1753, 1, 1))
                                        val = new DateTime(1753, 1, 1);
                                    else if (dtVal > new DateTime(9999, 12, 31))
                                        val = new DateTime(9999, 12, 31);
                                }
                                dataRow[colName] = val ?? DBNull.Value;
                            }
                            else
                            {
                                dataRow[colName] = DBNull.Value;
                            }
                        }
                        dt.Rows.Add(dataRow);
                    }

                    bulkCopy.WriteToServer(dt);
                }
            }
        }

        /// <summary>
        /// Converte o valor em literal SQL (entre aspas, ou NULL, etc.).
        /// </summary>
        static string ToSqlLiteral(object value)
        {
            if (value == null) return "NULL";
            if (value == DBNull.Value) return "NULL";

            if (value is string s)
            {
                // Escapar aspas
                s = s.Replace("'", "''");
                return $"N'{s}'";
            }
            if (value is bool b)
            {
                return b ? "1" : "0";
            }
            if (value is DateTime dt)
            {
                // Formato ISO
                return $"'{dt:yyyy-MM-dd HH:mm:ss.fff}'";
            }
            if (value is Guid g)
            {
                return $"'{g}'";
            }
            if (value is byte[] bytes)
            {
                // Transforma em 0x....
                var hex = BitConverter.ToString(bytes).Replace("-", "");
                return $"0x{hex}";
            }

            // numérico ou outros
            return value.ToString().Replace(',', '.');
        }

        #endregion

        #region Metadados de colunas

        /// <summary>
        /// Retorna metadados básicos das colunas (nome -> tipo .NET) da tabela de destino.
        /// </summary>
        static Dictionary<string, ColumnMeta> GetColumnsInfo(SqlConnection connection, (string TableName, string Schema) table)
        {
            var columnsInfo = new Dictionary<string, ColumnMeta>();

            var cols = connection.Query(@"
SELECT c.name AS ColumnName, t.name AS DataType
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(@fullName)
ORDER BY c.column_id",
                new { fullName = $"[{table.Schema}].[{table.TableName}]" });

            foreach (var c in cols)
            {
                string colName = c.ColumnName;
                string dataType = c.DataType;
                Type clrType = MapSqlToClrType(dataType);
                columnsInfo[colName] = new ColumnMeta { ColumnName = colName, DataType = dataType, ClrType = clrType };
            }

            return columnsInfo;
        }

        static Type MapSqlToClrType(string sqlType)
        {
            switch (sqlType.ToLower())
            {
                case "int": return typeof(int);
                case "bigint": return typeof(long);
                case "smallint": return typeof(short);
                case "tinyint": return typeof(byte);
                case "bit": return typeof(bool);
                case "decimal":
                case "numeric":
                case "money":
                case "smallmoney":
                    return typeof(decimal);
                case "float": return typeof(double);
                case "real": return typeof(float);
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                case "date":
                case "datetimeoffset":
                    return typeof(DateTime);
                case "char":
                case "varchar":
                case "text":
                case "nchar":
                case "nvarchar":
                case "ntext":
                    return typeof(string);
                case "binary":
                case "varbinary":
                case "image":
                    return typeof(byte[]);
                case "uniqueidentifier":
                    return typeof(Guid);
                case "time":
                    return typeof(TimeSpan);
                default:
                    return typeof(string);
            }
        }

        /// <summary>
        /// Monta o tipo SQL (com tamanho/precisão) para CREATE TABLE.
        /// </summary>
        static string GetSqlDataType(ColumnInfo col)
        {
            string baseType = col.DataType.ToLower();

            if (baseType == "binary" || baseType == "varbinary" ||
                baseType == "char" || baseType == "varchar" ||
                baseType == "nchar" || baseType == "nvarchar")
            {
                // max_length em bytes. Para nchar/nvarchar, o tamanho é max_length/2 em chars
                int maxLen = col.MaxLength;
                bool isNType = (baseType == "nchar" || baseType == "nvarchar");

                if (maxLen < 0)
                {
                    // -1 indica (max)
                    return baseType + "(max)";
                }
                else
                {
                    if (isNType) maxLen /= 2;
                    return $"{baseType}({maxLen})";
                }
            }
            else if (baseType == "decimal" || baseType == "numeric")
            {
                return $"{baseType}({col.Precision},{col.Scale})";
            }
            return baseType;
        }

        #endregion

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

    #region Classes de apoio

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

    public class TriggerInfo
    {
        public string TriggerName { get; set; }
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public string TriggerDefinition { get; set; }
        public bool IsDisabled { get; set; }
    }

    /// <summary>
    /// Metadados de cada coluna (para CREATE TABLE).
    /// </summary>
    public class ColumnInfo
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public int MaxLength { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public bool? IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public long? IdentitySeed { get; set; }
        public int? IdentityIncrement { get; set; }
        public bool? IsComputed { get; set; }
        public string ComputedDefinition { get; set; }
        public string DefaultDefinition { get; set; }
    }

    /// <summary>
    /// Metadados básicos para colunas no momento de BulkCopy (nome, tipo .NET).
    /// </summary>
    public class ColumnMeta
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public Type ClrType { get; set; }
    }

    #endregion
}
