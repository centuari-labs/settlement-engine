import type { Pool, PoolClient } from 'pg';
import type { Match } from '../../schemas/match';

/**
 * Cache for the primary key column name of the matches table.
 * This avoids querying the schema on every insert.
 */
let matchesPrimaryKeyColumn: string | null = null;

/**
 * Cache for NOT NULL columns and their data types in the matches table.
 * This avoids querying the schema on every insert.
 */
let matchesNotNullColumns: Array<{ column_name: string; data_type: string }> | null = null;

/**
 * Determine the primary key column name for the matches table.
 *
 * @param client - Database client.
 * @returns The primary key column name (either 'id' or 'match_id').
 */
const getMatchesPrimaryKeyColumn = async (client: PoolClient | Pool): Promise<string> => {
  if (matchesPrimaryKeyColumn) {
    return matchesPrimaryKeyColumn;
  }

  // Query information_schema to find the primary key column
  const result = await client.query<{ column_name: string }>(
    `
      SELECT column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.table_name = 'matches'
        AND tc.constraint_type = 'PRIMARY KEY'
      LIMIT 1
    `,
  );

  if (result.rows.length > 0) {
    matchesPrimaryKeyColumn = result.rows[0].column_name;
    return matchesPrimaryKeyColumn;
  }

  // Fallback: try 'match_id' first (most common based on foreign key reference)
  matchesPrimaryKeyColumn = 'match_id';
  return matchesPrimaryKeyColumn;
};

/**
 * Get all NOT NULL columns (excluding the primary key) for the matches table.
 *
 * @param client - Database client.
 * @returns Array of column names and their data types that are NOT NULL.
 */
const getMatchesNotNullColumns = async (
  client: PoolClient | Pool,
): Promise<Array<{ column_name: string; data_type: string }>> => {
  if (matchesNotNullColumns) {
    return matchesNotNullColumns;
  }

  const pkColumn = await getMatchesPrimaryKeyColumn(client);

  // Query information_schema to find all NOT NULL columns (excluding primary key)
  const result = await client.query<{ column_name: string; data_type: string }>(
    `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'matches'
        AND is_nullable = 'NO'
        AND column_name != $1
      ORDER BY ordinal_position
    `,
    [pkColumn],
  );

  matchesNotNullColumns = result.rows;
  return matchesNotNullColumns;
};

/**
 * Get a default value for a column based on its data type.
 *
 * @param dataType - PostgreSQL data type.
 * @returns A default value appropriate for the data type.
 */
const getDefaultValueForType = (dataType: string): string | number | boolean => {
  const normalizedType = dataType.toLowerCase();
  
  // Handle numeric types (must check before string types to avoid false matches)
  if (
    normalizedType.includes('int') ||
    normalizedType.includes('numeric') ||
    normalizedType.includes('decimal') ||
    normalizedType.includes('real') ||
    normalizedType.includes('double') ||
    normalizedType.includes('float') ||
    normalizedType === 'money' ||
    normalizedType.includes('serial')
  ) {
    return 0;
  }
  
  // Handle boolean types
  if (normalizedType === 'boolean' || normalizedType === 'bool') {
    return false;
  }
  
  // Handle timestamp/date/time types
  if (
    normalizedType.includes('timestamp') ||
    normalizedType === 'date' ||
    normalizedType.includes('time') ||
    normalizedType === 'interval'
  ) {
    // Use Unix epoch (1970-01-01 00:00:00 UTC) as a safe default
    return '1970-01-01 00:00:00';
  }
  
  // Handle UUID types
  if (normalizedType === 'uuid') {
    return '00000000-0000-0000-0000-000000000000';
  }
  
  // Handle string/text types
  if (
    normalizedType.includes('char') ||
    normalizedType === 'text' ||
    normalizedType === 'varchar' ||
    normalizedType === 'character varying'
  ) {
    return '';
  }
  
  // Default to empty string for unknown types (but log a warning in development)
  // In most cases, unknown types are likely to be strings or will have defaults
  return '';
};

/**
 * Insert a match record into the database.
 *
 * This helper is used in integration tests to ensure matches exist in the database
 * before attempting to create settlement_items that reference them via foreign key.
 *
 * Since settlement_items.match_id references matches table, we insert a minimal record
 * with just the primary key to satisfy the foreign key constraint.
 *
 * Foreign key constraints are temporarily disabled during the insert to allow inserting
 * matches without requiring related order records to exist first.
 *
 * @param client - Database client (can be a transaction client).
 * @param match - Match object to insert.
 * @returns Promise that resolves when the match is inserted.
 */
export const insertMatch = async (
  client: PoolClient | Pool,
  match: Match,
): Promise<void> => {
  const pkColumn = await getMatchesPrimaryKeyColumn(client);
  const notNullColumns = await getMatchesNotNullColumns(client);
  
  // Temporarily disable foreign key constraints for this session
  // This allows inserting matches without requiring related order records
  await client.query("SET session_replication_role = 'replica'");
  
  try {
    // Build column list: primary key + all NOT NULL columns
    const allColumns = [pkColumn, ...notNullColumns.map((col) => col.column_name)];
    const columnList = allColumns.join(', ');
    
    // Build values: match ID + default values for NOT NULL columns
    const values: Array<string | number | boolean> = [match.matchId];
    const placeholders: string[] = ['$1'];
    
    notNullColumns.forEach((col, idx) => {
      values.push(getDefaultValueForType(col.data_type));
      placeholders.push(`$${idx + 2}`);
    });
    
    await client.query(
      `
        INSERT INTO matches (${columnList})
        VALUES (${placeholders.join(', ')})
        ON CONFLICT (${pkColumn}) DO NOTHING
      `,
      values,
    );
  } finally {
    // Restore foreign key constraints
    // If transaction is aborted, this will fail, so we catch and ignore
    try {
      await client.query("SET session_replication_role = 'origin'");
    } catch (error) {
      // If transaction is aborted, ignore the error as the transaction will be rolled back
      // and the session setting will be reset when the connection is released
    }
  }
};

/**
 * Insert multiple match records into the database.
 *
 * This function efficiently inserts all matches in a single batch operation
 * with foreign key constraints disabled for the entire batch.
 *
 * @param client - Database client (can be a transaction client).
 * @param matches - Array of match objects to insert.
 * @returns Promise that resolves when all matches are inserted.
 */
export const insertMatches = async (
  client: PoolClient | Pool,
  matches: readonly Match[],
): Promise<void> => {
  if (matches.length === 0) {
    return;
  }

  const pkColumn = await getMatchesPrimaryKeyColumn(client);
  const notNullColumns = await getMatchesNotNullColumns(client);
  
  // Temporarily disable foreign key constraints for this session
  // This allows inserting matches without requiring related order records
  await client.query("SET session_replication_role = 'replica'");
  
  try {
    // Build column list: primary key + all NOT NULL columns
    const allColumns = [pkColumn, ...notNullColumns.map((col) => col.column_name)];
    const columnList = allColumns.join(', ');
    
    // Build values and placeholders for batch insert
    const values: Array<string | number | boolean> = [];
    const placeholders: string[] = [];
    let paramIndex = 1;
    
    matches.forEach((match) => {
      const rowPlaceholders: string[] = [];
      // Primary key value
      values.push(match.matchId);
      rowPlaceholders.push(`$${paramIndex++}`);
      
      // Default values for NOT NULL columns
      notNullColumns.forEach((col) => {
        const defaultValue = getDefaultValueForType(col.data_type);
        values.push(defaultValue);
        rowPlaceholders.push(`$${paramIndex++}`);
      });
      
      placeholders.push(`(${rowPlaceholders.join(', ')})`);
    });
    
    await client.query(
      `
        INSERT INTO matches (${columnList})
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (${pkColumn}) DO NOTHING
      `,
      values,
    );
  } finally {
    // Restore foreign key constraints
    // If transaction is aborted, this will fail, so we catch and ignore
    try {
      await client.query("SET session_replication_role = 'origin'");
    } catch (error) {
      // If transaction is aborted, ignore the error as the transaction will be rolled back
      // and the session setting will be reset when the connection is released
    }
  }
};
