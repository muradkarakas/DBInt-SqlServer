#include "pch.h"

#include "..\DBInt\db-interface.h"

#include "sqlserver-interface.h"

#include <windows.h>

SQLHENV     hEnv = NULL;

SQLSERVER_INTERFACE_API 
void 
sqlserverInitConnection(
	DBInt_Connection* conn
)
{
	conn->err = FALSE;
	conn->errText = NULL;
}


SQLSERVER_INTERFACE_API
DBInt_Connection*
sqlserverCreateConnection(
	HANDLE heapHandle,
	DBInt_SupportedDatabaseType dbType,
	const char* hostName,
	const char* dbName,
	const char* userName,
	const char* password
)
{
	DBInt_Connection* conn = (DBInt_Connection*)mkMalloc(heapHandle, sizeof(DBInt_Connection), __FILE__, __LINE__);
	conn->dbType = SODIUM_POSTGRESQL_SUPPORT;
	conn->errText = NULL;
	conn->heapHandle = heapHandle;
	conn->err = FALSE;

	strcpy_s(conn->hostName, HOST_NAME_LENGTH, hostName);

	/*if (IsPostgreSqLClientDriverLoaded == TRUE)
	{
		char* conninfo = mkStrcat(heapHandle, __FILE__, __LINE__, "host=", conn->hostName, " dbname=", dbName, " user=", userName, " password=", password, NULL);
		PGconn* pgConn = PQconnectdb(conninfo);
		// Check to see that the backend connection was successfully made 
		if (PQstatus(pgConn) != CONNECTION_OK) {
			conn->err = TRUE;
			conn->errText = PQerrorMessage(pgConn);
		}
		else {
			conn->connection.postgresqlHandle = pgConn;
			// starting a new transaction
			beginNewTransaction(conn);
		}

		mkFree(heapHandle, conninfo);
	}
	else
	{
		conn->err = TRUE;
		conn->errText = "PostgreSql client driver not loaded";
	}*/

	return conn;
}
/*

SQLSERVER_INTERFACE_API
char*
sqlserverGetPrimaryKeyColumn(
	DBInt_Connection* mkDBConnection,
	const char* schemaName,
	const char* tableName,
	int position
)
{
	char* retval = NULL;
	char positionStr[10];
	mkDBConnection->errText = NULL;

	mkItoa(position, positionStr);

	DBInt_Statement* stm = sqlserverCreateStatement(mkDBConnection);

	char* sql = mkStrcat(mkDBConnection->heapHandle, __FILE__, __LINE__,
		"SELECT "
		"	c.column_name, "
		"	c.data_type, "
		"	c.ordinal_position "
		"FROM "
		"	information_schema.table_constraints tc "
		"JOIN "
		"	information_schema.constraint_column_usage AS ccu USING(constraint_schema, constraint_name) "
		"JOIN "
		"	information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name "
		"WHERE "
		"		constraint_type = 'PRIMARY KEY' "
		"	and lower(c.table_schema) = lower('", schemaName, "') "
		"	and lower(tc.table_name) = lower('", tableName, "') "
		"	and c.ordinal_position = ", positionStr, " ",
		"order by "
		"	c.ordinal_position ",
		NULL
	);

	sqlserverPrepare(mkDBConnection, stm, sql);

	sqlserverExecuteSelectStatement(mkDBConnection, stm, sql);

	const char* tmp = sqlserverGetColumnValueByColumnName(mkDBConnection, stm, "COLUMN_NAME");

	if (tmp) {
		retval = mkStrdup(mkDBConnection->heapHandle, tmp, __FILE__, __LINE__);
	}
	return retval;
}

SQLSERVER_INTERFACE_API char* sqlserverGetDatabaseName(DBInt_Connection* conn) {
	PRECHECK(conn);

	char* retval = NULL;
	if (conn->connection.postgresqlHandle) {
		retval = PQdb(conn->connection.postgresqlHandle);
		if (retval) {
			retval = mkStrdup(conn->heapHandle, retval, __FILE__, __LINE__);
		}
	}

	POSTCHECK(conn);
	return retval;
}

SQLSERVER_INTERFACE_API BOOL sqlserverRollback(DBInt_Connection* conn) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, "rollback");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
	}
	PQclear(res);

	// starting a new transaction
	if (conn->errText == NULL) {
		beginNewTransaction(conn);
	}
	POSTCHECK(conn);
	// on success returns true
	return (conn->errText != NULL);
}

SQLSERVER_INTERFACE_API BOOL sqlserverCommit(DBInt_Connection* conn) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, "commit");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
	}
	PQclear(res);

	// starting a new transaction
	if (conn->errText == NULL) {
		beginNewTransaction(conn);
	}
	POSTCHECK(conn);
	// on success returns true
	return (conn->errText != NULL);
}

SQLSERVER_INTERFACE_API void	sqlserverFreeStatement(DBInt_Connection* conn, DBInt_Statement* stm) {
	PRECHECK(conn);

	if (stm == NULL || conn == NULL) {
		return;
	}

	if (stm->statement.postgresql.bindVariableCount > 0) {
		for (int paramIndex = 0; paramIndex < stm->statement.postgresql.bindVariableCount; paramIndex++) {
			if (stm->statement.postgresql.bindVariables[paramIndex]) {
				mkFree(conn->heapHandle, stm->statement.postgresql.bindVariables[paramIndex]);
				stm->statement.postgresql.bindVariables[paramIndex] = NULL;
			}
		}
		// Time to free all arrays
		mkFree(conn->heapHandle, stm->statement.postgresql.bindVariables);
		stm->statement.postgresql.bindVariables = NULL;

		mkFree(conn->heapHandle, stm->statement.postgresql.paramTypes);
		stm->statement.postgresql.paramTypes = NULL;

		mkFree(conn->heapHandle, stm->statement.postgresql.paramFormats);
		stm->statement.postgresql.paramFormats = NULL;

		mkFree(conn->heapHandle, stm->statement.postgresql.paramSizes);
		stm->statement.postgresql.paramSizes = NULL;

		stm->statement.postgresql.bindVariableCount = 0;
	}
	if (stm->statement.postgresql.resultSet) {
		PQclear(stm->statement.postgresql.resultSet);
		stm->statement.postgresql.resultSet = NULL;
	}
	mkFree(conn->heapHandle, stm);
}

SQLSERVER_INTERFACE_API void sqlserverDestroyConnection(DBInt_Connection* conn) {
	PRECHECK(conn);

	if (conn) {
		// close the connection to the database and cleanup 
		PQfinish(conn->connection.postgresqlHandle);
		conn->connection.postgresqlHandle = NULL;
	}
}

SQLSERVER_INTERFACE_API void postgresqlBindLob(DBInt_Connection* conn, DBInt_Statement* stm, const char* imageFileName, char* bindVariableName) {
	PRECHECK(conn);

	if (conn == NULL || imageFileName == NULL) {
		conn->errText = "Function parameter(s) incorrect";
	}

	conn->errText = NULL;

	// Import file content as large object
	Oid oid = writeLobContent(conn, imageFileName);
	if (oid > 0) {
		// success. Adding it into bind parameter list
		char oidStr[15];// = (char*)mkMalloc(conn->heapHandle, 15, __FILE__, __LINE__);
		//mkItoa(oid, );
		sprintf_s(oidStr, 15, "%d", oid);
		sqlserverAddNewParamEntry(conn, stm, bindVariableName, 0, 0, oidStr, strlen(oidStr));
		//mkFree(conn->heapHandle, oidStr);
	}
	else {
		conn->errText = "Bind is unsuccessful";
		sqlserverAddNewParamEntry(conn, stm, bindVariableName, 0, 0, "-1", 2);
	}
	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API void* sqlserverGetLob(DBInt_Connection* conn, DBInt_Statement* stm, const char* columnName, DWORD* sizeOfValue) {
	PRECHECK(conn);

	void* retVal = NULL;
	conn->errText = NULL;
	SODIUM_DATABASE_COLUMN_TYPE  colType = sqlserverGetColumnType(conn, stm, columnName);

	// OID can be stored in INTEGER column
	if (colType == HTSQL_COLUMN_TYPE_NUMBER) {
		// getting column index by name
		int colNum = PQfnumber(stm->statement.postgresql.resultSet, columnName);
		if (stm->statement.postgresql.currentRowNum == -1) {
			stm->statement.postgresql.currentRowNum = 0;
		}

		// getting lob id
		char* lobOid = PQgetvalue(stm->statement.postgresql.resultSet, stm->statement.postgresql.currentRowNum, colNum);
		if (lobOid) {
			// getting actual blob content
			Oid lobId = atoi(lobOid);
			if (lobId != InvalidOid) {
				retVal = getLobContent(conn, lobId, sizeOfValue);
			}
		}

		// returning blob binary content. return value should be FREE by caller 
		return retVal;
	}
	else if (colType == HTSQL_COLUMN_TYPE_LOB) {

		*sizeOfValue = sqlserverGetColumnSize(conn, stm, columnName);
		const void* a = sqlserverGetColumnValueByColumnName(conn, stm, columnName);
		return (void*)a;

	}
	else {
		conn->errText = "Not a lob. Use appropriate function for that column or change the type of column to integer";
		return NULL;
	}
	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API char* sqlserverExecuteInsertStatement(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	PGresult* res = NULL;
	size_t	sizeOfRetval = 15;
	char* retval = mkMalloc(conn->heapHandle, sizeOfRetval, __FILE__, __LINE__);
	mkItoa(0, retval);

	conn->errText = NULL;

	if (stm->statement.postgresql.bindVariableCount == 0) {
		// "insert" statement has no bind variables
		res = PQexec(conn->connection.postgresqlHandle, sql);
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {
			conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
			PQclear(res);
		}
		else {
			stm->statement.postgresql.currentRowNum = 0;
			stm->statement.postgresql.resultSet = res;
			char* rowCountAffected = PQcmdTuples(res);
			strcpy_s(retval, sizeOfRetval, rowCountAffected);
			//retval = postgresqlGetColumnValueByColumnName(conn, stm, "oid");
		}
	}
	else {
		// "insert" statement has bind variables
		res = PQexecParams(conn->connection.postgresqlHandle,
			sql,
			stm->statement.postgresql.bindVariableCount,
			stm->statement.postgresql.paramTypes,
			(const char* const*)stm->statement.postgresql.bindVariables,
			stm->statement.postgresql.paramSizes,
			stm->statement.postgresql.paramFormats,
			0);
		ExecStatusType statusType = PQresultStatus(res);
		if (statusType != PGRES_TUPLES_OK && statusType != PGRES_COMMAND_OK) {
			conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
			PQclear(res);
		}
		else {
			stm->statement.postgresql.currentRowNum = 0;
			stm->statement.postgresql.resultSet = res;
			char* rowCountAffected = PQcmdTuples(res);
			strcpy_s(retval, sizeOfRetval, rowCountAffected);
			//retval = postgresqlGetColumnValueByColumnName(conn, stm, "oid");
		}
	}

	POSTCHECK(conn);
	return retval;
}

SQLSERVER_INTERFACE_API void sqlserverExecuteUpdateStatement(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	PGresult* res = NULL;
	conn->errText = NULL;

	if (stm->statement.postgresql.bindVariableCount == 0) {
		// "update" statement has no bind variables
		res = PQexec(conn->connection.postgresqlHandle, sql);
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
		}
	}
	else {
		// "update" statement has bind variables
		res = PQexecParams(conn->connection.postgresqlHandle,
			sql,
			stm->statement.postgresql.bindVariableCount,
			stm->statement.postgresql.paramTypes,
			(const char* const*)stm->statement.postgresql.bindVariables,
			stm->statement.postgresql.paramSizes,
			stm->statement.postgresql.paramFormats,
			0);
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
		}
		else {
			stm->statement.postgresql.currentRowNum = 0;
			stm->statement.postgresql.resultSet = res;
		}
	}
	PQclear(res);
	stm->statement.postgresql.currentRowNum = 0;
	stm->statement.postgresql.resultSet = NULL;

	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API void sqlserverBindString(DBInt_Connection* conn, DBInt_Statement* stm, char* bindVariableName, char* bindVariableValue, size_t valueLength) {
	PRECHECK(conn);

	if (bindVariableName && bindVariableValue) {
		sqlserverAddNewParamEntry(conn,
			stm,
			bindVariableName,
			0 // 0 means auto detection,
			0 // variable format
			bindVariableValue,
			valueLength);
	}

	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API DBInt_Statement* sqlserverCreateStatement(DBInt_Connection* conn) {
	PRECHECK(conn);

	DBInt_Statement* retObj = (DBInt_Statement*)mkMalloc(conn->heapHandle, sizeof(DBInt_Statement), __FILE__, __LINE__);
	retObj->statement.postgresql.resultSet = NULL;
	retObj->statement.postgresql.currentRowNum = 0;
	retObj->statement.postgresql.bindVariableCount = 0;
	retObj->statement.postgresql.bindVariables = NULL;
	return retObj;
}

SQLSERVER_INTERFACE_API const char* sqlserverGetColumnNameByIndex(DBInt_Connection* conn, DBInt_Statement* stm, unsigned int index) {
	PRECHECK(conn);

	const char* columnName = NULL;
	columnName = (const char*)PQfname(stm->statement.postgresql.resultSet, index);
	return columnName;
}

SQLSERVER_INTERFACE_API const char* sqlserverGetColumnValueByColumnName(DBInt_Connection* conn, DBInt_Statement* stm, const char* columnName) {
	PRECHECK(conn);
	char* retVal = "";

	int colNum = PQfnumber(stm->statement.postgresql.resultSet, columnName);
	if (colNum < 0)
	{
		// Column number not found
	}
	else
	{
		if (stm->statement.postgresql.currentRowNum == -1) {
			stm->statement.postgresql.currentRowNum = 0;
		}
		retVal = PQgetvalue(stm->statement.postgresql.resultSet, stm->statement.postgresql.currentRowNum, colNum);
	}
	return retVal;
}


SQLSERVER_INTERFACE_API unsigned int sqlserverGetColumnSize(DBInt_Connection* conn, DBInt_Statement* stm, const char* columnName) {
	PRECHECK(conn);

	conn->errText = NULL;
	int colNum = PQfnumber(stm->statement.postgresql.resultSet, columnName);
	unsigned int colSize = PQfsize(stm->statement.postgresql.resultSet, colNum);
	return colSize;
}

SQLSERVER_INTERFACE_API
SODIUM_DATABASE_COLUMN_TYPE
sqlserverGetColumnType(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* columnName
)
{
	PRECHECK(conn);

	conn->errText = NULL;
	SODIUM_DATABASE_COLUMN_TYPE retVal = HTSQL_COLUMN_TYPE_NOTSET;
	int colNum = PQfnumber(stm->statement.postgresql.resultSet, columnName);
	Oid ctype = PQftype(stm->statement.postgresql.resultSet, colNum);
	switch (ctype) {
	case TIMESTAMPTZOID:
	case ABSTIMEOID:
	case TIMESTAMPOID:
	case DATEOID: {
		retVal = HTSQL_COLUMN_TYPE_DATE;
		break;
	}
	case CHAROID:
	case TEXTOID: {
		retVal = HTSQL_COLUMN_TYPE_TEXT;
		break;
	}
	case INT8OID:
	case INT2OID:
	case NUMERICOID:
	case INT4OID: {
		retVal = HTSQL_COLUMN_TYPE_NUMBER;
		break;
	}
	case OIDOID:
	case BYTEAOID: {
		retVal = HTSQL_COLUMN_TYPE_LOB;
		break;
	}
	}
	return retVal;
}

SQLSERVER_INTERFACE_API void sqlserverPrepare(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	conn->errText = NULL;
}


SQLSERVER_INTERFACE_API void sqlserverExecuteDescribe(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQdescribePrepared(conn->connection.postgresqlHandle, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
		PQclear(res);
	}
	else {
		stm->statement.postgresql.currentRowNum = 0;
		stm->statement.postgresql.resultSet = res;
	}
	POSTCHECK(conn);
}


SQLSERVER_INTERFACE_API void sqlserverExecuteSelectStatement(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
		PQclear(res);
	}
	else {
		stm->statement.postgresql.currentRowNum = 0;
		stm->statement.postgresql.resultSet = res;
	}
	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API void sqlserverRegisterString(DBInt_Connection* conn, DBInt_Statement* stm, const char* bindVariableName, int maxLength) {
	PRECHECK(conn);
	conn->errText = NULL;
}

SQLSERVER_INTERFACE_API void sqlserverExecuteDeleteStatement(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
	}
	PQclear(res);
	stm->statement.postgresql.resultSet = NULL;
	stm->statement.postgresql.currentRowNum = 0;
	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API void sqlserverExecuteAnonymousBlock(DBInt_Connection* conn, DBInt_Statement* stm, const char* sql) {
	PRECHECK(conn);

	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
	}
	PQclear(res);
	stm->statement.postgresql.resultSet = NULL;
	stm->statement.postgresql.currentRowNum = 0;
	POSTCHECK(conn);
}

SQLSERVER_INTERFACE_API unsigned int sqlserverGetAffectedRows(DBInt_Connection* conn, DBInt_Statement* stm) {
	PRECHECK(conn);

	return 0;
}

SQLSERVER_INTERFACE_API unsigned int sqlserverGetColumnCount(DBInt_Connection* conn, DBInt_Statement* stm) {
	PRECHECK(conn);

	unsigned int colCount = 0;
	colCount = PQnfields(stm->statement.postgresql.resultSet);
	return colCount;
}

SQLSERVER_INTERFACE_API const char* sqlserverGetLastErrorText(DBInt_Connection* conn) {
	PRECHECK(conn);
	return conn->errText;
}

SQLSERVER_INTERFACE_API int sqlserverGetLastError(DBInt_Connection* conn) {
	return 0;
}

SQLSERVER_INTERFACE_API void sqlserverSeek(DBInt_Connection* conn, DBInt_Statement* stm, int rowNum) {
	PRECHECK(conn);
	if (rowNum > 0) {
		stm->statement.postgresql.currentRowNum = rowNum - 1;
	}
}

SQLSERVER_INTERFACE_API BOOL	sqlserverNext(DBInt_Statement* stm) {
	stm->statement.postgresql.currentRowNum++;
	return sqlserverIsEof(stm);
}

SQLSERVER_INTERFACE_API BOOL	sqlserverIsEof(DBInt_Statement* stm) {
	if (stm == NULL || stm->statement.postgresql.resultSet == NULL) {
		return TRUE;
	}
	int nor = PQntuples(stm->statement.postgresql.resultSet);
	return (nor == 0 || stm->statement.postgresql.currentRowNum >= nor);
}

SQLSERVER_INTERFACE_API void	sqlserverFirst(DBInt_Statement* stm) {
	stm->statement.postgresql.currentRowNum = 0;
}

SQLSERVER_INTERFACE_API void	sqlserverLast(DBInt_Statement* stm) {
	int nor = PQntuples(stm->statement.postgresql.resultSet);
	stm->statement.postgresql.currentRowNum = nor - 1;
}

SQLSERVER_INTERFACE_API int	sqlserverPrev(DBInt_Statement* stm) {
	if (stm->statement.postgresql.currentRowNum > 0) {
		stm->statement.postgresql.currentRowNum--;
	}
	return (stm->statement.postgresql.currentRowNum == 0);
}

SQLSERVER_INTERFACE_API int sqlserverIsConnectionOpen(DBInt_Connection* conn) {
	PRECHECK(conn);

	BOOL retval = FALSE;
	conn->errText = NULL;
	PGresult* res = PQexec(conn->connection.postgresqlHandle, "");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		conn->errText = PQerrorMessage(conn->connection.postgresqlHandle);
		retval = (conn->errText == NULL || conn->errText[0] == '\0');
	}
	PQclear(res);
	POSTCHECK(conn);
	return retval;
}


*/