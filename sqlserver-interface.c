/**
 * This file is part of Sodium Language project
 *
 * Copyright © 2020 Murad Karakaþ <muradkarakas@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License v3.0
 * as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 *	https://choosealicense.com/licenses/gpl-3.0/
 */

#include "pch.h"

#include "..\DBInt\db-interface.h"

#include "sqlserver-interface.h"

SQLHENV     hEnv = NULL;

/*******************************************/
/* Macro to call ODBC functions and        */
/* report an error on failure.             */
/* Takes handle, handle type, and stmt     */
/*******************************************/

#define TRYODBC(h, ht, x)   {   RETCODE rc = x;\
                                if (rc != SQL_SUCCESS) \
                                { \
                                    _HandleDiagnosticRecord (h, ht, rc); \
                                } \
                                if (rc == SQL_ERROR) \
                                { \
                                    conn->err = TRUE; \
                                    conn->errText = "error occured"; \
                                    goto Exit;  \
                                }  \
                            }



void 
_SQLBindStringW(
	DBInt_Connection * conn, 
	DBInt_Statement* stm,
	SQLUSMALLINT colIndex,
	LPCWSTR szString, 
	SQLULEN strlen
)
{
	if (stm->statement.sqlserver.ParameterCount > 0) {

		ODBC_BINDING * binding = &stm->statement.sqlserver.bindVariables[colIndex - 1];

		if (binding->fCType == SQL_C_NUMERIC)
		{
			*((long*)binding->buffer) = _wtol(szString);
		
			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLBindParameter(
					*stm->statement.sqlserver.hStmt, 
					colIndex, 
					SQL_PARAM_INPUT, 
					SQL_C_SSHORT, 
					SQL_INTEGER, 
					0, 
					0, 
					binding->buffer, 
					0, 
					&binding->pcbValue));
		}
		else if (stm->statement.sqlserver.bindVariables[colIndex - 1].fCType == SQL_C_WCHAR) 
		{
			//SQLULEN len = wcslen(szString);
			size_t size = strlen * sizeof(WCHAR);
			memcpy_s(binding->buffer, size, szString, size);
			binding->pcbValue = SQL_NTS;

			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLBindParameter(
					*stm->statement.sqlserver.hStmt, 
					colIndex, 
					SQL_PARAM_INPUT, 
					SQL_C_WCHAR, 
					SQL_WCHAR,
					strlen,
					0, 
					binding->buffer,
					size,
					&binding->pcbValue));
		}
	}

Exit:
	return;
}

void 
_SQLBindStringA(
	DBInt_Connection * conn, 
	DBInt_Statement* stm,
	SQLUSMALLINT colIndex,
	LPCSTR szString,
	SQLULEN strlen
)
{
	if (stm->statement.sqlserver.ParameterCount > 0) {

		ODBC_BINDING * binding = &stm->statement.sqlserver.bindVariables[colIndex - 1];

		if (binding->fCType == SQL_C_NUMERIC)
		{
			*((long*)binding->buffer) = atol(szString);
		
			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLBindParameter(
					*stm->statement.sqlserver.hStmt, 
					colIndex, 
					SQL_PARAM_INPUT, 
					SQL_C_SSHORT, 
					SQL_INTEGER, 
					0, 
					0, 
					binding->buffer, 
					0, 
					&binding->pcbValue));
		}
		else if (stm->statement.sqlserver.bindVariables[colIndex - 1].fCType == SQL_C_WCHAR) 
		{
			size_t size = strlen * sizeof(WCHAR);
			memcpy_s(binding->buffer, size, szString, size);
			binding->pcbValue = SQL_NTS;

			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLBindParameter(
					*stm->statement.sqlserver.hStmt, 
					colIndex, 
					SQL_PARAM_INPUT, 
					SQL_C_WCHAR, 
					SQL_WCHAR,
					strlen,
					0, 
					binding->buffer,
					size,
					&binding->pcbValue));
		}
	}

Exit:
	return;
}

SQLSERVER_INTERFACE_API 
void 
sqlserverBindString(
	DBInt_Connection * conn,
	DBInt_Statement * stm, 
	char * bindVariableName, 
	char * bindVariableValue, 
	size_t valueLength
)
{
	SQLUSMALLINT	colIndex = atoi(bindVariableName);

	_SQLBindStringA(conn, stm, colIndex, bindVariableValue, valueLength);

}


SQLSERVER_INTERFACE_API 
void
sqlserverBindNumber(
	DBInt_Connection * conn,
	DBInt_Statement * stm, 
	char * bindVariableName, 
	char * bindVariableValue,
	size_t valueLength
)
{
	SQLUSMALLINT	colIndex = atoi(bindVariableName);

	size_t cCount = valueLength;
	wchar_t* wValue = mkMalloc(conn->heapHandle, (cCount + 1) * sizeof(wchar_t), __FILE__, __LINE__);
	mbstowcs_s(NULL, wValue, cCount + 1, bindVariableValue, cCount);
	wValue[cCount] = L'\0';

	_SQLBindStringW(conn, stm, colIndex, wValue, valueLength);

	mkFree(conn->heapHandle, wValue);

	return;
}

SQLSERVER_INTERFACE_API
char*
sqlserverExecuteInsertStatement(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* sql
)
{
	conn->errText = NULL;
	conn->err = FALSE;
	char* retval = mkMalloc(conn->heapHandle, 21, __FILE__, __LINE__);
	sqlserverExecuteSelectStatement(conn, stm, sql);
	mkItoa(stm->statement.sqlserver.cRowCount, retval);
	return retval;
}

SQLSERVER_INTERFACE_API
void 
sqlserverExecuteDeleteStatement(
	DBInt_Connection * conn,
	DBInt_Statement * stm, 
	const char * sql
)
{
	conn->errText = NULL;
	conn->err = FALSE;
	//char * retval = mkMalloc(conn->heapHandle, 21, __FILE__, __LINE__);
	sqlserverExecuteSelectStatement(conn, stm, sql);
	//mkItoa(stm->statement.sqlserver.cRowCount, retval);
}

SQLSERVER_INTERFACE_API
void
sqlserverExecuteUpdateStatement(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* sql
)
{
	conn->errText = NULL;
	conn->err = FALSE;
	//char * retval = mkMalloc(conn->heapHandle, 21, __FILE__, __LINE__);
	sqlserverExecuteSelectStatement(conn, stm, sql);
	//mkItoa(stm->statement.sqlserver.cRowCount, retval);
}

SQLSERVER_INTERFACE_API
void
sqlserverPrepare(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* sql
)
{
	conn->errText = NULL;
	conn->err = FALSE;

	// convertion char sql to wchar_t sql
	size_t sourceCharCount = strlen(sql);
	size_t memSize = (sizeof(wchar_t) * sourceCharCount) + sizeof(wchar_t);
	SQLWCHAR* wSql = mkMalloc(conn->heapHandle, memSize, __FILE__, __LINE__);
	mbstowcs_s(NULL, wSql, sourceCharCount + 1, sql, sourceCharCount);

	// Prepare
	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT,
		SQLPrepare(*stm->statement.sqlserver.hStmt, wSql, SQL_NTS));

	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT,
		SQLNumParams(
			*stm->statement.sqlserver.hStmt,
			&stm->statement.sqlserver.ParameterCount));

	if (stm->statement.sqlserver.ParameterCount > 0) {

		stm->statement.sqlserver.bindVariables = mkMalloc(conn->heapHandle, stm->statement.sqlserver.ParameterCount * sizeof(ODBC_BINDING), __FILE__, __LINE__);
		//
		//	Binding memory for parameters
		//	
		for (int colIndex = 0; colIndex < stm->statement.sqlserver.ParameterCount; colIndex++) {
			SQLSMALLINT		DataTypePtr;
			SQLULEN			ParameterSizePtr;
			SQLSMALLINT		DecimalDigitsPtr;
			SQLSMALLINT		NullablePtr;
			SQLINTEGER 		strlenOrIndPtr = SQL_NTS;

			//	describing parameter
			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLDescribeParam(
					*stm->statement.sqlserver.hStmt, //	StatementHandle
					(SQLUSMALLINT)colIndex + 1,
					&DataTypePtr,
					&ParameterSizePtr,
					&DecimalDigitsPtr,
					&NullablePtr));

			switch (DataTypePtr) {
				case SQL_CHAR:
				case SQL_VARCHAR:
				case SQL_WVARCHAR:
				case SQL_DECIMAL: {
					ParameterSizePtr *= 3;
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_WCHAR;
					break;
				}
				case SQL_NUMERIC: {
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_NUMERIC;
					break;
				}
				case SQL_INTEGER: {
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_LONG;
					break;
				}
				case SQL_SMALLINT: {
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_SHORT;
					break;
				}
				case SQL_REAL: {
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_FLOAT;
					break;
				}
				case SQL_DOUBLE:
				case SQL_FLOAT: {
					stm->statement.sqlserver.bindVariables[colIndex].fCType = SQL_C_DOUBLE;
					break;
				}
			}

			stm->statement.sqlserver.bindVariables[colIndex].buffer_length = ParameterSizePtr + sizeof(wchar_t);
			stm->statement.sqlserver.bindVariables[colIndex].buffer =
				mkMalloc(conn->heapHandle, stm->statement.sqlserver.bindVariables[colIndex].buffer_length, __FILE__, __LINE__);

		}
	}

Exit:
	return;
}



SQLSERVER_INTERFACE_API 
void 
sqlserverExecuteAnonymousBlock(
	DBInt_Connection * conn, 
	DBInt_Statement * stm, 
	const char* sql
)
{
	conn->errText = NULL;
	conn->err = FALSE;

	sqlserverExecuteSelectStatement(conn, stm, sql);
}

SQLSERVER_INTERFACE_API
void
sqlserverFreeStatement(
	DBInt_Connection * conn,
	DBInt_Statement * stm
)
{
	if (stm == NULL || conn == NULL) {
		return;
	}
	for (SQLSMALLINT iCol = 1; iCol <= stm->statement.sqlserver.cColCount; iCol++)
	{
		if (stm->statement.sqlserver.resultSet) {
			BINDING* pThisBinding = &stm->statement.sqlserver.resultSet[iCol - 1];
			if (pThisBinding->wRowData) {
				mkFree(conn->heapHandle, pThisBinding->wRowData);
				pThisBinding->wRowData = NULL;
			}		
			if (pThisBinding->chRowData) {
				mkFree(conn->heapHandle, pThisBinding->chRowData);
				pThisBinding->chRowData = NULL;
			}		
			if (pThisBinding->columnName) {
				mkFree(conn->heapHandle, pThisBinding->columnName);
				pThisBinding->columnName = NULL;
			}
			mkFree(conn->heapHandle, stm->statement.sqlserver.resultSet);
			stm->statement.sqlserver.resultSet = NULL;
		}
	}
	
	if (stm->statement.sqlserver.bindVariables) {
		for (SQLSMALLINT iCol = 0; iCol < stm->statement.sqlserver.ParameterCount; iCol++)
		{
			ODBC_BINDING* binding = &stm->statement.sqlserver.bindVariables[iCol];
			if (binding->buffer) {
				mkFree(conn->heapHandle, binding->buffer);
				binding->buffer = NULL;
			}	
		}
		mkFree(conn->heapHandle, stm->statement.sqlserver.bindVariables);
		stm->statement.sqlserver.bindVariables = NULL;
	}

	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT,
		SQLFreeStmt(*stm->statement.sqlserver.hStmt, SQL_CLOSE));

	SQLFreeHandle(SQL_HANDLE_STMT, *stm->statement.sqlserver.hStmt);
Exit:

	mkFree(conn->heapHandle, stm);
}

SQLSERVER_INTERFACE_API 
const char * 
sqlserverGetColumnNameByIndex(
	DBInt_Connection * conn, 
	DBInt_Statement * stm, 
	unsigned int index
) 
{
	char* columnName = NULL;
	if (stm->statement.sqlserver.cColCount > 0) {
		if (stm->statement.sqlserver.resultSet) {
			if (index <= stm->statement.sqlserver.cColCount) {
				BINDING* bind = &stm->statement.sqlserver.resultSet[index - 1];
				if (bind->columnName)
					columnName = bind->columnName;
			}
		}
	}
	return columnName;
}


SQLSERVER_INTERFACE_API 
const char * 
sqlserverGetColumnValueByColumnName(
	DBInt_Connection * conn, 
	DBInt_Statement * stm, 
	const char* columnName
)
{
	const char * retval = "";
	const char * colName = NULL;

	RETCODE         RetCode = SQL_SUCCESS;
	int             iCount = 0;

	for (int i = 1; i <= stm->statement.sqlserver.cColCount; i++) {
		colName = sqlserverGetColumnNameByIndex(conn, stm, i);
		if (colName) {
			if (_stricmp(colName, columnName) == 0) {
				BINDING* bind = &stm->statement.sqlserver.resultSet[i - 1];
				wcstombs_s(
					NULL,
					bind->chRowData,
					(bind->rowDataCharacterCount) * sizeof(char),
					bind->wRowData,
					wcslen(bind->wRowData));

				retval = bind->chRowData;

				break;
			}
		}
	}

	return retval;
}

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
BOOL	
sqlserverNext(
	DBInt_Connection * conn,
	DBInt_Statement * stm
)
{
	RETCODE     RetCode;

	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT, 
		RetCode = SQLFetch(*stm->statement.sqlserver.hStmt));

	stm->statement.sqlserver.isEof = (RetCode == SQL_NO_DATA_FOUND);

Exit:

	return stm->statement.sqlserver.isEof;
}


int
_GetColumnIndexByColumnName(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* columnName
)
{
	int retval = -1;
	const char* colName = NULL;
	for (int i = 1; i <= stm->statement.sqlserver.cColCount; i++) {
		colName = sqlserverGetColumnNameByIndex(conn, stm, i);
		if (colName) {
			if (_stricmp(colName, columnName) == 0) {
				retval = i - 1;
				break;
			}
		}
	}
	return retval;
}


SQLSERVER_INTERFACE_API
SODIUM_DATABASE_COLUMN_TYPE
sqlserverGetColumnType(
	DBInt_Connection* conn,
	DBInt_Statement* stm,
	const char* columnName
)
{
	SODIUM_DATABASE_COLUMN_TYPE retval = HTSQL_COLUMN_TYPE_NOTSET;
	conn->errText = NULL;
	conn->err = FALSE;

	int colIndex = _GetColumnIndexByColumnName(conn, stm, columnName);
	if (colIndex > -1) {
		BINDING* bind = &stm->statement.sqlserver.resultSet[colIndex];
		retval = bind->dataType;
	}

	return retval;
}




SQLSERVER_INTERFACE_API 
BOOL	
sqlserverIsEof(
	DBInt_Connection * conn,
	DBInt_Statement * stm
)
{
	return stm->statement.sqlserver.isEof;
}

SQLSERVER_INTERFACE_API 
void 
sqlserverSeek(
	DBInt_Connection * conn, 
	DBInt_Statement * stm, 
	int rowNum
)
{
	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT,
		SQLFetchScroll(*stm->statement.sqlserver.hStmt, SQL_FETCH_ABSOLUTE, rowNum));

Exit:
	return;
}

void 
_BindAllResultSetColumns(
	DBInt_Connection * conn,
	DBInt_Statement  * stm
)
{
	SQLSMALLINT     iCol;
	BINDING*		pThisBinding = NULL;
	//SQLLEN          cchDisplay;
	SQLLEN          ssType;

	stm->statement.sqlserver.resultSet = mkMalloc(conn->heapHandle, stm->statement.sqlserver.cColCount * sizeof(BINDING), __FILE__, __LINE__);

	for (iCol = 1; iCol <= stm->statement.sqlserver.cColCount; iCol++)
	{
		pThisBinding = &stm->statement.sqlserver.resultSet[iCol-1];


		// Figure out the display length of the column (we will
		// bind to char since we are only displaying data, in general
		// you should bind to the appropriate C type if you are going
		// to manipulate data since it is much faster...)

		TRYODBC(*stm->statement.sqlserver.hStmt,
			SQL_HANDLE_STMT,
			SQLColAttribute(*stm->statement.sqlserver.hStmt,
				iCol,
				SQL_DESC_DISPLAY_SIZE,
				NULL,
				0,
				NULL,
				&pThisBinding->rowDataCharacterCount));

		pThisBinding->rowDataCharacterCount += sizeof(wchar_t);

		// Figure out data type. All types can be found at
		//	https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types?view=sql-server-ver15
		//
		TRYODBC(*stm->statement.sqlserver.hStmt,
			SQL_HANDLE_STMT,
			SQLColAttribute(*stm->statement.sqlserver.hStmt,
				iCol,
				SQL_DESC_CONCISE_TYPE,
				NULL,
				0,
				NULL,
				&ssType));

		switch (ssType) {
			case SQL_INTERVAL_DAY:
			case SQL_INTERVAL_HOUR:
			case SQL_INTERVAL_MINUTE:
			case SQL_INTERVAL_SECOND:
			case SQL_INTERVAL_DAY_TO_HOUR:
			case SQL_INTERVAL_DAY_TO_MINUTE:
			case SQL_INTERVAL_DAY_TO_SECOND:
			case SQL_INTERVAL_HOUR_TO_MINUTE:
			case SQL_INTERVAL_HOUR_TO_SECOND:
			case SQL_INTERVAL_MINUTE_TO_SECOND:
			case SQL_DECIMAL:
			case SQL_NUMERIC:
			case SQL_SMALLINT:
			case SQL_INTEGER:
			case SQL_REAL:
			case SQL_FLOAT:
			case SQL_DOUBLE:
			case SQL_TINYINT:
			case SQL_BIGINT: {
				pThisBinding->dataType = HTSQL_COLUMN_TYPE_NUMBER;
				break;
			}
			case SQL_GUID:
			case SQL_CHAR:
			case SQL_VARCHAR:
			case SQL_LONGVARCHAR: {
				pThisBinding->dataType = HTSQL_COLUMN_TYPE_TEXT;
				break; 
			}
			case SQL_WCHAR: 
			case SQL_WVARCHAR:
			case SQL_WLONGVARCHAR: {
				// TODO: ??? wchar_t
				pThisBinding->dataType = HTSQL_COLUMN_TYPE_TEXT;
				break; 
			}
			case SQL_TYPE_DATE:
			case SQL_TYPE_TIMESTAMP: {
				pThisBinding->dataType = HTSQL_COLUMN_TYPE_DATE;
				break;
			}
		}

		// Allocate a buffer big enough to hold columnd data in wchar_t format
		size_t wMemSize = ((pThisBinding->rowDataCharacterCount + 1) * sizeof(WCHAR));
		pThisBinding->wRowData = mkMalloc(conn->heapHandle, wMemSize, __FILE__, __LINE__);

		// Allocate a buffer big enough to hold columnd data in char format
		size_t cMemSize = ((pThisBinding->rowDataCharacterCount + 1) * sizeof(char));
		pThisBinding->chRowData = mkMalloc(conn->heapHandle, cMemSize, __FILE__, __LINE__);

		if (!(pThisBinding->wRowData))
		{
			fwprintf(stderr, L"Out of memory!\n");
			exit(-100);
		}

		// Map this buffer to the driver's buffer.   At Fetch time,
		// the driver will fill in this data.  Note that the size is 
		// count of bytes (for Unicode).  All ODBC functions that take
		// SQLPOINTER use count of bytes; all functions that take only
		// strings use count of characters.

		TRYODBC(*stm->statement.sqlserver.hStmt,
			SQL_HANDLE_STMT,
			SQLBindCol(*stm->statement.sqlserver.hStmt,
				iCol,
				SQL_C_WCHAR,
				(SQLPOINTER)pThisBinding->wRowData,
				wMemSize,
				&pThisBinding->indPtr));

		wchar_t wColumnName[200] = L"";
		SQLSMALLINT cchColumnNameLength;
		SQLLEN		numericAttributePtr;

		// Now set the display size that we will use to display
		// the data.   Figure out the length of the column name
		TRYODBC(*stm->statement.sqlserver.hStmt,
			SQL_HANDLE_STMT,
			SQLColAttribute(*stm->statement.sqlserver.hStmt,
				iCol,
				SQL_DESC_NAME,
				wColumnName,
				200,
				&cchColumnNameLength,
				&numericAttributePtr));

		size_t memSize = (cchColumnNameLength/sizeof(wchar_t)) + sizeof(char);
		pThisBinding->columnName = mkMalloc(conn->heapHandle, memSize, __FILE__, __LINE__);
		wcstombs_s(NULL, pThisBinding->columnName, memSize, wColumnName, memSize-1);
	}

Exit:
	return;
}


SQLSERVER_INTERFACE_API
void
sqlserverExecuteSelectStatement(
	DBInt_Connection * conn, 
	DBInt_Statement * stm, 
	const char * sql
)
{
	RETCODE     RetCode;

	conn->errText = NULL;
	conn->err = FALSE;

	// convertion char sql to wchar_t sql
	/*size_t sourceCharCount = strlen(sql);
	size_t memSize = (sizeof(wchar_t) * sourceCharCount) + sizeof(wchar_t);
	SQLWCHAR * wSql = mkMalloc(conn->heapHandle, memSize, __FILE__, __LINE__);
	mbstowcs_s(NULL, wSql, sourceCharCount+1, sql, sourceCharCount);
	*/
	RetCode = SQLExecute(*stm->statement.sqlserver.hStmt);

	switch (RetCode)
	{
		case SQL_NEED_DATA:
		{
			fwprintf(stderr, L"Need more data\n");
			break;
		}
		case SQL_SUCCESS_WITH_INFO:
		{
			_HandleDiagnosticRecord(*stm->statement.sqlserver.hStmt, SQL_HANDLE_STMT, RetCode);
			// fall through
		}
		case SQL_SUCCESS:
		{
			// If this is a row-returning query, display
			// results
			TRYODBC(*stm->statement.sqlserver.hStmt,
				SQL_HANDLE_STMT,
				SQLNumResultCols(*stm->statement.sqlserver.hStmt, &stm->statement.sqlserver.cColCount));

			if (stm->statement.sqlserver.cColCount > 0)
			{
				_BindAllResultSetColumns(conn, stm);

				TRYODBC(*stm->statement.sqlserver.hStmt,
					SQL_HANDLE_STMT,
					RetCode = SQLFetch(*stm->statement.sqlserver.hStmt));
				
				stm->statement.sqlserver.isEof = (RetCode == SQL_NO_DATA_FOUND);
			}
			else
			{
				// If sql is not select, getting the row count affected
				TRYODBC(*stm->statement.sqlserver.hStmt,
					SQL_HANDLE_STMT,
					SQLRowCount(*stm->statement.sqlserver.hStmt, &stm->statement.sqlserver.cRowCount));
			}
			break;
		}

		case SQL_ERROR:
		{
			_HandleDiagnosticRecord(*stm->statement.sqlserver.hStmt, SQL_HANDLE_STMT, RetCode);
			break;
		}

		default:
			fwprintf(stderr, L"Unexpected return code %hd!\n", RetCode);

	}
	/*
	TRYODBC(*stm->statement.sqlserver.hStmt,
		SQL_HANDLE_STMT,
		SQLFreeStmt(*stm->statement.sqlserver.hStmt, SQL_CLOSE));*/
Exit:
	// Free ODBC handles and exit

	/*if (hDbc)
	{
		SQLDisconnect(hDbc);
		SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
	}

	if (hEnv)
	{
		SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
	}*/
	return;
}

SQLSERVER_INTERFACE_API
DBInt_Statement*
sqlserverCreateStatement(
	DBInt_Connection * conn
)
{
	conn->errText = NULL;
	conn->err = FALSE;

	DBInt_Statement* retObj = (DBInt_Statement*)mkMalloc(conn->heapHandle, sizeof(DBInt_Statement), __FILE__, __LINE__);
	
	retObj->statement.sqlserver.hStmt = mkMalloc(conn->heapHandle, sizeof(SQLHSTMT), __FILE__, __LINE__);
	retObj->statement.sqlserver.isEof = FALSE;

	TRYODBC(*conn->connection.sqlserverHandle,
		SQL_HANDLE_DBC,
		SQLAllocHandle(SQL_HANDLE_STMT, *conn->connection.sqlserverHandle, retObj->statement.sqlserver.hStmt));
	
	TRYODBC(*conn->connection.sqlserverHandle,
		SQL_HANDLE_STMT,
		SQLSetStmtAttr(*retObj->statement.sqlserver.hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, 0));

Exit:

	return retObj;
}



SQLSERVER_INTERFACE_API
DBInt_Connection*
sqlserverCreateConnection(
	HANDLE heapHandle,
	DBInt_SupportedDatabaseType dbType,
	const char * hostName,
	const char * instanceName,
	const char * databaseName,
	const char * userName,
	const char * password
)
{
	DBInt_Connection* conn = (DBInt_Connection*)mkMalloc(heapHandle, sizeof(DBInt_Connection), __FILE__, __LINE__);
	conn->dbType = SODIUM_SQLSERVER_SUPPORT;
	conn->heapHandle = heapHandle;
	conn->errText = NULL;
	conn->err = FALSE;
	
	// converting char input parameters to wchar_t
	wchar_t wHostName[MAX_PATH] = L"";
	strcpy_s(conn->hostName, HOST_NAME_LENGTH, hostName);
	mbstowcs_s(NULL, wHostName, MAX_PATH, conn->hostName, strnlen_s(conn->hostName, MAX_PATH));
	
	wchar_t wInstanceName[MAX_PATH] = L"";
	mbstowcs_s(NULL, wInstanceName, MAX_PATH, instanceName, strnlen_s(instanceName, MAX_PATH));
	
	wchar_t wDatabaseName[MAX_PATH] = L"";
	mbstowcs_s(NULL, wDatabaseName, MAX_PATH, databaseName, strnlen_s(databaseName, MAX_PATH));
	
	wchar_t wUserName[MAX_PATH] = L"";
	mbstowcs_s(NULL, wUserName, MAX_PATH, userName, strnlen_s(userName, MAX_PATH));
	
	wchar_t wPassword[MAX_PATH] = L"";
	mbstowcs_s(NULL, wPassword, MAX_PATH, password, strnlen_s(password, MAX_PATH));

	conn->connection_string = mkStrcatW(heapHandle, __FILE__, __LINE__,
		L"Driver={SQL Server};",
		L"Server=", wHostName, L"\\", wInstanceName, 
		L";Database=", wDatabaseName, 
		L";User Id=", wUserName, 
		L";Password=", wPassword, L";",
		NULL);
	
	// Allocate an environment
	if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv) == SQL_ERROR)
	{
		conn->errText = "Unable to allocate an environment handle";
		conn->err = TRUE;
	}
	else {
		TRYODBC(hEnv,
			SQL_HANDLE_ENV,
			SQLSetEnvAttr(hEnv,
				SQL_ATTR_ODBC_VERSION,
				(SQLPOINTER)SQL_OV_ODBC3,
				0));

		// TODO: release 'conn->connection.sqlserverHandle' on disconnect
		conn->connection.sqlserverHandle = mkMalloc(heapHandle, sizeof(SQLHANDLE), __FILE__, __LINE__);

		// Allocate a connection
		TRYODBC(hEnv,
			SQL_HANDLE_ENV,
			SQLAllocHandle(SQL_HANDLE_DBC, hEnv, conn->connection.sqlserverHandle));

		// Connect to the driver.  Use the connection string if supplied
		// on the input, otherwise let the driver manager prompt for input.

		TRYODBC(*conn->connection.sqlserverHandle,
			SQL_HANDLE_DBC,
			SQLDriverConnect(*conn->connection.sqlserverHandle,
				GetDesktopWindow(),
				conn->connection_string,
				SQL_NTS,
				NULL,
				0,
				NULL,
				SQL_DRIVER_COMPLETE | SQL_DRIVER_NOPROMPT));
	}

Exit:
	
	return conn;
}


SQLSERVER_INTERFACE_API
BOOL
sqlserverCommit(
	DBInt_Connection* conn
)
{
	char* query = "commit";

	conn->errText = NULL;
	conn->err = FALSE;

	DBInt_Statement* stm = sqlserverCreateStatement(conn);
	sqlserverPrepare(conn, stm, query);
	sqlserverExecuteAnonymousBlock(conn, stm, query);
	sqlserverFreeStatement(conn, stm);

	// on success returns true
	return (conn->errText != NULL);
}

/************************************************************************
/* HandleDiagnosticRecord : display error/warning information
/*
/* Parameters:
/*      hHandle     ODBC handle
/*      hType       Type of handle (HANDLE_STMT, HANDLE_ENV, HANDLE_DBC)
/*      RetCode     Return code of failing command
/************************************************************************/

void 
_HandleDiagnosticRecord(
	SQLHANDLE      hHandle,
	SQLSMALLINT    hType,
	RETCODE        RetCode
)
{
	SQLSMALLINT iRec = 0;
	SQLINTEGER  iError;
	WCHAR       wszMessage[1000];
	WCHAR       wszState[SQL_SQLSTATE_SIZE + 1];


	if (RetCode == SQL_INVALID_HANDLE)
	{
		fwprintf(stderr, L"Invalid handle!\n");
		return;
	}

	while (SQLGetDiagRec(hType,
		hHandle,
		++iRec,
		wszState,
		&iError,
		wszMessage,
		(SQLSMALLINT)(sizeof(wszMessage) / sizeof(WCHAR)),
		(SQLSMALLINT*)NULL) == SQL_SUCCESS)
	{
		// Hide data truncated..
		if (wcsncmp(wszState, L"01004", 5))
		{
			fwprintf(stderr, L"[%5.5s] %s (%d)\n", wszState, wszMessage, iError);
		}
	}

}

SQLSERVER_INTERFACE_API 
const char * 
sqlserverGetLastErrorText(
	DBInt_Connection * conn
) 
{
	return conn->errText;
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

SQLSERVER_INTERFACE_API unsigned int sqlserverGetColumnSize(DBInt_Connection* conn, DBInt_Statement* stm, const char* columnName) {
	PRECHECK(conn);

	conn->errText = NULL;
	int colNum = PQfnumber(stm->statement.postgresql.resultSet, columnName);
	unsigned int colSize = PQfsize(stm->statement.postgresql.resultSet, colNum);
	return colSize;
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

SQLSERVER_INTERFACE_API void sqlserverRegisterString(DBInt_Connection* conn, DBInt_Statement* stm, const char* bindVariableName, int maxLength) {
	PRECHECK(conn);
	conn->errText = NULL;
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



SQLSERVER_INTERFACE_API int sqlserverGetLastError(DBInt_Connection* conn) {
	return 0;
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