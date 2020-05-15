#pragma once

#include "..\DBInt\db-interface.h"


#define SQLSERVER_INTERFACE_API __declspec(dllexport)

/* DDL's PRIVATE FUNCTIONS  */

/* DDL's PUBLIC FUNCTIONS  */
SQLSERVER_INTERFACE_API void					sqlserverInitConnection(DBInt_Connection* conn);
SQLSERVER_INTERFACE_API DBInt_Connection	  * sqlserverCreateConnection(HANDLE heapHandle, DBInt_SupportedDatabaseType dbType, const char* hostName, const char* dbName, const char* userName, const char* password);
SQLSERVER_INTERFACE_API void					sqlserverDestroyConnection(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API int						sqlserverIsConnectionOpen(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API int						sqlserverIsEof(DBInt_Statement* stm);
SQLSERVER_INTERFACE_API void					sqlserverFirst(DBInt_Connection* mkConnection, DBInt_Statement* stm);
SQLSERVER_INTERFACE_API void					sqlserverLast(DBInt_Connection* mkConnection, DBInt_Statement* stm);
SQLSERVER_INTERFACE_API BOOL					sqlserverNext(DBInt_Statement* stm);
SQLSERVER_INTERFACE_API BOOL					sqlserverPrev(DBInt_Statement* stm);
SQLSERVER_INTERFACE_API DBInt_Statement		  * sqlserverCreateStatement(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API void					sqlserverFreeStatement(DBInt_Connection* mkConnection, DBInt_Statement* stm);
SQLSERVER_INTERFACE_API void					sqlserverSeek(DBInt_Connection* mkConnection, DBInt_Statement* stm, int rowNum);
SQLSERVER_INTERFACE_API int						sqlserverGetLastError(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API const char			  * sqlserverGetLastErrorText(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API BOOL					sqlserverCommit(DBInt_Connection* mkConnection);
/*	CALLER MUST RELEASE RETURN VALUE  */
SQLSERVER_INTERFACE_API char				  * sqlserverGetPrimaryKeyColumn(DBInt_Connection* mkDBConnection, const char* schemaName, const char* tableName, int position);
SQLSERVER_INTERFACE_API BOOL					sqlserverRollback(DBInt_Connection* mkConnection);
SQLSERVER_INTERFACE_API void					sqlserverRegisterString(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* bindVariableName, int maxLength);
SQLSERVER_INTERFACE_API unsigned int			sqlserverGetAffectedRows(DBInt_Connection* mkConnection, DBInt_Statement* stm);
SQLSERVER_INTERFACE_API void					sqlserverBindString(DBInt_Connection* mkDBConnection, DBInt_Statement* stm, char* bindVariableName, char* bindVariableValue, size_t valueLength);
SQLSERVER_INTERFACE_API void					sqlserverBindNumber(DBInt_Connection* mkDBConnection, DBInt_Statement* stm, char* bindVariableName, char* bindVariableValue, size_t valueLength);
SQLSERVER_INTERFACE_API void					sqlserverBindLob(DBInt_Connection* mkDBConnection, DBInt_Statement* stm, const char* imageFileName, char* bindVariableName);
SQLSERVER_INTERFACE_API void					sqlserverExecuteSelectStatement(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API void					sqlserverExecuteDescribe(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
/*  CALLER MUST RELEASE RETURN VALUE */
SQLSERVER_INTERFACE_API char				  * sqlserverExecuteInsertStatement(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API void					sqlserverExecuteDeleteStatement(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API void					sqlserverExecuteUpdateStatement(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API void					sqlserverExecuteAnonymousBlock(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API void					sqlserverPrepare(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* sql);
SQLSERVER_INTERFACE_API unsigned int			sqlserverGetColumnCount(DBInt_Connection* mkConnection, DBInt_Statement* stm);
SQLSERVER_INTERFACE_API const char			  * sqlserverGetColumnValueByColumnName(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* columnName);
SQLSERVER_INTERFACE_API void				  * sqlserverGetLob(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* columnName, DWORD* sizeOfValue);

SQLSERVER_INTERFACE_API SODIUM_DATABASE_COLUMN_TYPE
												sqlserverGetColumnType(
													DBInt_Connection* mkConnection,
													DBInt_Statement* stm,
													const char* columnName);

SQLSERVER_INTERFACE_API unsigned int			sqlserverGetColumnSize(DBInt_Connection* mkConnection, DBInt_Statement* stm, const char* columnName);
SQLSERVER_INTERFACE_API const char			  * sqlserverGetColumnNameByIndex(DBInt_Connection* mkDBConnection, DBInt_Statement* stm, unsigned int index);

/* Caller must release return value */
SQLSERVER_INTERFACE_API char				  * sqlserverGetDatabaseName(DBInt_Connection* conn);

