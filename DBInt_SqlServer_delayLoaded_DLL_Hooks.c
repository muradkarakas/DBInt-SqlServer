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

#include "DBInt_SqlServer_delayLoaded_DLL_Hooks.h"

FARPROC WINAPI delayedDllFailHook(unsigned dliNotify, PDelayLoadInfo pdli) 
{	
	switch (dliNotify) {
		case dliFailLoadLib: {
			//IsPostgreSqLClientDriverLoaded = FALSE;
			mkCoreDebug(__FILE__, __LINE__, "\nDBInt_SqlServer.dll says: DLL not found : ", pdli->szDll, NULL);
			HMODULE self = GetModuleHandle(0);
			return (FARPROC)self;
			break;
		}
		case dliFailGetProc: {
			//IsPostgreSqLClientDriverLoaded = FALSE;
			if (pdli->dlp.fImportByName) {
				// actually libpq export its functions by ordinal number not by name. so this part will probably not be run
				mkCoreDebug(__FILE__, __LINE__, "\nDBInt_SqlServer.dll says: Function not found : ", pdli->dlp.szProcName, NULL);
			}
			else {
				char str[10];
				sprintf_s(str, 10, "%d", pdli->dlp.dwOrdinal);
				mkCoreDebug(__FILE__, __LINE__, "\nDBInt_SqlServer.dll says: Function not found : ", str, NULL);
				/*if (pdli->dlp.dwOrdinal == 1) {
					//PQconnectdb
					return (FARPROC)PQconnectdbUnimplementedFunctionHook;
				} else if (pdli->dlp.dwOrdinal == 4) {
					//PQfinish
					return (FARPROC)PQfinish_UnimplementedFunctionHook;
				}
				else if(pdli->dlp.dwOrdinal == 14) {
					//PQstatus
					return (FARPROC)PQstatus_UnimplementedFunctionHook;
				} else if(pdli->dlp.dwOrdinal == 14) {
					//PQstatus
					return (FARPROC)PQstatus_UnimplementedFunctionHook;
				} else if (pdli->dlp.dwOrdinal == 15) {
					//PQerrorMessage
					return (FARPROC)PQerrorMessage_UnimplementedFunctionHook;
				}*/
			}
			//return (FARPROC)libpqUnimplementedFunctionHook;
			break;
		}
	}
	return 0;
}
extern PfnDliHook   __pfnDliFailureHook2 = delayedDllFailHook;

FARPROC WINAPI delayHook(unsigned dliNotify, PDelayLoadInfo pdli) 
{
	switch (dliNotify) {
		case dliStartProcessing:
			// If you want to return control to the helper, return 0.
			// Otherwise, return a pointer to a FARPROC helper function
			// that will be used instead, thereby bypassing the rest 
			// of the helper.
			break;
		case dliNotePreLoadLibrary:
			// If you want to return control to the helper, return 0.
			// Otherwise, return your own HMODULE to be used by the 
			// helper instead of having it call LoadLibrary itself.
			break;
		case dliNotePreGetProcAddress:
			// If you want to return control to the helper, return 0.
			// If you choose you may supply your own FARPROC function 
			// address and bypass the helper's call to GetProcAddress.
			break;
		case dliFailLoadLib: {
			//IsPostgreSqLClientDriverLoaded = FALSE;
			// LoadLibrary failed.
			// If you don't want to handle this failure yourself, return 0.
			// In this case the helper will raise an exception 
			// (ERROR_MOD_NOT_FOUND) and exit.
			// If you want to handle the failure by loading an alternate 
			// DLL (for example), then return the HMODULE for 
			// the alternate DLL. The helper will continue execution with 
			// this alternate DLL and attempt to find the
			// requested entrypoint via GetProcAddress.
			break;
		}
		case dliFailGetProc: {
			//IsPostgreSqLClientDriverLoaded = FALSE;
			// GetProcAddress failed.
			// If you don't want to handle this failure yourself, return 0.
			// In this case the helper will raise an exception 
			// (ERROR_PROC_NOT_FOUND) and exit.
			// If you choose you may handle the failure by returning 
			// an alternate FARPROC function address.
			break;
		}
		case dliNoteEndProcessing:
			// This notification is called after all processing is done. 
			// There is no opportunity for modifying the helper's behavior
			// at this point except by longjmp()/throw()/RaiseException. 
			// No return value is processed.
			break;
		default:
			return NULL;
	}
	return NULL;
}
extern PfnDliHook   __pfnDliNotifyHook2 = delayHook;
