#pragma once

#define DELAYIMP_INSECURE_WRITABLE_HOOKS
#include <delayimp.h>
#include "..\SodiumShared\SodiumShared.h"
#include "DBInt_SqlServer_delayLoaded_DLL_FuncImps.h"


#ifdef __cplusplus
extern "C" {
#endif

	// delay-loaded DLLs implementation functions
	FARPROC WINAPI		delayedDllFailHook(unsigned dliNotify, PDelayLoadInfo pdli);
	FARPROC WINAPI		delayHook(unsigned dliNotify, PDelayLoadInfo pdli);

#ifdef __cplusplus
}
#endif






