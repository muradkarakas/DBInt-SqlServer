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






