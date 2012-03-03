/*
 * utils.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#include "common.h"

void generateUUID(uuidstr_t newuuid) {
	uuid_t uuid;
	uuid_generate(uuid);
	uuid_unparse(uuid, newuuid);
}
