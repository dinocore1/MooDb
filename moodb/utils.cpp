/*
 * utils.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#include "common.h"
#include "utils.h"
#include "Poco/UUID.h"
#include "Poco/UUIDGenerator.h"



using Poco::UUID;
using Poco::UUIDGenerator;

UUIDGenerator& generator = UUIDGenerator::defaultGenerator();

std::string generateUUID() {

	UUID uuid1(generator.create());
	return uuid1.toString();
	/*
	uuid_t uuid;
	uuid_generate(uuid);
	uuid_unparse(uuid, newuuid);
	 */
}

