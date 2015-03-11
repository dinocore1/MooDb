
#include <moodb/moodb.h>
#include "utils.h"
#include "moodb_imp.h"

#include <moodb/rapidjson/stringbuffer.h>
#include <moodb/rapidjson/writer.h>

#include <db_cxx.h>


using namespace std;

BEGIN_MOODB_NAMESPACE

GuidGenerator guidGenerator;

class MooDB::Impl {
public:
    Db mDB;

    Impl()
    : mDB(NULL, 0)
    {}

    ~Impl() {
        mDB.close(0);
    }
};

MooDB::MooDB() {
    mImpl = new MooDB::Impl();
}

MooDB::~MooDB() {
    delete mImpl;
}



Record::Record() {
    mImpl = new Record::Impl();
}

Record::~Record() {
    delete mImpl;
}

std::string Record::getId() {
    return mImpl->mId;
}

SavedRevision* Record::setData(const Document &doc) {
    mImpl->mData.CopyFrom(doc, mImpl->mData.GetAllocator());
    std::string id = getId();
    const char* str = id.c_str();
    Dbt key((void*)str, strlen(str)+1);

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);
    str = buffer.GetString();
    Dbt value((void*)str, strlen(str)+1);

    int ret = mImpl->mDB->mDB.put(NULL, &key, &value, 0);

}

void MooDB::open(const string& filename) {
    u_int32_t oFlags = DB_CREATE;
    mImpl->mDB.open(NULL, filename.c_str(), NULL, DB_BTREE, oFlags, 0);

}

Record MooDB::createRecord() {
    Record retval;

    retval.mImpl->mDB = mImpl;
    retval.mImpl->mId = guidGenerator.newGuid().toString();

    return retval;
}

Record MooDB::getRecord(const std::string &id) {

    Dbt key, value;
    {
        const char *str = id.c_str();
        key.set_data((void *) str);
        key.set_size(strlen(str) + 1);
    }


    Record retval;
    retval.mImpl->mDB = mImpl;
    retval.mImpl->mId = id;

    int ret = mImpl->mDB.get(NULL, &key, &value, 0);
    if(ret == 0) {
        const char* datastr = (const char*)value.get_data();
        retval.mImpl->mData.Parse(datastr);
    }

    return retval;
}



END_MOODB_NAMESPACE
