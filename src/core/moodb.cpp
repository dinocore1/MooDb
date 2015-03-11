
#include <moodb/moodb.h>
#include "utils.h"
#include "moodb_imp.h"
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
    std::string retval = mImpl->mId.toString();
    return retval;
}

void MooDB::open(const string& filename) {
    u_int32_t oFlags = DB_CREATE;
    mImpl->mDB.open(NULL, filename.c_str(), NULL, DB_BTREE, oFlags, 0);

}

Record MooDB::createRecord() {
    Record retval;

    retval.mImpl->mId = guidGenerator.newGuid();

    return retval;
}



END_MOODB_NAMESPACE
