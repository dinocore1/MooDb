
#include <moodb/moodb.h>
#include "utils.h"
#include <db_cxx.h>

using namespace std;

BEGIN_MOODB_NAMESPACE




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

void MooDB::open(const string& filename) {
    u_int32_t oFlags = DB_CREATE;
    mImpl->mDB.open(NULL, filename.c_str(), NULL, DB_BTREE, oFlags, 0);

}

Record MooDB::createRecord() {

}



END_MOODB_NAMESPACE
