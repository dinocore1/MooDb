
#include <moodb/moodb.h>

using namespace moodb;

int main(int argc, char** argv) {

    MooDB db;
    db.open("test.db");

    Record r1 = db.createRecord();

}