
#include <moodb/moodb.h>

#include <iostream>

using namespace moodb;
using namespace std;

int main(int argc, char** argv) {

    MooDB db;
    db.open("test.db");

    Record r1 = db.createRecord();
    cout << r1.getId() << endl;

}