
#include <moodb/moodb.h>

#include <iostream>

using namespace moodb;
using namespace std;

int main(int argc, char** argv) {

    MooDB db;
    db.open("test.db");

    Record r1 = db.createRecord();
    cout << r1.getId() << endl;

    Document document;
    document.SetObject();
    document.AddMember("message", "hello world, dude", document.GetAllocator());
    r1.setData(document);

    Record r2 = db.getRecord("55");
    r2.setData(document);


}