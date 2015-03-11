
using namespace std;

BEGIN_MOODB_NAMESPACE

class Record::Impl {
public:
    MooDB::Impl *mDB;

    std::string mId;

    Document mData;
};

END_MOODB_NAMESPACE