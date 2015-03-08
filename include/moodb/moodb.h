#ifndef MOODB_H_
#define MOODB_H_

#include <moodb/rapidjson/document.h>

using namespace rapidjson;

namespace moodb {

class MooDB {
public:

  static MooDB* open(const char* filename);

  sp<Record> getRecord();

};


class SavedRevision {
public:
  SavedRevision* getPreviousRevision();
  Document getDocument();
};

class Record {
public:
  virtual ~Record();

  std::string getId();

  SavedRevision* getCurrentRevision();
  Document getData();
  SavedRevision* setData(Document doc);
};

class RowIterator {
public:
  virtual ~RowIterator();

  bool hasNext();
  sp<Record> next();

};

class Query {
public:
  sp<RowIterator> run();
  void setStartKey(const Value& key);
  void setEndKey(const Value& key);
};

class View {
public:
  Query* createQuery();

};


}

#endif /* MOODB_H_ */
