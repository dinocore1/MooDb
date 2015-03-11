#ifndef MOODB_H_
#define MOODB_H_

#include <moodb/rapidjson/document.h>
#include <memory>

using namespace rapidjson;

namespace moodb {

class SavedRevision {
public:
  SavedRevision* getPreviousRevision();
  Document getDocument();
};

class Record {
friend class MooDB;
public:
  Record();
  virtual ~Record();

  std::string getId();

  SavedRevision* getCurrentRevision();
  Document getData();
  SavedRevision* setData(Document doc);

private:
  class Impl;
  Impl* mImpl;
};

class MooDB {
public:
  MooDB();
  virtual ~MooDB();

  void open(const std::string& filename);
  void close();

  Record createRecord();
  Record getRecord(const std::string& id);
  void deleteRecord(const std::string& id);

private:
  class Impl;
  Impl* mImpl;

};


class RowIterator {
public:
  virtual ~RowIterator();

  bool hasNext();
  Record* next();

private:
  class Impl;
  Impl* mImpl;

};

class Query {
public:
  RowIterator run();
  void setStartKey(const Value& key);
  void setEndKey(const Value& key);

private:
  class Impl;
  Impl* mImpl;
};

class View {
public:
  Query* createQuery();

};


}

#endif /* MOODB_H_ */
