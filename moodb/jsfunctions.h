

#define JSPREPAREFUNC	"					\
    queryObj.emit = function(key, value) {	\
        var row = this.values[key];			\
        if(row == undefined){				\
            row = new Array();				\
            this.values[key] = row;			\
        }									\
        row[row.length] = value;			\
        };									\
        									\
    queryObj.values = {};					\
"


