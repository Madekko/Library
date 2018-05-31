var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

MongoClient.connect(url, function (err, db) {
    if (err) throw err;
    var dbo = db.db("test");
    dbo.collection('students').aggregate(
        [
            {
                $lookup:
                    {
                        from: "books",
                        localField: "_id",
                        foreignField: "_id",
                        as: "rentals"
                    }
            },
            {
                $out: "rentals"
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
    });
});