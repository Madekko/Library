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

    // aggregations only for books collection
    dbo.collection('books').aggregate(
        [
            {
                $bucketAuto: {
                    groupBy: "$pageCount",
                    buckets: 3
                }
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });


    dbo.collection('books').aggregate(
        [
            {
                $match: {"pageCount": {$gt: 550}}
            },
            {
                $count: "More than 750 pages"
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

    dbo.collection('books').aggregate(
        [
            {
                $sortByCount: "$categories"
            },
            {
                $limit: 3
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

    dbo.collection('books').aggregate(
        [
            {
                $match: {"categories": "Java"}
            },
            {
                $sort: {"title": 1}
            },
            {
                $project: {
                    _id: 0,
                    isbn: 0,
                    publishedDate: 0,
                    thumbnailUrl: 0,
                    status: 0,
                    longDescription: 0,
                    shortDescription: 0,
                    categories: 0
                }
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

    dbo.collection('books').aggregate(
        [
            {
                $sortByCount: "$authors"
            },
            {
                $limit: 1
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

    dbo.collection('books').aggregate(
        [
            {
                $sort: {"publishedDate": -1}
            },
            {
                $project: {
                    isbn: 0,
                    thumbnailUrl: 0,
                    status: 0,
                    longDescription: 0,
                    shortDescription: 0,
                    categories: 0
                }
            },
            {
                $limit: 10
            },
            {
                $out: "Newest books"
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
    });

    // aggregations only for students collection
    // TODO
    //
    //
    // aggregations only for rentals collection

    dbo.collection('rentals').aggregate(
        [
            {
                $sample: {size: 100}
            },
            {
                $addFields: {
                    open: generateRandomDate(new Date().setFullYear(2017, 0, 1), new Date().setFullYear(2018, 05, 30))
                }
            }
            //TODO To trzeba jeszcze przemyśleć, czy robimy te z datami, czy jednak tworzymy id, id,
            //TODO bo chyba jednak to nie jest trudne, a ta kolekcja coś mi nie pasuje, bo powielamy dane
            // ,
            // {
            //     $out : "Rentals with dates"
            // }

        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

});

function generateRandomDate(start, end) {
    return new Date(start + Math.random() * (end - start));
}