var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

MongoClient.connect(url, function (err, db) {
    if (err) throw err;
    var dbo = db.db("test");

    //Tutaj tworzymy jest kolekcja tylko z id i datą wypożyczenia
    dbo.collection('students').aggregate(
        [
            {
                $lookup:
                    {
                        from: "books",
                        localField: "_id",
                        foreignField: "_id",
                        as: "books"
                    }
            },
            {
                $project: {
                    name: 0,
                    scores: 0,
                    "books.isbn": 0,
                    "books.thumbnailUrl": 0,
                    "books.status": 0,
                    "books.longDescription": 0,
                    "books.shortDescription": 0,
                    "books.categories": 0,
                    "books.authors": 0,
                    "books.categories": 0,
                    "books.publishedDate": 0,
                    "books.pageCount": 0,
                    "books.title": 0
                }
            },
            {
                $addFields: {
                    "books.date": generateRandomDate(new Date().setFullYear(2017, 0, 1), new Date().setFullYear(2018, 05, 30))
                }
            },
            {
                $out: "rentals"
            }
        ]
    ).toArray(function (err, result) {
        if (err) throw err;
        console.log(result);
    });

    // Ta kolekcja może być zbędna jak zrezygnujemy z tych łączonych
    dbo.collection('students').aggregate(
        [
            {
                $lookup:
                    {
                        from: "books",
                        localField: "_id",
                        foreignField: "_id",
                        as: "books"
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
    // aggregations only for rentals collection
    // TODO

});

function generateRandomDate(start, end) {
    return new Date(start + Math.random() * (end - start));
}