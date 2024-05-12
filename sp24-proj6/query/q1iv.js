// Task 1iv

db.ratings.aggregate([
    // find userId = 186
    {$match: {
        userId: 186
    }},
    // sorted by timestamp DESC
    {$sort: {
        timestamp: -1
    }},
    // only need the recent 5 record
    {$limit: 
        5
    },
    // use group by null and $push to create a document with needed fields
    {$group: {
        _id: null,
        movieIds: {$push: "$movieId"},
        ratings: {$push: "$rating"},
        timestamps: {$push: "$timestamp"}
    }},
    // output needed fields
    {$project: {
        movieIds: 1,
        ratings: 1,
        timestamps: 1,
        _id: 0
    }}
]);