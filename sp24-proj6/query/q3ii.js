// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    // unwind the crew array
    {$unwind: 
        "$crew"
    },
    // find all record that has Wes Anderson and he is Director
    {$match: {
        $and: [
            {"crew.id": 5655},
            {"crew.job": "Director"}
        ]
    }},
    // then unwind the cast
    {$unwind: 
        "$cast"
    },
    // group by cast.id and cast.name
    {$group: {
        _id: {
            id: "$cast.id",
            name: "$cast.name"
        },
        count: {$sum: 1}
    }},
    // output needed Info
    {$project: {
        _id: 0,
        count: "$count",
        id: "$_id.id",
        name: "$_id.name"
    }},
    // sorted by count DESC, id ASC
    {$sort: {
        count: -1,
        id: 1
    }},
    // limit 5
    {$limit:
        5
    }
]);