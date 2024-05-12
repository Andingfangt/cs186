// Task 3i

db.credits.aggregate([
    // TODO: Write your query here
    //find all movie that has Stan Lee'id
    {$match: {
        cast: {$elemMatch: {id: 7624}}
    }},
    // unwind those cast(array)
    {$unwind: 
        "$cast"
    },
    // only maintain the id == 7624
    {$match: {
        "cast.id": 7624
    }},
    // join with movies_metadata on movieId
    {$lookup: {
        from: "movies_metadata",
        localField: "movieId",
        foreignField: "movieId",
        as: "movie_data"
    }},
    // $lookup stage returning arrays for the title and release_date fields instead of single values.
    // so first need to unwind the arrays returned by $lookup
    { $unwind: 
        "$movie_data" 
    },
    // get needed info
    {$project: {
        _id: 0,
        title: "$movie_data.title",
        release_date: "$movie_data.release_date",
        character: "$cast.character"
    }},
    // sorted by release_data DESC
    {$sort: {
        release_date: -1
    }}
]);