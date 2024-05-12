// Task 1i

db.keywords.aggregate([
    // TODO: Write your query here
    // Find all movies labeled with the keyword "mickey mouse" or "marvel comic"
    {$match: {
        $or: [
            {keywords: {$elemMatch: {name: "mickey mouse"}}},
            {keywords: {$elemMatch: {name: "marvel comic"}}}
        ]
    }},
    // Order output in ascending order of movieId
    {$sort: {
        movieId: 1
    }},
    // Only out put movieId
    {$project: {
        _id: 0,
        movieId: 1
    }}
]);