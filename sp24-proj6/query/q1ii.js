// Task 1ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    // Only include Comedy movies with 50 or more votes.
    {$match: {
        $and: [
            {vote_count: {$gte: 50}},
            {genres: {$elemMatch: {name: "Comedy"}}}
        ]
    }},
    // Ordered by average vote DESC, breaking ties by vote count DESC, and any further ties in ASC of movieId
    {$sort: {
        vote_average: -1,
        vote_count: -1,
        movieId: 1
    }},
    // Return the top 50
    {$limit:
        50
    },
    // Only output "title", "vote_average", "vote_count", "movieId"
    {$project: {
        title: 1,
        vote_average: 1,
        vote_count: 1,
        movieId: 1,
        _id: 0
    }}
]);