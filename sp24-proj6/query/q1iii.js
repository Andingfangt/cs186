// Task 1iii

db.ratings.aggregate([
    // TODO: Write your query here
    // group by rating, and count each group
    {$group: {
        _id: "$rating",
        count: {$sum: 1}
    }},
    // sorted by rating DESC
    {$sort: {
        _id: -1
    }},
    // output "count" and "rating"
    {$project: {
        _id: 0,
        rating: "$_id",
        count: 1
    }}
]);