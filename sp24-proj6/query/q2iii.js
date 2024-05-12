// Task 2iii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    // reset the budget = "unknown" if the "$budget" field don't know
    {$project: {
        budget: {$cond: {
            if: {$or: [
                // four cases
                {$eq: ["$budget", false]},
                {$eq: ["$budget", null]},
                {$eq: ["$budget", ""]},
                {$eq: ["$budget", undefined]},
            ]},
            then: "unknown",
            else: "$budget"
        }},
    }},
    // reset the budget = know to int value
    {$project: {
        budget: {$cond: {
            if: {$or: [
                // tow case do not change
                {$eq: ["$budget", "unknown"]},
                {$isNumber: "$budget"}
            ]},
            then: "$budget",
            // trim the "$" "USD" and " ", and convert to int
            else: {$toInt: {
                $trim: {
                    input: "$budget",
                    chars: "USD$ "
                }
            }}
        }},
    }},
    // group by the round(budget, -7)
    {$group: {
        _id: {$cond: {
            if: {$isNumber: "$budget"},
            then: {$round: ["$budget", -7]},
            else: "$budget"
        }},
        count: {$sum: 1}
    }},
    //sorted by _id ASC
    {$sort: {
        _id: 1
    }},
    // output budget and count
    {$project: {
        _id: 0,
        budget: "$_id",
        count: 1
    }}
]);