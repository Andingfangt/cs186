// Task 2i

// some defined constants
const mean_vote = 7;
const minimum_vote = 1838;

db.movies_metadata.aggregate([
    // TODO: Write your query here
    // we sure need fields's vote_count >= 1838
    {$match: {
        vote_count: {$gte: minimum_vote}
    }},
    // get all we needed fields
    {$project: {
        _id: 0,
        title: 1,
        vote_count: 1,
        // $score = (v_c) / (v_c+1838) * v_r + (1838) / (1838+v_c) * 7 $
        score: {$round: [
            {$add: [
                {$multiply: [
                    {$divide: [
                        "$vote_count",
                        {$add: ["$vote_count", minimum_vote]}
                    ]},
                    "$vote_average"
                ]},
                {$multiply: [
                    {$divide: [
                        minimum_vote,
                        {$add: ["$vote_count", minimum_vote]}
                    ]},
                    mean_vote
                ]}
            ]},
            2
        ]}
    }},
    // sorted by score DESC, break ties by vote_count DESC and ASC of title
    {$sort: {
        score: -1,
        vote_count: -1,
        title: 1
    }},
    // return the highest 20
    {$limit: 
        20
    }
])